#!/usr/bin/env python3
"""
DAG Proxy — Load Balancer for VSA Lithography Nodes
════════════════════════════════════════════════════

Single endpoint (dag.msgraph.de) that routes to healthy VSA nodes.
Includes:
- Health-based routing
- Sticky sessions (optional)
- Automatic failover
- Aggregated endpoints for cross-node operations
"""

import os
import asyncio
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import random
import time
from typing import List, Dict, Optional
from dataclasses import dataclass, field

app = FastAPI(title="DAG Proxy", version="1.0.0")

# VSA nodes - internal Railway URLs preferred
NODES = {
    "vsa01": os.getenv("VSA01_URL", "https://dag-vsa01.msgraph.de"),
    "vsa02": os.getenv("VSA02_URL", "https://dag-vsa02.msgraph.de"),
    "vsa03": os.getenv("VSA03_URL", "https://dag-vsa03.msgraph.de"),
}

# Health state
@dataclass
class NodeHealth:
    url: str
    healthy: bool = False
    last_check: float = 0
    latency_ms: float = 999
    vector_count: int = 0

health_state: Dict[str, NodeHealth] = {
    node_id: NodeHealth(url=url) for node_id, url in NODES.items()
}

HEALTH_CHECK_INTERVAL = 10  # seconds


async def check_node_health(node_id: str, node: NodeHealth):
    """Check single node health."""
    try:
        start = time.time()
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{node.url}/health")
            latency = (time.time() - start) * 1000
            
            if r.status_code == 200:
                data = r.json()
                node.healthy = data.get("ok", False)
                node.latency_ms = latency
                node.vector_count = data.get("total_vectors", 0)
            else:
                node.healthy = False
        node.last_check = time.time()
    except Exception as e:
        node.healthy = False
        node.last_check = time.time()


async def health_check_loop():
    """Background health checker."""
    while True:
        tasks = [
            check_node_health(node_id, node) 
            for node_id, node in health_state.items()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(health_check_loop())


def get_healthy_nodes() -> List[str]:
    """Return list of healthy node IDs."""
    return [
        node_id for node_id, node in health_state.items() 
        if node.healthy
    ]


def pick_node(strategy: str = "random") -> Optional[str]:
    """Pick a node based on strategy."""
    healthy = get_healthy_nodes()
    if not healthy:
        return None
    
    if strategy == "random":
        return random.choice(healthy)
    elif strategy == "fastest":
        return min(healthy, key=lambda n: health_state[n].latency_ms)
    elif strategy == "fullest":
        return max(healthy, key=lambda n: health_state[n].vector_count)
    else:
        return healthy[0]


# ═══════════════════════════════════════════════════════════════════════════════
# PROXY ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {
        "service": "dag-proxy",
        "description": "Load balancer for VSA Lithography nodes",
        "endpoints": {
            "/health": "Aggregated health of all nodes",
            "/nodes": "Individual node status",
            "/proxy/{path}": "Proxy to any healthy node",
            "/broadcast/{path}": "Broadcast to ALL nodes",
            "/vectors/*": "Proxied vector operations",
        }
    }


@app.get("/health")
async def aggregated_health():
    """Aggregated health across all nodes."""
    healthy = get_healthy_nodes()
    total_vectors = sum(h.vector_count for h in health_state.values() if h.healthy)
    
    return {
        "status": "healthy" if healthy else "degraded",
        "healthy_nodes": len(healthy),
        "total_nodes": len(NODES),
        "nodes": {
            node_id: {
                "healthy": node.healthy,
                "latency_ms": round(node.latency_ms, 2),
                "vectors": node.vector_count,
                "url": node.url,
            }
            for node_id, node in health_state.items()
        },
        "total_vectors": total_vectors,
        "quorum": len(healthy) >= 2,  # 2 of 3 = quorum
    }


@app.get("/nodes")
async def list_nodes():
    """List all nodes with status."""
    return {
        node_id: {
            "url": node.url,
            "healthy": node.healthy,
            "latency_ms": round(node.latency_ms, 2),
            "vectors": node.vector_count,
            "last_check": node.last_check,
        }
        for node_id, node in health_state.items()
    }


@app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_to_node(path: str, request: Request, strategy: str = "fastest"):
    """Proxy request to a healthy node."""
    node_id = pick_node(strategy)
    if not node_id:
        raise HTTPException(503, "No healthy nodes available")
    
    node_url = health_state[node_id].url
    target_url = f"{node_url}/{path}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Forward the request
        response = await client.request(
            method=request.method,
            url=target_url,
            content=await request.body(),
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            params=request.query_params,
        )
        
        return JSONResponse(
            content=response.json() if response.headers.get("content-type", "").startswith("application/json") else {"raw": response.text},
            status_code=response.status_code,
            headers={"X-DAG-Node": node_id}
        )


@app.api_route("/broadcast/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def broadcast_to_all(path: str, request: Request):
    """Broadcast request to ALL healthy nodes. For writes that must hit all."""
    healthy = get_healthy_nodes()
    if not healthy:
        raise HTTPException(503, "No healthy nodes available")
    
    body = await request.body()
    results = {}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for node_id in healthy:
            node_url = health_state[node_id].url
            target_url = f"{node_url}/{path}"
            
            try:
                response = await client.request(
                    method=request.method,
                    url=target_url,
                    content=body,
                    headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
                    params=request.query_params,
                )
                results[node_id] = {
                    "status": response.status_code,
                    "data": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text
                }
            except Exception as e:
                results[node_id] = {"status": "error", "error": str(e)}
    
    return {
        "broadcast": True,
        "nodes_hit": len(results),
        "results": results
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE ENDPOINTS (Proxied)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/vectors/count")
async def vectors_count():
    """Get vector count from fastest node."""
    node_id = pick_node("fastest")
    if not node_id:
        raise HTTPException(503, "No healthy nodes")
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{health_state[node_id].url}/vectors/count")
        return {**r.json(), "source_node": node_id}


@app.post("/vectors/upsert")
async def vectors_upsert(request: Request):
    """Upsert vector - broadcasts to ALL nodes for consistency."""
    return await broadcast_to_all("vectors/upsert", request)


@app.get("/vectors/get/{vector_id}")
async def vectors_get(vector_id: str):
    """Get vector from fastest node."""
    node_id = pick_node("fastest")
    if not node_id:
        raise HTTPException(503, "No healthy nodes")
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{health_state[node_id].url}/vectors/get/{vector_id}")
        return {**r.json(), "source_node": node_id}


@app.get("/schema")
async def get_schema():
    """Get schema from any healthy node."""
    node_id = pick_node("fastest")
    if not node_id:
        raise HTTPException(503, "No healthy nodes")
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{health_state[node_id].url}/schema")
        return r.json()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
