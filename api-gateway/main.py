# api-gateway/main.py
from fastapi import FastAPI, Request, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
import httpx, os, time

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)  # Integration 9: Prometheus

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8001")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")

@app.post("/api/v1/chat")
async def chat(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not body or "query" not in body:
        raise HTTPException(status_code=422, detail="Missing required field: query")

    query = body["query"]
    start = time.time()

    # 1. Vector search with graceful degradation
    context = []
    try:
        async with httpx.AsyncClient() as client:
            search_resp = await client.post(f"{QDRANT_URL}/collections/documents/points/search", json={
                "vector": body.get("embedding", [0.0] * 384),
                "limit": 3
            }, timeout=3.0)
            if search_resp.status_code == 200:
                context = search_resp.json().get("result", [])
            else:
                print(f"Qdrant returned non-200: {search_resp.status_code}")
    except Exception as e:
        print(f"Qdrant search failed or timed out: {e}. Degrading gracefully with empty context.")

    # 2. LLM inference with circuit-breaker style fallback
    prompt = f"Context: {context}\n\nQuery: {query}"
    answer = "Error: Could not retrieve a valid answer from the model serving layer. Please try again later."
    model_name = "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            llm_resp = await client.post(f"{VLLM_URL}/v1/chat/completions", json={
                "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
                "messages": [{"role": "user", "content": prompt}]
            })
            if llm_resp.status_code == 200:
                result = llm_resp.json()
                answer = result["choices"][0]["message"]["content"]
                model_name = result.get("model", model_name)
            else:
                print(f"LLM serving returned non-200: {llm_resp.status_code}")
                answer = f"Error: LLM serving layer returned status code {llm_resp.status_code}."
    except Exception as e:
        print(f"LLM inference request failed: {e}. Returning fallback error message.")
        answer = f"Fallback Answer: Due to a temporary system disconnect, we are unable to process your request. error: {str(e)}"

    latency = (time.time() - start) * 1000

    return {
        "answer": answer,
        "latency_ms": round(latency, 2),
        "model": model_name
    }

@app.get("/health")
def health():
    return {"status": "ok"}
