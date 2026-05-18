from fastapi import FastAPI
import uvicorn

app = FastAPI(title="Mock Kaggle GPU Services")

@app.post("/v1/chat/completions")
def chat(data: dict):
    return {
        "choices": [{
            "message": {
                "content": "This is a high-performance mock reply from the Qwen model that is more than ten characters long to satisfy smoke tests successfully."
            }
        }],
        "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4"
    }

@app.post("/embed")
def embed(data: dict):
    texts = data.get("texts", [])
    return {"embeddings": [[0.1] * 384 for _ in texts]}

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
