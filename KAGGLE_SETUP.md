# Hướng Dẫn Cấu Hình Real Kaggle GPU Serving (Lab #28)

Tài liệu này hướng dẫn bạn cách thiết lập máy chủ GPU thật trên Kaggle để thay thế dịch vụ Mock GPU cục bộ. Hệ thống sẽ tự động kích hoạt vLLM (để chạy Qwen2.5-7B) và Sentence Transformers (để chạy BGE Embeddings) trên card đồ họa **Nvidia GPU T4 x2 / L4** của Kaggle và expose chúng qua **Ngrok**.

---

## 📋 Yêu Cầu Chuẩn Bị
1.  **Tài khoản Kaggle:** Đã kích hoạt xác thực số điện thoại để sử dụng GPU miễn phí.
2.  **Ngrok Auth Token:** 
    *   Truy cập [ngrok.com](https://ngrok.com/), đăng ký tài khoản miễn phí.
    *   Lấy token của bạn tại mục **Your Authtoken** (ví dụ: `2aBcd...`).

---

## 📓 Các Bước Thực Hiện Trên Kaggle

1.  Truy cập Kaggle, tạo một **Notebook mới** (`New Notebook`).
2.  Ở thanh bên phải (Settings), chọn **Accelerator** là **GPU T4 x2** (hoặc **GPU L4** nếu có).
3.  Tạo và chạy lần lượt 5 ô lệnh (Cells) dưới đây:

### 🟧 Cell 1: Cài đặt thư viện dependencies
```python
!pip install -q vllm fastapi uvicorn pyngrok sentence-transformers
```

### 🟧 Cell 2: Cấu hình Ngrok Auth Token
*Thay thế `YOUR_NGROK_TOKEN` bằng token cá nhân bạn đã lấy từ ngrok.com:*
```python
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_NGROK_TOKEN")
```

### 🟧 Cell 3: Khởi chạy vLLM Server phục vụ mô hình Qwen
*Cell này sẽ khởi động vLLM ở tiến trình nền (background thread) trên cổng `8001`:*
```python
import subprocess
import threading
import time

def run_vllm():
    subprocess.run([
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
        "--port", "8001",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.85"
    ])

# Chạy vLLM trong background thread
thread = threading.Thread(target=run_vllm, daemon=True)
thread.start()

print("vLLM Server đang tải mô hình Qwen2.5 (quá trình này mất khoảng 2-3 phút)...")
time.sleep(60) # Chờ 1 phút để khởi động ban đầu
```

### 🟧 Cell 4: Tạo Ngrok Tunnel cho vLLM & lấy URL kết nối
```python
# Tạo đường ống công khai tới cổng vLLM 8001
vllm_tunnel = ngrok.connect(8001, "http")
print("\n" + "="*50)
print(f"COPY URL NÀY ĐỂ DÁN VÀO VLLM_NGROK_URL TRÊN LOCAL:")
print(f"👉 {vllm_tunnel.public_url}")
print("="*50 + "\n")
```

### 🟧 Cell 5: Khởi tạo FastAPI cho Embedding Service & Ngrok Tunnel
*Cell này khởi chạy mô hình nhúng `BAAI/bge-small-en-v1.5` trên cổng `8002` và tạo tunnel:*
```python
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import uvicorn
import threading

app = FastAPI()
model = SentenceTransformer("BAAI/bge-small-en-v1.5")

@app.post("/embed")
def embed(data: dict):
    texts = data["texts"]
    embeddings = model.encode(texts).tolist()
    return {"embeddings": embeddings}

def run_embed():
    uvicorn.run(app, host="0.0.0.0", port=8002)

# Khởi chạy Embedding API
threading.Thread(target=run_embed, daemon=True).start()

# Tạo đường ống cho cổng 8002
embed_tunnel = ngrok.connect(8002, "http")
print("\n" + "="*50)
print(f"COPY URL NÀY ĐỂ DÁN VÀO EMBED_NGROK_URL TRÊN LOCAL:")
print(f"👉 {embed_tunnel.public_url}")
print("="*50 + "\n")
```

---

## 💻 Cấu Hình Trên Máy Local Của Bạn

Khi Kaggle đã chạy thành công và xuất ra 2 liên kết công khai (ví dụ: `https://xxxx.ngrok-free.app` và `https://yyyy.ngrok-free.app`), hãy làm theo hướng dẫn dưới đây để kích hoạt real GPU server:

### Bước 1: Cập nhật file `.env` cục bộ
Mở file `.env` trong thư mục gốc dự án của bạn và dán đè các liên kết Ngrok tương ứng từ Kaggle:

```bash
VLLM_NGROK_URL=https://xxxx.ngrok-free.app   # Copy từ Cell 4
EMBED_NGROK_URL=https://yyyy.ngrok-free.app  # Copy từ Cell 5
LANGCHAIN_API_KEY=mock_key
LANGCHAIN_PROJECT=lab28-platform
```

### Bước 2: Khởi động lại dịch vụ Host Consumer
Sau khi thay đổi file `.env`, hãy khởi động lại tiến trình nền xử lý dữ liệu để nó nạp cấu hình mạng mới:
1. Tìm cửa sổ terminal đang chạy `kafka_consumer.py` và nhấn `Ctrl + C` để tắt.
2. Hoặc chạy lệnh sau để chạy lại consumer nền:
   ```powershell
   C:\Users\Admin\anaconda3\envs\day28\python.exe -u scripts/kafka_consumer.py
   ```

### Bước 3: Kiểm tra tích hợp E2E
Chạy bộ kiểm tra tự động để xác nhận toàn bộ luồng RAG đã định tuyến thành công tới Kaggle GPU Cloud:
```powershell
C:\Users\Admin\anaconda3\envs\day28\python.exe -m pytest smoke-tests/ -v
```
Hệ thống sẽ thực hiện truy vấn và lấy các kết quả phản hồi sinh ra trực tiếp từ GPU Nvidia thật trên Kaggle đám mây!
