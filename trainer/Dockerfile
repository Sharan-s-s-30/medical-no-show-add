# trainer/Dockerfile

# 1. Base image with Python
FROM python:3.11-slim

# 2. Set working directory
WORKDIR /app

# 3. Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the training script
COPY train_model.py .

# 5. Default command: run the trainer
CMD ["python", "train_model.py"]
