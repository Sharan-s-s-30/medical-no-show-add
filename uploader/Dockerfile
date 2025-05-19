# 1. Base image with Python runtime
FROM python:3.11-slim

# 2. Set working directory inside the container
WORKDIR /app

# 3. Copy only the requirements first (cache-friendly)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your service code into the image
COPY . ./uploader

# 6. Default command: run the service
CMD ["python", "-m", "uploader.uploader"]

