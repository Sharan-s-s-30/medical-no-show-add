FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY model_server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and model artifact
COPY model_server/api.py .
COPY models/no_show_model.joblib ./models/

# Run the FastAPI app
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]