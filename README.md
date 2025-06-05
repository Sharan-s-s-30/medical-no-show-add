# Medical Appointments No-Show 

## Overview
RabbitMQ-driven ETL for the UCI Medical Appointment No-Shows dataset.



### 1. Clean everything (stop and remove volumes)
```bash
docker compose down -v
```

### 2. Build core Python services
```bash
docker compose build producer processor uploader frontend
```

### 3. Start RabbitMQ, PostgreSQL, and the Frontend
```bash
docker compose up -d rabbitmq postgres frontend
```

### 4. Publish the raw CSV
```bash
docker compose run --rm producer python producer.py --file data/raw/medical_appointments.csv
```

### 5. Process and clean the CSV
```bash
docker compose run --rm processor python processor.py
```

### 6. Ingest raw data into PostgreSQL
```bash
docker compose run --rm uploader python -m uploader.uploader upload-raw
```

### 7. Ingest cleaned data into PostgreSQL
```bash
docker compose run --rm uploader python -m uploader.uploader upload
```

### 8. Verify row counts in the database
```bash
docker compose exec postgres bash -lc 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT COUNT(*) FROM raw_appointments;"'
docker compose exec postgres bash -lc 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT COUNT(*) FROM processed_appointments;"'
```

### 9. Train the ML model offline and split data
```bash
mkdir -p models
docker build -t noshow-trainer -f trainer/Dockerfile trainer
docker run --rm --network medical-no-show-add_default --env-file .env -v "$(pwd -W)/models:/app/models" noshow-trainer
```

### 10. Build and start the model server
```bash
docker compose build model_server
docker compose up -d model_server
```

### 11. Test the frontend
Visit [http://localhost:3001/](http://localhost:3001/) in your browser.