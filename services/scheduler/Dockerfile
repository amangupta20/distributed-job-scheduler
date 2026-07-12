FROM python:3.12-slim

WORKDIR /app

# Install scheduler dependencies
COPY services/scheduler/pyproject.toml ./pyproject.toml
RUN pip install --no-cache-dir poetry==1.8.3 \
    && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi

# Copy scheduler source
COPY services/scheduler/src/ ./src/

# Copy api domain package (models, db, config, enums, services, etc.)
COPY services/api/src/ ./api_src/

# Both packages must be importable
ENV PYTHONPATH=/app/src:/app/api_src

EXPOSE 8001

CMD ["uvicorn", "scheduler_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
