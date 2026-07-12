FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir poetry==1.8.3 \
    && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi

# Copy source
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY alembic.ini ./

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "scheduler_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
