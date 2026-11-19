# One image for every service.  docker-compose.yml chooses what each
# container runs: a cluster node (Raft + application server), the LLM
# server, or the web client.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Scripts may have Windows line endings if the repo was cloned on Windows.
RUN sed -i 's/\r$//' docker/node-entrypoint.sh \
    && chmod +x docker/node-entrypoint.sh

EXPOSE 8000 50051 50052 50061
