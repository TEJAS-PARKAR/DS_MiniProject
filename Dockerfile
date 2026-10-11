FROM python:3.13-slim

WORKDIR /app

# Install build dependencies
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir pyyaml pandas matplotlib

# Copy application source and configurations
COPY config/ config/
COPY src/ src/

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "src.node.branch_node", "--node-id", "branch-0", "--config", "config/docker_defaults.yaml"]
