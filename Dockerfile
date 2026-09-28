FROM python:3.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    ripgrep \
    procps \
    libpq-dev \
    tmux \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://chatgpt.com/codex/install.sh | sh

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Codex config
RUN mkdir -p /root/.codex
COPY config.toml /root/.codex/config.toml


COPY . /workspace
ENV ALPHAPOOL_LOG_LEVEL=debug
WORKDIR /workspace
CMD ["python", "-m", "src.main"]
