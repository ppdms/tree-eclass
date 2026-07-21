FROM python:3.14-slim

ARG TARGETARCH=amd64
ARG DISCORD_CHAT_EXPORTER_VERSION=2.47.3

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-ell \
    && rm -rf /var/lib/apt/lists/*

# Install the pinned, self-contained DiscordChatExporter CLI used by the
# optional app.messages exporter. It is disabled unless explicitly configured.
RUN case "${TARGETARCH}" in \
        amd64) dce_arch=x64; dce_sha256=8f86bd3a2c2f4412ffbbb2dcb9348642f8f929ad94a4f290ff0f78068c44fc86 ;; \
        arm64) dce_arch=arm64; dce_sha256=955b58d4bd6ca9107387f4c62bf3a0608bb7837e6f9decf3a216150bd2d888d9 ;; \
        arm) dce_arch=arm; dce_sha256=3a248ad8b92f5e75071fa273627f7d0c555a8b63d754c56c126057e20b5e6fe3 ;; \
        *) echo "Unsupported DiscordChatExporter architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && mkdir -p /opt/discord-exporter /tmp/discord-exporter \
    && curl -fsSL \
        "https://github.com/Tyrrrz/DiscordChatExporter/releases/download/${DISCORD_CHAT_EXPORTER_VERSION}/DiscordChatExporter.Cli.linux-${dce_arch}.zip" \
        -o /tmp/discord-exporter.zip \
    && echo "${dce_sha256}  /tmp/discord-exporter.zip" | sha256sum -c - \
    && python3 -m zipfile -e /tmp/discord-exporter.zip /tmp/discord-exporter \
    && cp -a /tmp/discord-exporter/. /opt/discord-exporter/ \
    && chmod 0755 /opt/discord-exporter/DiscordChatExporter.Cli \
    && rm -rf /tmp/discord-exporter /tmp/discord-exporter.zip

# Build and install diff-pdf from source
RUN apt-get update && apt-get install -y --no-install-recommends \
    automake \
    autoconf \
    g++ \
    make \
    pkg-config \
    git \
    libpoppler-glib-dev \
    libwxgtk3.2-dev \
    xvfb \
    xauth \
    && git clone --depth=1 https://github.com/vslavik/diff-pdf.git /tmp/diff-pdf \
    && cd /tmp/diff-pdf \
    && ./bootstrap \
    && ./configure \
    && make \
    && make install \
    && mv /usr/local/bin/diff-pdf /usr/local/bin/diff-pdf-bin \
    && printf '#!/bin/sh\nexec xvfb-run -a diff-pdf-bin "$@"\n' > /usr/local/bin/diff-pdf \
    && chmod +x /usr/local/bin/diff-pdf \
    && rm -rf /tmp/diff-pdf \
    && apt-get purge -y automake autoconf g++ make pkg-config git \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Download and install fonts
RUN apt-get update && apt-get install -y unzip && \
    mkdir -p /app/app/web/static/fonts /tmp/fonts && \
    cd /tmp/fonts && \
    # # Download Roboto (replaced by Inter)
    # curl -L -o roboto.zip "https://github.com/googlefonts/roboto-3-classic/releases/download/v3.011/Roboto_v3.011.zip" && \
    # unzip -q roboto.zip && \
    # cp unhinted/static/Roboto-Regular.ttf /app/app/web/static/fonts/ && \
    # cp unhinted/static/Roboto-Medium.ttf /app/app/web/static/fonts/ && \
    # cp unhinted/static/Roboto-Bold.ttf /app/app/web/static/fonts/ && \
    # Download Inter (variable font)
    curl -L -o inter.zip "https://github.com/rsms/inter/releases/download/v4.1/Inter-4.1.zip" && \
    unzip -q inter.zip && \
    find /tmp/fonts -name 'InterVariable.woff2' ! -name '*Italic*' -exec cp {} /app/app/web/static/fonts/ \; && \
    find /tmp/fonts -name 'InterVariable-Italic.woff2' -exec cp {} /app/app/web/static/fonts/ \; && \
    # Download RobotoMono
    curl -L -o robotomono.zip "https://github.com/googlefonts/RobotoMono/archive/refs/tags/v3.001.zip" && \
    unzip -q robotomono.zip && \
    cp RobotoMono-3.001/fonts/ttf/RobotoMono-Regular.ttf /app/app/web/static/fonts/ && \
    cp RobotoMono-3.001/fonts/ttf/RobotoMono-Medium.ttf /app/app/web/static/fonts/ && \
    cp RobotoMono-3.001/fonts/ttf/RobotoMono-Bold.ttf /app/app/web/static/fonts/ && \
    # Cleanup
    cd /app && \
    rm -rf /tmp/fonts && \
    apt-get remove -y unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Make entrypoint executable
RUN chmod +x docker-entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DB_FILE=/data/eclass.db
ENV KNOWLEDGE_DB_FILE=/data/knowledge.db

# Expose port
EXPOSE 8000

# Run the application
ENTRYPOINT ["./docker-entrypoint.sh"]
