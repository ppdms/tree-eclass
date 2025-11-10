FROM python:3.14-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
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
    # Download Roboto
    cd /tmp/fonts && \
    curl -L -o roboto.zip "https://github.com/googlefonts/roboto-3-classic/releases/download/v3.011/Roboto_v3.011.zip" && \
    unzip -q roboto.zip && \
    cp unhinted/static/Roboto-Regular.ttf /app/app/web/static/fonts/ && \
    cp unhinted/static/Roboto-Medium.ttf /app/app/web/static/fonts/ && \
    cp unhinted/static/Roboto-Bold.ttf /app/app/web/static/fonts/ && \
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

# Create directory for database and downloads
RUN mkdir -p /data/downloads

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DB_FILE=/data/eclass.db

# Expose port
EXPOSE 8000

# Run the application
ENTRYPOINT ["./docker-entrypoint.sh"]
