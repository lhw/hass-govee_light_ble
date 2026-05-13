FROM mcr.microsoft.com/devcontainers/python:3.14-trixie

# Install Bluetooth stack and development libraries for BLE development
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bluez \
        libbluetooth-dev \
        libdbus-1-dev \
        ffmpeg \
        libjpeg62-turbo \
        libpcap0.8 \
    && rm -rf /var/lib/apt/lists/*
