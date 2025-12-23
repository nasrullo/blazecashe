#!/bin/bash

set -e

echo "🚀 Installing BlazeCache..."

# Build release binary
echo "Building BlazeCache..."
cargo build --release --bin blazecache

# Copy binary to system location
echo "Installing binary to /usr/local/bin..."
sudo cp target/release/blazecache /usr/local/bin/

# Make it executable
sudo chmod +x /usr/local/bin/blazecache

# Create user and group
echo "Creating blazecache user..."
sudo useradd -r -s /bin/false blazecache 2>/dev/null || true

# Create data directory
sudo mkdir -p /var/lib/blazecache
sudo chown blazecache:blazecache /var/lib/blazecache

# Install systemd service
echo "Installing systemd service..."
sudo cp blazecache.service /etc/systemd/system/
sudo systemctl daemon-reload

echo "✅ BlazeCache installed successfully!"
echo ""
echo "Usage:"
echo "  # Start manually:"
echo "  blazecache -p 6784 -m 64"
echo ""
echo "  # Start as service:"
echo "  sudo systemctl start blazecache"
echo "  sudo systemctl enable blazecache"
echo ""
echo "  # Check status:"
echo "  sudo systemctl status blazecache"
