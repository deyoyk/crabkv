#!/usr/bin/env bash
set -euo pipefail

# CrabKV Installer
# Downloads and runs CrabKV server for your platform
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --port 6380
#   curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --autostart

REPO="deyoyk/crabkv"
VERSION="${CRABKV_VERSION:-latest}"
PORT=6379
INSTALL_DIR="${CRABKV_INSTALL_DIR:-$HOME/.local/bin}"
AUTOSTART=false

usage() {
    cat <<EOF
CrabKV Installer

Usage:
  install.sh [OPTIONS]

Options:
  -p, --port PORT        Port to run CrabKV on (default: 6379)
  -d, --dir DIR          Installation directory (default: ~/.local/bin)
  -a, --autostart        Set up autostart for your platform
  -v, --version VERSION  Specific version to install (default: latest)
  -h, --help             Show this help message

Examples:
  curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash
  curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --port 6380
  curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --autostart
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -d|--dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -a|--autostart)
            AUTOSTART=true
            shift
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux*)     os="linux" ;;
        Darwin*)    os="macos" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        *)
            echo "Error: Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)   arch="amd64" ;;
        aarch64|arm64)   arch="arm64" ;;
        *)
            echo "Error: Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

get_release_url() {
    local platform="$1"
    local version="$2"

    if [[ "$version" == "latest" ]]; then
        version=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    fi

    case "$platform" in
        linux-amd64)
            echo "https://github.com/${REPO}/releases/download/${version}/crabkv-linux-amd64.tar.gz"
            ;;
        linux-arm64)
            echo "https://github.com/${REPO}/releases/download/${version}/crabkv-linux-arm64.tar.gz"
            ;;
        macos-amd64)
            echo "https://github.com/${REPO}/releases/download/${version}/crabkv-macos-amd64.tar.gz"
            ;;
        macos-arm64)
            echo "https://github.com/${REPO}/releases/download/${version}/crabkv-macos-arm64.tar.gz"
            ;;
        *)
            echo "Error: No pre-built binary for platform: $platform"
            echo "You can build from source: cargo install --git https://github.com/${REPO}.git"
            exit 1
            ;;
    esac
}

setup_autostart() {
    local binary_path="$1"
    local platform
    platform=$(detect_platform)
    local os="${platform%%-*}"

    case "$os" in
        linux)
            local service_dir="$HOME/.config/systemd/user"
            mkdir -p "$service_dir"
            cat > "$service_dir/crabkv.service" <<EOF
[Unit]
Description=CrabKV Server
After=network.target

[Service]
Type=simple
ExecStart=${binary_path} -p ${PORT}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
            systemctl --user daemon-reload 2>/dev/null || true
            systemctl --user enable crabkv.service 2>/dev/null || true
            systemctl --user start crabkv.service 2>/dev/null || true
            echo "Autostart configured via systemd user service"
            echo "Manage with: systemctl --user {start|stop|restart|status} crabkv"
            ;;
        macos)
            local plist_dir="$HOME/Library/LaunchAgents"
            mkdir -p "$plist_dir"
            cat > "$plist_dir/com.crabkv.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.crabkv</string>
    <key>ProgramArguments</key>
    <array>
        <string>${binary_path}</string>
        <string>-p</string>
        <string>${PORT}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/crabkv.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/crabkv.stderr.log</string>
</dict>
</plist>
EOF
            launchctl unload "$plist_dir/com.crabkv.plist" 2>/dev/null || true
            launchctl load "$plist_dir/com.crabkv.plist"
            echo "Autostart configured via launchd"
            echo "Manage with: launchctl {start|stop} com.crabkv"
            ;;
        windows)
            echo "For Windows autostart, add CrabKV to Startup folder:"
            echo "  Win+R shell:startup"
            echo "  Place a shortcut to ${binary_path} -p ${PORT}"
            ;;
    esac
}

main() {
    echo "CrabKV Installer"
    echo "================"
    echo ""

    local platform
    platform=$(detect_platform)
    local os="${platform%%-*}"
    echo "Detected platform: $platform"

    local url
    url=$(get_release_url "$platform" "$VERSION")
    echo "Download URL: $url"

    mkdir -p "$INSTALL_DIR"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    echo "Downloading..."
    local filename
    filename=$(basename "$url")

    if [[ "$os" == "windows" ]]; then
        # Windows: use PowerShell to download and extract
        local zip_path="$tmp_dir/$filename"
        curl -fsSL -o "$zip_path" "$url"
        powershell.exe -Command "Expand-Archive -Path '$zip_path' -DestinationPath '$tmp_dir' -Force"
        local binary_path="$INSTALL_DIR/crabkv.exe"
        cp "$tmp_dir/crabkv-windows-amd64.exe" "$binary_path"
    else
        curl -fsSL "$url" | tar xz -C "$tmp_dir"
        local binary_path="$INSTALL_DIR/crabkv"
        cp "$tmp_dir/crabkv-${platform#*-}" "$binary_path" 2>/dev/null || \
        cp "$tmp_dir/crabkv-${platform%%-*}-$(uname -m)" "$binary_path" 2>/dev/null || \
        cp "$tmp_dir"/crabkv* "$binary_path"
        chmod +x "$binary_path"
    fi

    echo "Installed to: $binary_path"

    # Add to PATH if needed
    if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
        echo ""
        echo "Add to your PATH:"
        echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
        echo ""
        echo "Or add to ~/.bashrc / ~/.zshrc for persistence"
    fi

    if [[ "$AUTOSTART" == true ]]; then
        setup_autostart "$binary_path"
    fi

    echo ""
    echo "Starting CrabKV on port $PORT..."
    if [[ "$os" == "windows" ]]; then
        start "" "$binary_path" -p "$PORT"
    else
        nohup "$binary_path" -p "$PORT" > /tmp/crabkv.log 2>&1 &
        echo "PID: $!"
    fi

    echo ""
    echo "CrabKV is running on port $PORT"
    echo "Connect with: redis-cli -p $PORT"
    echo "Stop with: kill $!"
}

main
