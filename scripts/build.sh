#!/bin/bash
# Build Prism

cd "$(dirname "$0")/.."

echo "Building Prism..."
xcodebuild build -scheme prism -destination 'platform=macOS' 2>&1 | grep -E "error:|warning:|BUILD"
