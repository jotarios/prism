#!/bin/bash
# Build and run Prism

cd "$(dirname "$0")/.."

echo "Building Prism..."
xcodebuild -scheme prism -destination 'platform=macOS' 2>&1 | grep -E "BUILD"

if [ $? -eq 0 ]; then
    echo "Opening Prism..."
    open prism/prism.xcodeproj
fi
