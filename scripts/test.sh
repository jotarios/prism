#!/bin/bash
# Run Prism tests

cd "$(dirname "$0")/.."

echo "Running tests..."
cd prism
xcodebuild test -scheme prism -destination 'platform=macOS' -only-testing:prismTests/DatabaseTests 2>&1 | grep -E "Test case|passed|failed|BUILD"
