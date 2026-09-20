#!/bin/bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

echo "Building the project..."
mvn clean package

echo "Starting Code List Downloader (one-shot)..."
exec java -jar target/eu-codelist-downloader-1.0.0-jar-with-dependencies.jar --once "$@"
