#!/bin/bash
# Run the Code List Downloader application

echo "Building the project..."
mvn clean package

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

JAR_FILE="target/eu-codelist-downloader-1.0.0-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    echo "Build may have failed or produced a different filename."
    exit 1
fi

echo ""
echo "Starting Code List Downloader (one-shot)..."
echo ""

java -jar "$JAR_FILE" --once

