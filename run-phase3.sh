#!/bin/bash
echo "Running Phase 3 (Download)..."
echo ""

# Build and copy dependencies
mvn clean compile dependency:copy-dependencies -q

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Run Phase 3
java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.PhaseRunner 3

if [ $? -ne 0 ]; then
    echo ""
    echo "Execution failed with error code: $?"
fi

echo ""
echo "Done! Check:"
echo "- src/main/resources/phase3/downloads-*.csv for timestamped CSV files"
echo "- src/main/resources/phase3/downloads-latest.csv for latest CSV file"

