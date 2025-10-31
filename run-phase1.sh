#!/bin/bash
echo "Running Phase 1 (Analysis)..."
echo ""

# Build and copy dependencies
mvn clean compile dependency:copy-dependencies -q

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Run the test
java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.TestPhases

if [ $? -ne 0 ]; then
    echo ""
    echo "Execution failed with error code: $?"
fi

echo ""
echo "Done! Check:"
echo "- Logs above for detected files"
echo "- src/main/resources/phase1/inventory-*.csv for timestamped CSV files"
echo "- src/main/resources/phase1/inventory-latest.csv for latest CSV file"

