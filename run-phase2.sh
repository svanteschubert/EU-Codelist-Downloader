#!/bin/bash
echo "Running Phase 2 (Compare)..."
echo ""

# Build and copy dependencies
mvn clean compile dependency:copy-dependencies -q

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Run Phase 2
java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.PhaseRunner 2

if [ $? -ne 0 ]; then
    echo ""
    echo "Execution failed with error code: $?"
fi

echo ""
echo "Done! Check:"
echo "- src/main/resources/phase2/diff-*.csv for timestamped CSV files"
echo "- src/main/resources/phase2/diff-latest.csv for latest CSV file"

