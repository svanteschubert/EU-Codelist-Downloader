#!/bin/bash
echo "Running Registry Link Extractor..."
echo ""

# First compile and copy dependencies
mvn compile dependency:copy-dependencies -q

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Then run using java
java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.RegistryLinkExtractor

if [ $? -ne 0 ]; then
    echo ""
    echo "Execution failed with error code: $?"
fi

echo ""
echo "Done! Check src/main/resources/registry-links-analysis.txt"

