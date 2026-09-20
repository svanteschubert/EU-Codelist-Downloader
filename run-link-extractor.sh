#!/bin/bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

echo "Running Registry Link Extractor..."
mvn compile dependency:copy-dependencies -q
exec java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.RegistryLinkExtractor "$@"
