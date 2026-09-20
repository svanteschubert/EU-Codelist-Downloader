#!/bin/bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

echo "Running Phase 2..."
mvn compile dependency:copy-dependencies -q
exec java -cp "target/classes:target/dependency/*" org.standict.codelist.shared.PhaseRunner 2 "$@"
