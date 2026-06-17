#!/usr/bin/env bash
# Translate (if PlusCal) and model-check a TLA+ spec with TLC on JDK 21.
# Usage: ./run.sh <SpecName> [extra TLC args]
set -euo pipefail
cd "$(dirname "$0")"
JH="$(/usr/libexec/java_home -v 21)"
SPEC="${1:?usage: run.sh <SpecName> [extra TLC args]}"
shift || true
if grep -qE -- '--algorithm|--fair algorithm' "$SPEC.tla" 2>/dev/null; then
  echo "## PlusCal translate: $SPEC.tla"
  "$JH/bin/java" -cp tla2tools.jar pcal.trans "$SPEC.tla" | tail -3 || true
fi
echo "## TLC: $SPEC (JDK $($JH/bin/java -version 2>&1 | head -1))"
exec "$JH/bin/java" -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -config "$SPEC.cfg" -workers auto "$@" "$SPEC.tla"
