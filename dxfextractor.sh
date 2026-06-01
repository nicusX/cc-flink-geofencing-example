#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$SCRIPT_DIR/target/geofencing-example-1.0.jar"
M2_REPO="${HOME}/.m2/repository"
LOG4J_VERSION="2.25.3"

LOG4J_API="$M2_REPO/org/apache/logging/log4j/log4j-api/$LOG4J_VERSION/log4j-api-$LOG4J_VERSION.jar"
LOG4J_CORE="$M2_REPO/org/apache/logging/log4j/log4j-core/$LOG4J_VERSION/log4j-core-$LOG4J_VERSION.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found: $JAR" >&2
  echo "Run 'mvn package -DskipTests' first." >&2
  exit 1
fi

exec java -cp "$JAR:$LOG4J_API:$LOG4J_CORE" \
  io.confluent.example.geofencing.cli.Dxf2NamedAreaMapsCLI "$@"
