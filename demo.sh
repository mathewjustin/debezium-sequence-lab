#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

echo "This run tests whether Debezium advances an explicit PostgreSQL sequence during CDC."
echo
LAB_QUIET=1 SHOW_KAFKA_EVENT=1 "$ROOT_DIR/scripts/run.sh"
