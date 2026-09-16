#!/bin/sh
# Exercise the pinned client's actual dispatch/ack path without provider access.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root/src"
go mod download go.mau.fi/whatsmeow
module_dir=$(go list -m -f '{{.Dir}}' go.mau.fi/whatsmeow)
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/gows-status-ack.XXXXXX")
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
cp -R "$module_dir/." "$test_dir/"
chmod -R u+w "$test_dir"
cp "$repo_root/tests/whatsmeow/status_ack_regression_test.go" "$test_dir/"
cd "$test_dir"
go test -count=1 -run '^(TestStatusMediaFrameAttemptsAcknowledgement|TestDecideAck)$' .
