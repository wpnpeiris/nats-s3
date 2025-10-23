#!/usr/bin/env bash
set -euo pipefail

# Suppress Python BrokenPipeError warnings from AWS CLI
export PYTHONWARNINGS="ignore::BrokenPipeError"

ENDPOINT="http://localhost:5222"
BUCKET="conformance-bucket-$(date +%s)"
TMPDIR="$(mktemp -d)"
PASS_COUNT=0
FAIL_COUNT=0

cleanup() {
  rm -rf "$TMPDIR" || true
}
trap cleanup EXIT

pass() { echo "PASS: $1"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo "FAIL: $1"; FAIL_COUNT=$((FAIL_COUNT+1)); }
run_or_fail() { if "$@"; then pass "$1 ${2-}"; else fail "$1 ${2-}"; fi }

echo "Endpoint: $ENDPOINT"
echo "Bucket:   $BUCKET"

# 1) Make bucket
if aws s3 mb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null; then
  pass "aws s3 mb"
else
  fail "aws s3 mb"; exit 1
fi

# 2) List buckets should include our bucket
buckets_output=$(aws s3 ls --endpoint-url="$ENDPOINT" 2>/dev/null)
if echo "$buckets_output" | grep -q "$BUCKET"; then
  pass "aws s3 ls (service)"
else
  fail "aws s3 ls (service)"; exit 1
fi

# 3) Put object
echo "hello world" > "$TMPDIR/file.txt"
if aws s3 cp "$TMPDIR/file.txt" "s3://$BUCKET/hello.txt" --endpoint-url="$ENDPOINT" >/dev/null; then
  pass "aws s3 cp put"
else
  fail "aws s3 cp put"; exit 1
fi

# 4) List bucket contents
if aws s3 ls "s3://$BUCKET" --endpoint-url="$ENDPOINT" 2>/dev/null | grep -q "hello.txt"; then
  pass "aws s3 ls (bucket)"
else
  fail "aws s3 ls (bucket)"; exit 1
fi

# 5) Head object via s3api
if aws s3api head-object --bucket "$BUCKET" --key "hello.txt" --endpoint-url="$ENDPOINT" >/dev/null; then
  pass "aws s3api head-object"
else
  fail "aws s3api head-object"; exit 1
fi

# 6) Get object and compare content
if aws s3 cp "s3://$BUCKET/hello.txt" "$TMPDIR/out.txt" --endpoint-url="$ENDPOINT" >/dev/null; then
  if diff -q "$TMPDIR/file.txt" "$TMPDIR/out.txt" >/dev/null; then
    pass "aws s3 cp get + content match"
  else
    fail "content mismatch after get"; exit 1
  fi
else
  fail "aws s3 cp get"; exit 1
fi

# 7) Delete object
if aws s3 rm "s3://$BUCKET/hello.txt" --endpoint-url="$ENDPOINT" >/dev/null; then
  pass "aws s3 rm"
else
  fail "aws s3 rm"; exit 1
fi

# 8) Remove bucket
if aws s3 rb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null; then
  pass "aws s3 rb"
else
  fail "aws s3 rb"; exit 1
fi

echo "Conformance subset summary: PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
if [ "$FAIL_COUNT" != "0" ]; then exit 1; fi

