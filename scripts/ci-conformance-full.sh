#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="http://localhost:5222"
BUCKET="fullconf-$(date +%s)-$RANDOM"
TMPDIR="$(mktemp -d)"
RESULTS="$PWD/conformance-full.txt"
PASS_COUNT=0
FAIL_COUNT=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
pass() { echo "PASS: $1" | tee -a "$RESULTS"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo "FAIL: $1" | tee -a "$RESULTS"; FAIL_COUNT=$((FAIL_COUNT+1)); }

cleanup() {
  rm -rf "$TMPDIR" || true
}
trap cleanup EXIT

run() {
  # run cmd; on success pass name; else fail name (do not exit)
  local name="$1"; shift
  if "$@"; then pass "$name"; else fail "$name"; fi
}

assert_grep() {
  # grep for pattern in file/stdin; usage: assert_grep name pattern file
  local name="$1"; local pattern="$2"; local target="$3"
  if grep -qE "$pattern" "$target"; then pass "$name"; else fail "$name"; fi
}

echo "AWS CLI full conformance against $ENDPOINT" | tee "$RESULTS"
echo "Bucket: $BUCKET" | tee -a "$RESULTS"

# ---------- Bucket lifecycle ----------
run "bucket.create" aws s3 mb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null

# Duplicate create should fail
if aws s3 mb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null 2>"$TMPDIR/mb2.err"; then
  fail "bucket.create.duplicate_should_fail"
else
  pass "bucket.create.duplicate_should_fail"
fi

# Service list includes bucket
aws s3 ls --endpoint-url="$ENDPOINT" >"$TMPDIR/svc.txt" || true
assert_grep "service.list.contains_bucket" "$BUCKET" "$TMPDIR/svc.txt"

# Non-existent bucket list should fail
if aws s3 ls "s3://no-such-$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null 2>"$TMPDIR/ls-no.err"; then
  fail "bucket.list_nonexistent_should_fail"
else
  pass "bucket.list_nonexistent_should_fail"
fi

# ---------- Object basic ops ----------
echo "hello world" > "$TMPDIR/hello.txt"
run "object.put" aws s3 cp "$TMPDIR/hello.txt" "s3://$BUCKET/hello.txt" --endpoint-url="$ENDPOINT" >/dev/null

aws s3 ls "s3://$BUCKET" --endpoint-url="$ENDPOINT" >"$TMPDIR/blist.txt" || true
assert_grep "bucket.list_contains_object" "hello.txt" "$TMPDIR/blist.txt"

run "object.head" aws s3api head-object --bucket "$BUCKET" --key hello.txt --endpoint-url="$ENDPOINT" >/dev/null

run "object.get" aws s3 cp "s3://$BUCKET/hello.txt" "$TMPDIR/out.txt" --endpoint-url="$ENDPOINT" >/dev/null
if diff -q "$TMPDIR/hello.txt" "$TMPDIR/out.txt" >/dev/null; then pass "object.get_content_match"; else fail "object.get_content_match"; fi

# Head of non-existent should fail
if aws s3api head-object --bucket "$BUCKET" --key missing.txt --endpoint-url="$ENDPOINT" >/dev/null 2>"$TMPDIR/head-miss.err"; then
  fail "object.head_missing_should_fail"
else
  pass "object.head_missing_should_fail"
fi

# ---------- Metadata and content-type ----------
echo "meta file" > "$TMPDIR/meta.txt"
run "object.put_with_metadata" aws s3api put-object \
  --bucket "$BUCKET" --key meta.txt \
  --body "$TMPDIR/meta.txt" \
  --content-type text/plain \
  --metadata foo=bar,baz=qux \
  --endpoint-url="$ENDPOINT" >/dev/null

aws s3api head-object --bucket "$BUCKET" --key meta.txt --endpoint-url="$ENDPOINT" >"$TMPDIR/head-meta.json" || true
assert_grep "object.head_has_content_type" '"ContentType":\s*"text/plain"' "$TMPDIR/head-meta.json"
assert_grep "object.head_has_metadata_foo" '"Foo":\s*"bar"' "$TMPDIR/head-meta.json"

# ---------- Copy object ----------
run "object.copy" aws s3api copy-object --bucket "$BUCKET" --copy-source "$BUCKET/hello.txt" --key copy.txt --endpoint-url="$ENDPOINT" >/dev/null
run "object.get_copy" aws s3 cp "s3://$BUCKET/copy.txt" "$TMPDIR/copy.txt" --endpoint-url="$ENDPOINT" >/dev/null
if diff -q "$TMPDIR/hello.txt" "$TMPDIR/copy.txt" >/dev/null; then pass "object.copy_content_match"; else fail "object.copy_content_match"; fi

# ---------- Prefixes and delimiter ----------
mkdir -p "$TMPDIR/dirs"
echo a > "$TMPDIR/dirs/dir1-file1"
echo b > "$TMPDIR/dirs/dir1-file2"
echo c > "$TMPDIR/dirs/dir2-file1"
aws s3 cp "$TMPDIR/dirs/dir1-file1" "s3://$BUCKET/dir1/file1" --endpoint-url="$ENDPOINT" >/dev/null || true
aws s3 cp "$TMPDIR/dirs/dir1-file2" "s3://$BUCKET/dir1/file2" --endpoint-url="$ENDPOINT" >/dev/null || true
aws s3 cp "$TMPDIR/dirs/dir2-file1" "s3://$BUCKET/dir2/file1" --endpoint-url="$ENDPOINT" >/dev/null || true

# list-objects-v2 with delimiter
aws s3api list-objects-v2 --bucket "$BUCKET" --delimiter '/' --endpoint-url="$ENDPOINT" --output json >"$TMPDIR/list-delim.json" || true
assert_grep "list.delimiter_commonprefixes_dir1" '"Prefix":\s*"dir1/"' "$TMPDIR/list-delim.json"
assert_grep "list.delimiter_commonprefixes_dir2" '"Prefix":\s*"dir2/"' "$TMPDIR/list-delim.json"

# ---------- Range GET ----------
dd if=/dev/zero bs=1 count=64 of="$TMPDIR/range.src" >/dev/null 2>&1 || true
aws s3 cp "$TMPDIR/range.src" "s3://$BUCKET/range.bin" --endpoint-url="$ENDPOINT" >/dev/null || true
aws s3api get-object --bucket "$BUCKET" --key range.bin --range bytes=0-9 --endpoint-url="$ENDPOINT" "$TMPDIR/range.out" >/dev/null 2>&1 || true
if [ -f "$TMPDIR/range.out" ] && [ "$(wc -c < "$TMPDIR/range.out")" -eq 10 ]; then pass "object.get_range_0_9"; else fail "object.get_range_0_9"; fi

# ---------- Multipart upload ----------
dd if=/dev/zero of="$TMPDIR/large.bin" bs=1M count=6 >/dev/null 2>&1 || true
UPLOAD_ID=$(aws s3api create-multipart-upload --bucket "$BUCKET" --key mpu.bin --endpoint-url="$ENDPOINT" --query UploadId --output text 2>/dev/null || true)
  if [ -n "${UPLOAD_ID:-}" ] && [ "$UPLOAD_ID" != "None" ]; then
  head -c 3145728 "$TMPDIR/large.bin" > "$TMPDIR/part1.bin" 2>/dev/null || true
  tail -c +3145729 "$TMPDIR/large.bin" > "$TMPDIR/part2.bin" 2>/dev/null || true
  ETAG1=$(aws s3api upload-part --bucket "$BUCKET" --key mpu.bin --part-number 1 --body "$TMPDIR/part1.bin" --upload-id "$UPLOAD_ID" --endpoint-url="$ENDPOINT" --query ETag --output text 2>/dev/null | tr -d '"' || true)
  ETAG2=$(aws s3api upload-part --bucket "$BUCKET" --key mpu.bin --part-number 2 --body "$TMPDIR/part2.bin" --upload-id "$UPLOAD_ID" --endpoint-url="$ENDPOINT" --query ETag --output text 2>/dev/null | tr -d '"' || true)
  if [ -n "${ETAG1:-}" ] && [ -n "${ETAG2:-}" ]; then
    cat >"$TMPDIR/parts.json" <<EOF
{"Parts":[{"ETag":"$ETAG1","PartNumber":1},{"ETag":"$ETAG2","PartNumber":2}]}
EOF
    if aws s3api complete-multipart-upload --bucket "$BUCKET" --key mpu.bin --upload-id "$UPLOAD_ID" --multipart-upload file://"$TMPDIR/parts.json" --endpoint-url="$ENDPOINT" >/dev/null 2>&1; then
      pass "mpu.complete"
      # head object to confirm presence
      if aws s3api head-object --bucket "$BUCKET" --key mpu.bin --endpoint-url="$ENDPOINT" >/dev/null 2>&1; then pass "mpu.head_after_complete"; else fail "mpu.head_after_complete"; fi
    else
      fail "mpu.complete"
    fi
  else
    fail "mpu.upload_parts"
  fi
else
  fail "mpu.create"
fi

# ---------- Delete multiple objects ----------
echo 1 > "$TMPDIR/d1"; echo 2 > "$TMPDIR/d2"
aws s3 cp "$TMPDIR/d1" "s3://$BUCKET/del1" --endpoint-url="$ENDPOINT" >/dev/null || true
aws s3 cp "$TMPDIR/d2" "s3://$BUCKET/del2" --endpoint-url="$ENDPOINT" >/dev/null || true
cat >"$TMPDIR/delete.json" <<EOF
{"Objects":[{"Key":"del1"},{"Key":"del2"}],"Quiet":false}
EOF
run "objects.delete_batch" aws s3api delete-objects --bucket "$BUCKET" --delete file://"$TMPDIR/delete.json" --endpoint-url="$ENDPOINT" >/dev/null

# ---------- Presigned URL ----------
echo "signed" > "$TMPDIR/signed.txt"
aws s3 cp "$TMPDIR/signed.txt" "s3://$BUCKET/signed.txt" --endpoint-url="$ENDPOINT" >/dev/null || true
URL=$(aws s3 presign "s3://$BUCKET/signed.txt" --endpoint-url="$ENDPOINT" --expires-in 60 2>/dev/null || true)
if [ -n "${URL:-}" ]; then
  BODY=$(curl -fsSL "$URL" 2>/dev/null || true)
  if [ "$BODY" = "signed" ]; then pass "presign.get"; else fail "presign.get"; fi
else
  fail "presign.generate"
fi

# ---------- Bucket deletion semantics ----------
# Non-empty bucket removal should fail
aws s3 cp "$TMPDIR/hello.txt" "s3://$BUCKET/tmp.txt" --endpoint-url="$ENDPOINT" >/dev/null || true
if aws s3 rb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null 2>"$TMPDIR/rb-nonempty.err"; then
  fail "bucket.remove_nonempty_should_fail"
else
  pass "bucket.remove_nonempty_should_fail"
fi
# cleanup and delete
aws s3 rm "s3://$BUCKET" --recursive --endpoint-url="$ENDPOINT" >/dev/null 2>&1 || true
run "bucket.remove" aws s3 rb "s3://$BUCKET" --endpoint-url="$ENDPOINT" >/dev/null

echo "Summary: PASS=$PASS_COUNT FAIL=$FAIL_COUNT" | tee -a "$RESULTS"
echo "Detailed results saved to: $RESULTS"

# Exit non-zero if any test failed
if [ "$FAIL_COUNT" -ne 0 ]; then exit 1; fi
