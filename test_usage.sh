#!/bin/bash
# Test gorout usage tracking
cd /root/AI/GoRout

KEY=$(python3 -c 'import json; print(json.load(open("/root/.gorout/config.json"))["providers"][0]["api_keys"][0])')
echo "Using key: ${KEY:0:15}... (len=${#KEY})"

echo
echo "--- 4 requests through gorout ---"
for i in 1 2 3 4; do
  curl -s -X POST http://localhost:9988/openai/chat/completions \
    -H "Authorization: Bearer *** \
    -H "Content-Type: application/json" \
    --data-raw '{"model":"openrouter/auto","messages":[{"role":"user","content":"say hi in 1 word"}],"max_tokens":10}' \
    -o /tmp/r$i.json -w "req %i: HTTP=%{http_code} bytes=%{size_download}\n" "$i"
done

echo
echo "--- response 1 (first 400 bytes) ---"
head -c 400 /tmp/r1.json
echo
echo
echo "--- USAGE ---"
./gorout usage

echo
echo "--- USAGE RESET ---"
./gorout usage-reset

echo
echo "--- USAGE AFTER RESET ---"
./gorout usage
