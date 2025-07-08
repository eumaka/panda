#!/bin/bash

echo "Running on $(hostname)"
echo "Start time: $(date)"

if [ $# -ne 1 ]; then
    echo "[ERROR] Usage: $0 '<stf_json>'"
    exit 1
fi

STF_JSON="$1"

echo "[INFO] Received STF JSON:"
echo "$STF_JSON"

# Check jq
if ! command -v jq &>/dev/null; then
    echo "[ERROR] jq is not installed!"
    exit 1
fi

FILENAME=$(echo "$STF_JSON" | jq -r '.filename')
START=$(echo "$STF_JSON" | jq -r '.start')
END=$(echo "$STF_JSON" | jq -r '.end')
STATE=$(echo "$STF_JSON" | jq -r '.state')
SUBST=$(echo "$STF_JSON" | jq -r '.subst')
TYPE=$(echo "$STF_JSON" | jq -r '.type')
RID=$(echo "$STF_JSON" | jq -r '.rid')

echo "[INFO] Metadata:"
echo "  filename: $FILENAME"
echo "  start:    $START"
echo "  end:      $END"
echo "  state:    $STATE"
echo "  subst:    $SUBST"
echo "  type:     $TYPE"
echo "  rid:      $RID"

cat > myout.txt <<EOF
Processed STF:
  filename: $FILENAME
  start:    $START
  end:      $END
  state:    $STATE
  subst:    $SUBST
  type:     $TYPE
  rid:      $RID
EOF

echo "[INFO] Output written to myout.txt"
echo "Done at: $(date)"
