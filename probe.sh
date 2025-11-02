BASE="${BASE:-https://plant-agent-i32khy5nrq-el.a.run.app}"
PATHS=(
  "/pm/trends?minutes=120&limit=240"
  "/pm/trends?minutes=120&limit=240&source=bq"
  "/trends?minutes=120&limit=240&source=pm"
  "/trends?minutes=120&limit=240&source=pm_bq"
  "/pm/segments?minutes=180"
  "/pm/segments?minutes=120"
  "/segments/pm?minutes=180"
)
for P in "${PATHS[@]}"; do
  CODE=$(/usr/bin/curl -sS -o /dev/null -w "%{http_code}" "$BASE$P")
  printf "%3s  %s\n" "$CODE" "$P"
done
echo "—"
echo "Example body (first working trends endpoint):"
for P in "${PATHS[@]}"; do
  CODE=$(/usr/bin/curl -sS -o /dev/null -w "%{http_code}" "$BASE$P")
  if [[ "$CODE" == "200" ]]; then
    /usr/bin/curl -sS "$BASE$P" | /usr/bin/python3 -m json.tool | head -n 40
    break
  fi
done
EOF