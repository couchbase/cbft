curl -XGET -H "Content-Type: application/json" \
  -u $USER:$PASSWORD \
  "http://$HOST:8094/api/query?longerThan=7s"