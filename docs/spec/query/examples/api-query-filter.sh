curl -XGET -H "Content-Type: application/json" \
  -u $USER:$PASSWORD \
  "$BASEPATH/api/query?longerThan=7s"