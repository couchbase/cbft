curl -XGET -H "Content-Type: application/json" \
-u $USER:$PASSWORD \
"$BASEPATH/api/query/index/DemoIndex1?longerThan=1ms"