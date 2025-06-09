curl -XGET -H "Content-Type: application/json" \
-u $USER:$PASSWORD \
"http://$HOST:8094/api/query/index/DemoIndex1?longerThan=1ms"