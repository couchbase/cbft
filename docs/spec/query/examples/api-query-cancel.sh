curl -X POST -H "Content-Type: application/json" -u $USER:$PASSWORD \
  "http://$HOST:8094/api/query/24/cancel" -d \
  '{ "uuid": "b91d75480470f979f65f04e8f20a1f7b" }'