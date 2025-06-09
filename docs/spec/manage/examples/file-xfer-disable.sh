curl -X PUT http://$HOST:8094/api/managerOptions \
-u $USER:$PASSWORD \
-H "Content-type:application/json" \
-d '{"disableFileTransferRebalance": "true" }'