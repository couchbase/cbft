curl -X PUT $BASEPATH/api/managerOptions \
-u $USER:$PASSWORD \
-H "Content-type:application/json" \
-d '{"disableFileTransferRebalance": "true" }'