curl -X GET -u $USER:$PASSWORD \
  "http://$NODE1:8094/api/diag" > cbft-01.json

curl -X GET -u $USER:$PASSWORD \
  "http://$NODE2:8094/api/diag" > cbft-02.json

curl -X GET -u $USER:$PASSWORD \
  "http://$NODE3:8094/api/diag" > cbft-03.json