#!/bin/bash

# Creates a new pod, builds the binary locally, copies it to the pod and starts the clone

set -ex

shard=$1

dc=sjc1
account_id=833102219637
if [ "$SQM_ENV" == "production" ]; then
  account_id=912375853625
  dc=sjc2b
fi

#GOOS=linux GOARCH=amd64 go build -o cloner ./cmd/cloner
#sqm --admin-rw kubectl -- -n ${SQM_SERVICE} cp ./cloner $USER-adhoc:/root
sqm --admin-rw kubectl -- -n ${SQM_SERVICE} exec -ti $USER-adhoc /root/cloner -- \
  clone \
  --source-type vitess \
  --source-host ${SQM_ENV}.franklin-vtgate.gns.square \
  --source-egress-socket @egress.sock \
  --source-grpc-custom-header X-SQ-ENVOY-GNS-LABEL=${dc} \
  --source-database "${shard}@replica" \
  --target-misk-datasource /etc/secrets/db/${SQM_SERVICE}-tidb5_config.yaml \
  --writer-count 20 \
  --reader-count 20
