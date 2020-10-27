#!/bin/bash

# Creates a new pod, builds the binary locally, copies it to the pod and starts the clone

set -ex

shard=$1

kubectlrw() {
  sqm --admin-rw kubectl -- "$@"
}
kubectl() {
  sqm --admin kubectl -- "$@"
}

namespace=${SQM_SERVICE}
job_id=$(date +%s)
name=${USER}-clone-${job_id}

cat <<EOF | kubectlrw -n $namespace apply -f -
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  labels:
    square-envoy-injection: enabled
    istio-envoy-injection: enabled
    square_task: cloner
    component: cloner
spec:
  containers:
  - name: cloner
    image: golang
    command: ["sleep"]
    args: ["86400"]
    ports:
    - name: metrics
      containerPort: 9102
      protocol: TCP
    - name: envoy-admin
      containerPort: 8081
      protocol: TCP
    - name: http-envoy-prom
      containerPort: 15090
      protocol: TCP
    volumeMounts:
    - mountPath: /root
      name: home-volume
    - mountPath: /etc/config
      name: config-files-volume
      readOnly: true
    - mountPath: /etc/config/db
      name: db-config
      readOnly: true
    - mountPath: /etc/secrets/db
      name: db-secrets
      readOnly: true
    - mountPath: /etc/secrets/service
      name: service-secrets-volume
      readOnly: true
    - mountPath: /etc/secrets/ssl
      name: ssl-secrets-volume
      readOnly: true
  volumes:
  - name: db-config
    configMap:
      defaultMode: 420
      name: db-config
  - name: db-secrets
    secret:
      defaultMode: 420
      secretName: db-secrets
  - name: service-secrets-volume
    secret:
      defaultMode: 420
      secretName: service-secrets
  - name: ssl-secrets-volume
    secret:
      defaultMode: 420
      secretName: ssl-secrets
  - name: config-files-volume
    configMap:
      defaultMode: 420
      name: config-files
  - name: home-volume
    emptyDir: {}
EOF

# TODO wait properly
sleep 30

GOOS=linux GOARCH=amd64 go build -o cloner ./cmd/cloner
sqm --admin-rw kubectl -- -n ${SQM_SERVICE} cp ./cloner "${name}":/root
# TODO lots of hardcoded stuff here still
sqm --admin-rw kubectl -- -n ${SQM_SERVICE} exec "${name}" -ti \
  /root/cloner -- \
  --source-type vitess --source-egress-socket @egress.sock \
  --source-host ${SQM_ENV}.franklin-vtgate.gns.square \
  --source-database ${shard} \
  --target-misk-datasource /etc/secrets/db/${SQM_SERVICE}-tidb5_config.yaml \
  clone --copy-schema
sqm --admin-rw kubectl -- -n ${SQM_SERVICE} delete pod "${name}"