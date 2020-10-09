#!/bin/bash

set -ex

kubectlrw() {
  sqm --admin-rw kubectl -- "$@"
}
kubectl() {
  sqm --admin kubectl -- "$@"
}

namespace=${SQM_SERVICE}
name=${USER}-adhoc

cat <<EOF | kubectlrw -n $namespace apply -f -
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  labels:
    square-envoy-injection: true
spec:
  containers:
    - name: main
      image: golang
      command: ["sleep"]
      args: ["86400"]
  volumes:
  - configMap:
      defaultMode: 420
      name: db-config
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
EOF
