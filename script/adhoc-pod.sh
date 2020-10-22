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
