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

sha=a80c28b900319e6e7b0e89f2a2d659f9df6ef857
namespace=${SQM_SERVICE}
job_id=$(date +%s)
job=${USER}-clone-${job_id}

cat <<EOF | kubectlrw -n $namespace apply -f -
---
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
spec:
  template:
    metadata:
      labels:
        square-envoy-injection: enabled
        istio-envoy-injection: enabled
        square_task: cloner
        component: cloner
    spec:
      restartPolicy: Never
      containers:
      - name: cloner
        image: 833102219637.dkr.ecr.us-east-1.amazonaws.com/cloner:${sha}
        command: ["/cloner"]
        args:
        - "--source-type"
        - "vitess"
        - "--source-egress-socket"
        - "@egress.sock"
        - "--source-host"
        - "${SQM_ENV}.franklin-vtgate.gns.square"
        - "--source-database"
        - "${shard}"
        - "--target-misk-datasource"
        - "/etc/secrets/db/${SQM_SERVICE}-tidb5_config.yaml"
        - "clone"
        - "--reader-count"
        - "5"
        - "--chunker-count"
        - "5"
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

echo Monitor progress by running:
echo   kubectl port-forward -n ${SQM_SERVICE}-tidb5 jobs/${job} 9102:9102
echo   open https://localhost:8289/metrics
echo   open https://localhost:8289/debug/pprof

sleep 2
kubectl -n ${namespace} describe jobs/${job}
sleep 3
kubectl -n ${namespace} logs -f jobs/${job} cloner

