#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

# Upgrades TRAM from a release to the code of this repository in the cluster
# of the current kubectl context, and checks that it keeps running and that
# events buffered on a volume survive the upgrade:
#
#   1. install the release's chart, and an Engine with storage whose streams
#      buffer what clients send, for a receiver that does not exist yet
#   2. send records, which wait in the queue on the volume
#   3. upgrade to the chart and image of this repository
#   4. check that the Engine runs the new image, its streams are mounted again
#      and the CRDs are those of the repository
#   5. create the receiver and check that every buffered record arrives
#
# The images have to be in the cluster already, e.g. loaded with kind load,
# as both are used with imagePullPolicy IfNotPresent. `task test:upgrade` sets
# up a kind cluster and runs this; .github/workflows/upgrade.yml does in CI.
#
# Environment:
#   FROM_VERSION  release to upgrade from, without v, e.g. 0.1.0 (required)
#   IMAGE         repository of the images (default ghcr.io/go-tram/tram)
#   NEW_TAG       tag of the image built from this repository (default upgrade-test)
#   RECORDS       number of records to buffer (default 20)

set -euo pipefail

: "${FROM_VERSION:?set FROM_VERSION to the release to upgrade from, e.g. 0.1.0}"
IMAGE="${IMAGE:-ghcr.io/go-tram/tram}"
NEW_TAG="${NEW_TAG:-upgrade-test}"
RECORDS="${RECORDS:-20}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
ns=upgrade-test
system=tram-system
port=14318

step() { printf '\n==> %s\n' "$*"; }

diagnose() {
  step "Diagnostics"
  kubectl get engines,streams,statefulsets,deployments,pods,pvc,jobs -n "${ns}" -o wide || true
  kubectl get streams -n "${ns}" -o yaml || true
  kubectl describe engines -n "${ns}" || true
  kubectl logs -n "${system}" -l app.kubernetes.io/name=tram --tail=200 --all-containers || true
  kubectl logs -n "${ns}" -l tram.io/engine --tail=200 --prefix || true
}
trap 'status=$?; if [ "${status}" -ne 0 ]; then diagnose; fi' EXIT

# wait_for runs the command until it succeeds or the timeout in seconds ends.
wait_for() {
  local timeout=$1 description=$2
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      echo "Timed out after ${timeout}s waiting for ${description}" >&2
      return 1
    fi
    sleep 2
  done
}

engine_runs_image() {
  local image
  image="$(kubectl get statefulset tram-engine-blue-line -n "${ns}" \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
  [ "${image}" = "$1" ]
}

mounted() {
  [ "$(kubectl get stream "$1" -n "${ns}" -o jsonpath='{.status.mounted}')" = "true" ]
}

step "Install release ${FROM_VERSION}"
helm install tram oci://ghcr.io/go-tram/charts/tram --version "${FROM_VERSION}" \
  --namespace "${system}" --create-namespace \
  --set image.pullPolicy=IfNotPresent --wait --timeout 3m

step "Create an Engine with storage whose streams buffer for a receiver that does not exist yet"
kubectl create namespace "${ns}"
kubectl apply -f "${here}/buffered.yaml"
wait_for 120 "the Engine's StatefulSet" kubectl get statefulset tram-engine-blue-line -n "${ns}"
kubectl rollout status statefulset/tram-engine-blue-line -n "${ns}" --timeout=3m
wait_for 120 "the writer to be mounted" mounted writer
wait_for 120 "the reader to be mounted" mounted reader

step "Buffer ${RECORDS} records"
records=""
for i in $(seq 1 "${RECORDS}"); do
  records+="${records:+,}{\"body\":{\"stringValue\":\"upgrade-record-${i}\"}}"
done
payload="{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[${records}]}]}]}"
kubectl port-forward -n "${ns}" svc/tram-engine-blue-line "${port}:4318" >/dev/null &
forward=$!
wait_for 60 "the port-forward" curl -s -o /dev/null "http://127.0.0.1:${port}/"
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  --data "${payload}" "http://127.0.0.1:${port}/v1/logs")"
kill "${forward}"
if [ "${code}" != "200" ]; then
  echo "The writer answered ${code} instead of 200" >&2
  exit 1
fi
echo "Stored ${RECORDS} records; the reader retries them while the receiver is missing"

step "Upgrade to this repository's chart and image ${IMAGE}:${NEW_TAG}"
kubectl apply --server-side --force-conflicts -f "${repo}/charts/tram/crds/"
helm upgrade tram "${repo}/charts/tram" --namespace "${system}" \
  --set image.repository="${IMAGE}" --set image.tag="${NEW_TAG}" --set image.pullPolicy=IfNotPresent \
  --wait --timeout 3m

step "Check the Engine, its streams and the CRDs"
wait_for 180 "the Engine to run ${IMAGE}:${NEW_TAG}" engine_runs_image "${IMAGE}:${NEW_TAG}"
kubectl rollout status statefulset/tram-engine-blue-line -n "${ns}" --timeout=3m
wait_for 120 "the writer to be mounted" mounted writer
wait_for 120 "the reader to be mounted" mounted reader
if ! kubectl diff --server-side -f "${repo}/charts/tram/crds/" >/dev/null; then
  echo "The CRDs in the cluster differ from charts/tram/crds after the upgrade" >&2
  kubectl diff --server-side -f "${repo}/charts/tram/crds/" || true
  exit 1
fi
echo "The Engine runs the new image, its streams are mounted and the CRDs are up to date"

step "Create the receiver and wait for the buffered records"
kubectl apply -f "${here}/receiver.yaml"
wait_for 120 "the receiver's Deployment" kubectl get deployment tram-engine-receiver -n "${ns}"
kubectl rollout status deployment/tram-engine-receiver -n "${ns}" --timeout=3m
wait_for 120 "the receiver stream to be mounted" mounted receiver

delivered() {
  local n
  n="$(kubectl logs -n "${ns}" -l tram.io/engine=receiver --tail=-1 2>/dev/null |
    grep -o 'upgrade-record-[0-9]*' | sort -u | wc -l | tr -d ' ')"
  echo "Delivered ${n} of ${RECORDS} records"
  [ "${n}" -eq "${RECORDS}" ]
}
deadline=$((SECONDS + 240))
until delivered; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "Not every buffered record arrived after the upgrade" >&2
    exit 1
  fi
  sleep 5
done

step "Upgrade from ${FROM_VERSION} passed: the buffered records survived it"
