#!/bin/sh
set -eu

export HOSTNAME="$FLY_MACHINE_ID.vm.$FLY_APP_NAME.internal"
export BIND_ADDRESS="$FLY_PRIVATE_IP"
export SERVER_ID="fly_server_$FLY_MACHINE_ID"

ALL_MACHINE_IDS=$(dig +short "all.vms.$FLY_APP_NAME.internal" TXT \
  | tr -d '"' \
  | tr ',' '\n' \
  | sed 's/ .*//')

out=""
sep=""
for ID in $ALL_MACHINE_IDS; do
  out="${out}${sep}${ID}.vm.$FLY_APP_NAME.internal"
  sep=","
done

export BOOTSTRAP_NODES="${out}"

# echo "bootstrap_nodes=$BOOTSTRAP_NODES"


map_region() {
  local input="$1"

  case "$input" in
    ams | cdg | fra | lhr | arn)
      echo "eu-west"
      ;;

    ewr | iad)
      echo "us-east"
      ;;

    lax | sjc)
      echo "us-west"
      ;;

    nrt | sin)
      echo "asia-east"
      ;;

    *)
      echo "Unsupported region: $input"
      return 1
      ;;
  esac
}

REGION="$(map_region "$FLY_REGION")" || exit 1
FLY_PRIMARY_REGION="$PRIMARY_REGION"
PRIMARY_REGION="$(map_region "$FLY_PRIMARY_REGION")" || exit 1

export REGION=$REGION
export PRIMARY_REGION=$PRIMARY_REGION
export VALID_REGIONS="eu-west,us-east,us-west,asia-east"

echo "region=$REGION"
echo "primary_region=$PRIMARY_REGION"
echo "valid_regions=$VALID_REGIONS"

PACKAGE=questly
BASE=$(dirname "$0")
COMMAND="${1-default}"

run() {
  exec erl \
    -pa "$BASE"/*/ebin \
    -eval "$PACKAGE@@main:run($PACKAGE)" \
    -noshell \
    -extra "$@"
}

shell() {
  erl -pa "$BASE"/*/ebin
}

case "$COMMAND" in
run)
  shift
  run "$@"
  ;;

shell)
  shell
  ;;

*)
  echo "usage:" >&2
  echo "  fly-start.sh \$COMMAND" >&2
  echo "" >&2
  echo "commands:" >&2
  echo "  run    Run the project main function" >&2
  echo "  shell  Run an Erlang shell" >&2
  exit 1
  ;;
esac
