#!/bin/sh
# Upgrades the comb on each host in turn, one at a time, with upgrade-comb.sh over ssh. It stops at
# the first host that does not end on the new comb, so a bad binary reaches one node only.
#
# usage: roll.sh NEW_BINARY HOST...
#
# ROLL_PAUSE is how many seconds to wait between hosts, 30 by default, so scout and the gates see
# a node taking cells again before the next one is cordoned. HIVE_COMB_BIN, HIVE_COMB_UNIT,
# HIVE_SOCKET, HIVECTL and HIVE_UP_WAIT are passed on to upgrade-comb.sh when set.
set -eu

[ $# -ge 2 ] || { echo "usage: roll.sh NEW_BINARY HOST..." >&2; exit 1; }
NEW=$1
shift
HERE=$(cd "$(dirname "$0")" && pwd)
PAUSE=${ROLL_PAUSE:-30}
ENV=
for v in HIVE_COMB_BIN HIVE_COMB_UNIT HIVE_SOCKET HIVECTL HIVE_UP_WAIT; do
  eval "x=\${$v:-}"
  [ -z "$x" ] || ENV="$ENV $v='$x'"
done

n=0
for host in "$@"; do
  [ $n -eq 0 ] || sleep "$PAUSE"
  n=$((n + 1))
  dir=/tmp/hivebox-upgrade.$$
  echo "== $host ($n of $#)"
  ssh "$host" "mkdir -p $dir"
  scp -q "$NEW" "$host:$dir/hive-comb"
  scp -q "$HERE/upgrade-comb.sh" "$host:$dir/upgrade-comb.sh"
  code=0
  ssh "$host" "$ENV sh $dir/upgrade-comb.sh $dir/hive-comb" || code=$?
  ssh "$host" "rm -rf $dir" || true
  if [ $code -ne 0 ]; then
    echo "stopped at $host, which exited $code. The hosts after it still run the old comb." >&2
    exit $code
  fi
done
echo "all $# hosts run the new comb"
