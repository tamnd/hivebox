#!/bin/sh
# Upgrades the comb on this node in place. It cordons the node, puts the new binary in, restarts
# the comb and checks the new one took back every cell, then lifts the cordon. The cells keep
# running throughout, since a comb that stops leaves them for the next one. If the new comb does
# not answer in time or could not take back a cell, the old binary goes back in and the node stays
# cordoned.
#
# usage: upgrade-comb.sh NEW_BINARY [--drain DURATION] [--stop-rest]
#
# --drain empties the node first, as `hivectl node drain --timeout DURATION` does, for when the
# host is to be rebooted after. It is not needed for the comb alone.
#
# The environment can change where things are:
#   HIVE_COMB_BIN    the comb's binary, /usr/local/bin/hive-comb
#   HIVE_COMB_UNIT   its systemd unit, hive-comb.service
#   HIVE_SOCKET      the comb's socket, /run/hivebox/comb.sock
#   HIVECTL          hivectl, from PATH by default
#   HIVE_UP_WAIT     seconds to wait for the new comb to answer, 120
#
# Exits 0 when the node runs the new comb, 1 when it was rolled back, and 2 when it could not be
# rolled back either and needs a person.
set -eu

BIN=${HIVE_COMB_BIN:-/usr/local/bin/hive-comb}
UNIT=${HIVE_COMB_UNIT:-hive-comb.service}
SOCK=${HIVE_SOCKET:-/run/hivebox/comb.sock}
CTL=${HIVECTL:-hivectl}
WAIT=${HIVE_UP_WAIT:-120}

say() { echo "upgrade-comb: $*" >&2; }
die() { say "$*"; exit 1; }
ms() { date +%s%3N; }

[ $# -ge 1 ] || die "usage: upgrade-comb.sh NEW_BINARY [--drain DURATION] [--stop-rest]"
NEW=$1
shift
DRAIN= STOP_REST=
while [ $# -gt 0 ]; do
  case $1 in
    --drain) [ $# -ge 2 ] || die "--drain needs a duration"; DRAIN=$2; shift 2 ;;
    --stop-rest) STOP_REST=--stop-rest; shift ;;
    *) die "unknown argument $1" ;;
  esac
done

# Reads `hivectl node status --shell` into S_ variables. Fails when the comb does not answer.
status() {
  out=$("$CTL" --socket "$SOCK" node status --shell 2>/dev/null) || return 1
  S_CORDONED= S_LIVE= S_VERSION= S_UP_MS= S_TAKEN_BACK= S_ENDED_WHILE_DOWN= S_LOST_ON_START=
  while IFS='=' read -r k v; do
    case $k in
      CORDONED|LIVE|VERSION|UP_MS|TAKEN_BACK|ENDED_WHILE_DOWN|LOST_ON_START) eval "S_$k=\$v" ;;
    esac
  done <<EOF
$out
EOF
  [ -n "$S_LIVE" ]
}

# Restarts the comb and waits until a comb that started after the restart answers. Sets DOWN_MS
# to how long nothing answered.
restart() {
  t0=$(ms)
  # A comb that kept failing may have hit systemd's start limit, which would refuse the restart.
  systemctl reset-failed "$UNIT" 2>/dev/null || true
  systemctl restart "$UNIT" || true
  while :; do
    now=$(ms)
    if status && [ "$S_UP_MS" -le $((now - t0)) ]; then
      DOWN_MS=$((now - t0))
      return 0
    fi
    [ $((now - t0)) -lt $((WAIT * 1000)) ] || return 1
    sleep 0.2
  done
}

NEW_VERSION=$("$NEW" --version 2>/dev/null) || die "$NEW does not run: $("$NEW" --version 2>&1 | head -1)"
NEW_VERSION=${NEW_VERSION#hive-comb }
[ -x "$BIN" ] || die "there is no comb at $BIN"
status || die "the comb on $SOCK does not answer, so there is nothing to upgrade in place"
OLD_VERSION=$S_VERSION
WAS_CORDONED=$S_CORDONED
START=$(ms)
say "node runs hive-comb $OLD_VERSION with $S_LIVE cells, upgrading to $NEW_VERSION"

if [ "$WAS_CORDONED" = 0 ]; then
  "$CTL" --socket "$SOCK" node cordon >/dev/null
fi
if [ -n "$DRAIN" ]; then
  # A drain that left cells running exits 1, and those cells go on under the new comb.
  "$CTL" --socket "$SOCK" node drain --timeout "$DRAIN" $STOP_REST >&2 || true
fi
status || die "the comb stopped answering before the restart"
LIVE=$S_LIVE

# The new binary goes in with a rename, so the comb's path is never half written.
cp "$NEW" "$BIN.new"
chmod 755 "$BIN.new"
cp -p "$BIN" "$BIN.prev"
mv -f "$BIN.new" "$BIN"

if restart && [ "$S_LOST_ON_START" = 0 ]; then
  if [ "$WAS_CORDONED" = 0 ]; then
    "$CTL" --socket "$SOCK" node uncordon >/dev/null
  fi
  say "now hive-comb $S_VERSION, in $(( $(ms) - START )) ms with the comb down $DOWN_MS ms." \
    "Of $LIVE cells it took back $S_TAKEN_BACK, and $S_ENDED_WHILE_DOWN ended on their own meanwhile"
  exit 0
fi

if [ -n "${S_LOST_ON_START:-}" ] && [ "$S_LOST_ON_START" != 0 ]; then
  say "the new comb could not take back $S_LOST_ON_START of $LIVE cells, so the old one goes back"
else
  say "the new comb did not answer within ${WAIT}s, so the old one goes back"
fi
mv -f "$BIN.prev" "$BIN"
if restart; then
  say "rolled back to hive-comb $S_VERSION in $(( $(ms) - START )) ms, which took back $S_TAKEN_BACK" \
    "cells and lost $S_LOST_ON_START. The node stays cordoned until someone looks"
  exit 1
fi
say "the old comb does not answer either. See journalctl -u $UNIT"
exit 2
