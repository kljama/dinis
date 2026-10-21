#!/bin/sh
# Runs the prober tests that need real ICMP sockets (pkg/pinger/icmp_netns_test.go) in a
# private network namespace. No root is needed: unshare maps the user to root in a new user
# namespace, which owns the new network namespace.
#
# The namespace has the loopback interface, ping sockets allowed for this group, and veth0
# (10.99.0.1/24) whose peer has no address, so 10.99.0.77 does not answer ARP.
#
# Usage: scripts/netns-test.sh [go test flags], e.g. scripts/netns-test.sh -v -race
set -e
cd "$(dirname "$0")/.."
exec unshare --user --map-root-user --net sh -ec '
  ip link set lo up
  ip link add veth0 type veth peer name veth1
  ip addr add 10.99.0.1/24 dev veth0
  ip link set veth0 up
  ip link set veth1 up
  echo "0 0" > /proc/sys/net/ipv4/ping_group_range
  go test -tags netns -count=1 -run Netns "$@" ./pkg/pinger
' netns-test "$@"
