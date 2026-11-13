#!/usr/bin/env bash
# End-to-end test of the ntcp transport over a TUN device.
#
# Runs in a fresh unprivileged user + network namespace (`unshare -rn`), so it
# needs no root and never touches the host's network: the namespace, its TUN
# devices and every process vanish when the script exits.
#
# Inside the namespace:
#   ntcp0  10.73.0.1/24 (kernel)  <->  10.73.0.2 (ntcp, thread 0)
#   ntcp1  10.74.0.1/24 (kernel)  <->  10.74.0.2 (ntcp, thread 1)
# xylem-echo-server listens on 0.0.0.0:9999. Each scenario runs xylem with
# the xylem-echo protocol in closed loop, once over ntcp and once over kernel
# TCP, and prints throughput and p50/p99 from the JSON results. A run fails
# if it completes no requests, exits non-zero or logs a warning or error.
#
# Usage: tests/ntcp/tun_netns.sh            (builds, then runs both scenarios)
# Environment: NTCP_PROFILE=release|dev (default release), NTCP_DURATION
# (default 3s), NTCP_CONNECTIONS (per thread, default 4).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE="${NTCP_PROFILE:-release}"
DURATION="${NTCP_DURATION:-3s}"
CONNECTIONS="${NTCP_CONNECTIONS:-4}"
PORT=9999
case "$PROFILE" in
release) TARGET_DIR="$ROOT/target/release" ;;
dev) TARGET_DIR="$ROOT/target/debug" ;;
*)
    echo "NTCP_PROFILE must be release or dev" >&2
    exit 2
    ;;
esac

if [[ "${1:-}" != "--inside" ]]; then
    for tool in cargo unshare ip python3; do
        if ! command -v "$tool" >/dev/null; then
            echo "$tool not found: run this in the flake's devShell (nix develop)" >&2
            exit 1
        fi
    done
    if ! unshare --user --map-root-user --net true; then
        echo "cannot create an unprivileged user + network namespace;" \
            "the kernel must allow them (user.max_user_namespaces," \
            "kernel.unprivileged_userns_clone, AppArmor's" \
            "kernel.apparmor_restrict_unprivileged_userns)" >&2
        exit 1
    fi
    if [[ ! -c /dev/net/tun ]]; then
        echo "/dev/net/tun is missing: the kernel needs TUN support" >&2
        exit 1
    fi
    echo "Building xylem (--features ntcp) and xylem-echo-server ($PROFILE)..."
    (cd "$ROOT" && cargo build --quiet --profile "$PROFILE" --features ntcp \
        -p xylem-cli -p xylem-echo-server)
    export NTCP_PARENT_NETNS
    NTCP_PARENT_NETNS="$(readlink /proc/self/ns/net)"
    exec unshare --user --map-root-user --net "$0" --inside
fi

# ---- inside the namespace --------------------------------------------------

if [[ "$(readlink /proc/self/ns/net)" == "${NTCP_PARENT_NETNS:-}" ]]; then
    echo "refusing to run: not in a new network namespace" >&2
    exit 1
fi

XYLEM="$TARGET_DIR/xylem"
ECHO="$TARGET_DIR/xylem-echo-server"
WORK="$(mktemp -d /tmp/xylem-ntcp-e2e.XXXXXX)"
SERVER_PID=""
cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

ip link set lo up
for i in 0 1; do
    # `ip tuntap add` creates IFF_TUN | IFF_NO_PI, as Tun::open requires
    ip tuntap add dev "ntcp$i" mode tun
    ip addr add "10.7$((3 + i)).0.1/24" dev "ntcp$i"
    ip link set "ntcp$i" mtu 1500 up
done

"$ECHO" --bind 0.0.0.0 --port "$PORT" --threads 2 >"$WORK/echo.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 50); do
    if (exec 3<>"/dev/tcp/10.73.0.1/$PORT") 2>/dev/null; then
        break
    fi
    sleep 0.1
done
kill -0 "$SERVER_PID"

# profile NAME TRANSPORT THREADS TARGET_IP
profile() {
    local name=$1 transport=$2 threads=$3 target=$4
    cat <<EOF
[experiment]
name = "$name"
duration = "$DURATION"

[[traffic_groups]]
name = "echo"
protocol = "xylem-echo"
target = "$target:$PORT"
transport = "$transport"
threads = $threads
connections_per_thread = $CONNECTIONS
max_pending_per_connection = 1
traffic_policy = { type = "closed-loop" }

[ntcp]
backend = "tun"
mtu = 1500
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },
    { ifname = "ntcp1", local_ip = "10.74.0.2" },
]

[output]
format = "json"
file = "$WORK/$name.json"
EOF
}

FAILED=0
# run NAME TRANSPORT THREADS NTCP_ENDPOINTS
run() {
    local name=$1 transport=$2 threads=$3 endpoints=$4
    profile "$name" "$transport" "$threads" 10.73.0.1 >"$WORK/$name.toml"
    # Keep only as many endpoints as threads run ntcp
    if [[ "$endpoints" == 1 ]]; then
        sed -i '/ntcp1/d' "$WORK/$name.toml"
    fi
    local status=0
    "$XYLEM" -P "$WORK/$name.toml" >"$WORK/$name.out" 2>"$WORK/$name.err" || status=$?
    python3 - "$name" "$status" "$WORK/$name.json" "$WORK/$name.err" <<'EOF' || FAILED=1
import json, re, sys
name, status, result, log = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
problems = []
with open(log) as f:
    warnings = [l.rstrip() for l in f if re.search(r"\b(WARN|ERROR)\b", l)]
if status != 0:
    problems.append(f"exit status {status}")
if warnings:
    problems.append(f"{len(warnings)} warning/error log line(s)")
try:
    with open(result) as f:
        g = json.load(f)["global"]
except Exception as e:
    g = None
    problems.append(f"no JSON results ({e})")
if g is not None and g["total_requests"] == 0:
    problems.append("no requests completed")
if g is not None:
    lat = g["latency"]
    print(f"{name:<14} {g['total_requests']:>10} req  {g['throughput_rps']:>11.0f} req/s  "
          f"p50 {lat['p50_us']:>8.1f} us  p99 {lat['p99_us']:>8.1f} us")
if problems:
    print(f"{name}: FAILED: {'; '.join(problems)}")
    for line in warnings[:10]:
        print("   ", line)
    if status != 0:
        with open(log) as f:
            print("    stderr tail:", *f.readlines()[-10:], sep="\n    ")
    sys.exit(1)
EOF
}

echo
echo "xylem-echo, closed loop, $CONNECTIONS connections per thread, $DURATION"
echo "-------------------------------------------------------------------------------"
run ntcp-1thread ntcp "[0]" 1
run tcp-1thread tcp "[0]" 1
run ntcp-2threads ntcp "[0, 1]" 2
run tcp-2threads tcp "[0, 1]" 2

if [[ "$FAILED" != 0 ]]; then
    echo "FAILED" >&2
    exit 1
fi
echo "PASSED"
