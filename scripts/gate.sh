#!/usr/bin/env bash
# The CI-equivalent gate from AGENTS.md: clippy in all three feature
# configurations, the S3 build, and the test suite in all three. Logs to $1
# (default target/gate.log).
#
# A step passes only if its exit status is 0 AND, for `cargo test`, every test
# binary it ran printed `test result: ok`. Checking for the absence of FAILED,
# or piping through tail, has hidden a failing binary before (AGENTS.md).
set -uo pipefail
cd "$(dirname "$0")/.."
log=${1:-target/gate.log}
mkdir -p "$(dirname "$log")"
: >"$log"
fail=0

step() {
    local out rc
    out=$(mktemp)
    echo "== $*" | tee -a "$log"
    "$@" >"$out" 2>&1
    rc=$?
    cat "$out" >>"$log"
    if [[ $rc -eq 0 && "$1 $2" == "cargo test" ]]; then
        local ran ok
        ran=$(grep -cE '^ +(Running|Doc-tests) ' "$out" || true)
        ok=$(grep -c 'test result: ok' "$out" || true)
        if [[ "$ran" -ne "$ok" ]]; then
            echo "   $ran test binaries ran but $ok reported 'test result: ok'" | tee -a "$log"
            rc=1
        fi
    fi
    rm -f "$out"
    echo "== rc=$rc $*" | tee -a "$log"
    [[ $rc -ne 0 ]] && fail=1
    return 0
}

step cargo clippy --all-targets -- -D warnings
step cargo clippy --all-targets --features unsafe-fastpath -- -D warnings
step cargo clippy --all-targets --no-default-features -- -D warnings
step cargo build --features s3
step cargo test
step cargo test --features unsafe-fastpath
step cargo test --no-default-features

echo "GATE fail=$fail (log: $log)"
exit $fail
