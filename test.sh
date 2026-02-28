#!/usr/bin/env bash
set -euo pipefail

# test.sh - Run pgx tests against specific database targets
#
# Usage:
#   ./test.sh [target] [go test flags...]
#
# Targets:
#   pg14    - PostgreSQL 14 (port 5414)
#   pg15    - PostgreSQL 15 (port 5415)
#   pg16    - PostgreSQL 16 (port 5416)
#   pg17    - PostgreSQL 17 (port 5417)
#   pg18    - PostgreSQL 18 (port 5432) [default]
#   crdb    - CockroachDB (port 26257)
#   all     - Run against all targets sequentially
#
# Examples:
#   ./test.sh                          # Test against PG18
#   ./test.sh pg14                     # Test against PG14
#   ./test.sh crdb                     # Test against CockroachDB
#   ./test.sh all                      # Test against all targets
#   ./test.sh pg16 -run TestConnect    # Test specific test against PG16
#   ./test.sh pg18 -count=1 -v         # Verbose, no cache, PG18

# Color output (disabled if not a terminal)
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    GREEN=''
    RED=''
    BLUE=''
    NC=''
fi

log_info()  { echo -e "${BLUE}==> $*${NC}"; }
log_ok()    { echo -e "${GREEN}==> $*${NC}"; }
log_err()   { echo -e "${RED}==> $*${NC}" >&2; }

# Wait for a database to accept connections
wait_for_ready() {
    local connstr="$1"
    local label="$2"
    local max_attempts=30
    local attempt=0

    log_info "Waiting for $label to be ready..."
    while ! psql "$connstr" -c "SELECT 1" > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge "$max_attempts" ]; then
            log_err "$label did not become ready after $max_attempts attempts"
            return 1
        fi
        sleep 1
    done
    log_ok "$label is ready"
}

# Directory containing this script (used to locate testsetup/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="$SCRIPT_DIR/testsetup/certs"

# Copy client certificates to /tmp for TLS tests
setup_client_certs() {
    if [ -d "$CERTS_DIR" ]; then
        base64 -d "$CERTS_DIR/ca.pem.b64" > /tmp/ca.pem
        base64 -d "$CERTS_DIR/pgx_sslcert.crt.b64" > /tmp/pgx_sslcert.crt
        base64 -d "$CERTS_DIR/pgx_sslcert.key.b64" > /tmp/pgx_sslcert.key
    fi
}

# Initialize CockroachDB (create database if not exists)
init_crdb() {
    local connstr="postgresql://root@localhost:26257/?sslmode=disable"
    wait_for_ready "$connstr" "CockroachDB"
    log_info "Ensuring pgx_test database exists on CockroachDB..."
    psql "$connstr" -c "CREATE DATABASE IF NOT EXISTS pgx_test" 2>/dev/null || true
}

# Run tests against a single target
run_tests() {
    local target="$1"
    shift
    local extra_args=("$@")

    local label=""
    local port=""

    case "$target" in
        pg14) label="PostgreSQL 14"; port=5414 ;;
        pg15) label="PostgreSQL 15"; port=5415 ;;
        pg16) label="PostgreSQL 16"; port=5416 ;;
        pg17) label="PostgreSQL 17"; port=5417 ;;
        pg18) label="PostgreSQL 18"; port=5432 ;;
        crdb)
            label="CockroachDB (port 26257)"
            init_crdb
            log_info "Testing against $label"
            if ! PGX_TEST_DATABASE="postgresql://root@localhost:26257/pgx_test?sslmode=disable&experimental_enable_temp_tables=on" \
                go test -count=1 "${extra_args[@]}" ./...; then
                log_err "Tests FAILED against $label"
                return 1
            fi
            log_ok "Tests passed against $label"
            return 0
            ;;
        *)
            log_err "Unknown target: $target"
            log_err "Valid targets: pg14, pg15, pg16, pg17, pg18, crdb, all"
            return 1
            ;;
    esac

    setup_client_certs

    log_info "Testing against $label (port $port)"
    if ! PGX_TEST_DATABASE="host=localhost port=$port user=postgres password=postgres dbname=pgx_test" \
         PGX_TEST_UNIX_SOCKET_CONN_STRING="host=/var/run/postgresql port=$port user=postgres dbname=pgx_test" \
         PGX_TEST_TCP_CONN_STRING="host=127.0.0.1 port=$port user=pgx_md5 password=secret dbname=pgx_test" \
         PGX_TEST_MD5_PASSWORD_CONN_STRING="host=127.0.0.1 port=$port user=pgx_md5 password=secret dbname=pgx_test" \
         PGX_TEST_SCRAM_PASSWORD_CONN_STRING="host=127.0.0.1 port=$port user=pgx_scram password=secret dbname=pgx_test" \
         PGX_TEST_PLAIN_PASSWORD_CONN_STRING="host=127.0.0.1 port=$port user=pgx_pw password=secret dbname=pgx_test" \
         PGX_TEST_TLS_CONN_STRING="host=localhost port=$port user=pgx_ssl password=secret sslmode=verify-full sslrootcert=/tmp/ca.pem dbname=pgx_test" \
         PGX_TEST_TLS_CLIENT_CONN_STRING="host=localhost port=$port user=pgx_sslcert sslmode=verify-full sslrootcert=/tmp/ca.pem sslcert=/tmp/pgx_sslcert.crt sslkey=/tmp/pgx_sslcert.key dbname=pgx_test" \
         PGX_SSL_PASSWORD=certpw \
         go test -count=1 "${extra_args[@]}" ./...; then
        log_err "Tests FAILED against $label"
        return 1
    fi
    log_ok "Tests passed against $label"
}

# Main
main() {
    local target="${1:-pg18}"

    if [ "$target" = "all" ]; then
        shift || true
        local targets=(pg14 pg15 pg16 pg17 pg18 crdb)
        local failed=()

        for t in "${targets[@]}"; do
            echo ""
            log_info "=========================================="
            log_info "Target: $t"
            log_info "=========================================="
            if ! run_tests "$t" "$@"; then
                failed+=("$t")
                log_err "FAILED: $t"
            fi
        done

        echo ""
        if [ ${#failed[@]} -gt 0 ]; then
            log_err "Failed targets: ${failed[*]}"
            return 1
        else
            log_ok "All targets passed"
        fi
    else
        shift || true
        run_tests "$target" "$@"
    fi
}

main "$@"
