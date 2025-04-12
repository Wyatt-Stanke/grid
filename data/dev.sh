#!/bin/bash
set -eux
cleanup() {
    docker compose rm -fsv
    docker volume rm -f data_pgdata
}
trap cleanup EXIT
docker compose up
