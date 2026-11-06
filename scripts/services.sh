#!/usr/bin/env bash
set -euo pipefail
case "${1:-up}" in
up)
  docker inspect clearinghouse-pg >/dev/null 2>&1 || docker run -d --name clearinghouse-pg -e POSTGRES_PASSWORD=localdemo -e POSTGRES_DB=clearinghouse -p 127.0.0.1:55432:5432 postgres:18
  docker start clearinghouse-pg >/dev/null
  docker inspect clearinghouse-nats >/dev/null 2>&1 || docker run -d --name clearinghouse-nats -p 127.0.0.1:54222:4222 nats:2.12 -js -sd /data
  docker start clearinghouse-nats >/dev/null
  for attempt in $(seq 1 60); do
    if docker exec clearinghouse-pg pg_isready -U postgres -d clearinghouse >/dev/null; then exit 0; fi
    sleep 1
  done
  echo 'PostgreSQL did not become ready' >&2; exit 1;;
down) docker stop clearinghouse-pg clearinghouse-nats;;
*) echo 'usage: services.sh up|down' >&2; exit 2;;
esac
