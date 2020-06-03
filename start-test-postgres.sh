#!/bin/sh

docker build --tag=sqlg-postgres ./sqlg-testdb-postgres

docker run --rm -d --name sqlg-postgres -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust sqlg-postgres
