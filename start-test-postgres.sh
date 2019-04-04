#!/bin/sh

docker build --tag=sqlg-postgres ./sqlg-testdb-postgres

docker run -p 5432:5432 sqlg-postgres
