#/bin/bash
set -e 

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE spiderdata;
  CREATE USER spider LOGIN PASSWORD 'friendlyneighborhoodspider';
  GRANT ALL PRIVILEGES ON DATABASE spiderdata TO spider;
EOSQL

psql -v ON_ERROR_STOP=1 --username spider --dbname spiderdata <<-EOSQL
  CREATE TABLE scrapped_data (url TEXT PRIMARY KEY, title TEXT, html TEXT);
EOSQL
