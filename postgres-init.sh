#!/bin/bash

# Inspired by https://github.com/mrts/docker-postgresql-multiple-databases/blob/master/create-multiple-postgresql-databases.sh
# DB names hardcoded, script is created for demo purposes.
PG_PORT="5431"
user="postgres"
set -euxo pipefail

function create_user_and_database() {
	local user=$1
	local database=$2
	echo "Creating database '$database'."
	psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" <<-EOSQL
    CREATE DATABASE $database;
    GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL
}

# 1. Create databases
create_user_and_database "postgres" "insideairbnb"

# 2. Create table for insideairbnb listings
psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" insideairbnb <<-EOSQL
CREATE TABLE IF NOT EXISTS listings(
  id                             BIGINT,
  name                           TEXT,
  host_id                        INTEGER,
  host_name                      VARCHAR(100),
  neighbourhood_group            VARCHAR(100),
  neighbourhood                  VARCHAR(100),
  latitude                       NUMERIC(18,16),
  longitude                      NUMERIC(18,16),
  room_type                      VARCHAR(100),
  price                          INTEGER,
  minimum_nights                 INTEGER,
  number_of_reviews              INTEGER,
  last_review                    DATE,
  reviews_per_month              NUMERIC(5,2),
  calculated_host_listings_count INTEGER,
  availability_365               INTEGER,
  number_of_reviews_ltm          INTEGER,
  license                        VARCHAR(100),
  download_date                  DATE NOT NULL
);
EOSQL

# 3. Enable PostGIS
psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" insideairbnb <<-EOSQL
CREATE EXTENSION IF NOT EXISTS postgis;
EOSQL

# 4. Create table for neighbourhoods
psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" insideairbnb <<-EOSQL
CREATE TABLE IF NOT EXISTS neighbourhoods(
  id                             SERIAL PRIMARY KEY,
  neighbourhood                  VARCHAR(100),
  neighbourhood_group            VARCHAR(100),
  download_date                  DATE,
  geometry                       GEOMETRY(Multipolygon, 4326)
);
EOSQL

# 5. Download Inside Airbnb Amsterdam listings data (http://insideairbnb.com/get-the-data.html)
listing_url="http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{DATE}/visualisations/listings.csv"
listing_dates="
2023-09-03
2023-06-05
2023-03-09
"

mkdir -p /tmp/insideairbnb
for d in ${listing_dates}
do
  url=${listing_url/\{DATE\}/$d}
  wget $url -O /tmp/insideairbnb/listing-$d.csv

  # Hacky way to add the "download_date", by appending the date to all rows in the downloaded file
  sed -i "" "1 s/$/,download_date/" /tmp/insideairbnb/listing-$d.csv
  sed -i "" "2,$ s/$/,$d/" /tmp/insideairbnb/listing-$d.csv

  psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" insideairbnb <<-EOSQL
  \COPY listings FROM '/tmp/insideairbnb/listing-$d.csv' DELIMITER ',' CSV HEADER QUOTE '"';
EOSQL
done

# 6. Download Inside Airbnb Amsterdam neighbourhoods data (http://insideairbnb.com/get-the-data.html)
listing_url="http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{DATE}/visualisations/neighbourhoods.geojson"
listing_dates="
2023-09-03
2023-06-05
2023-03-09
"

mkdir -p /tmp/neighbourhoods
for d in ${listing_dates}
do
  url=${listing_url/\{DATE\}/$d}
  wget $url -O /tmp/neighbourhoods/neighbourhoods-$d.geojson

  # Hacky way to add the "download_date", by appending the date to all entries in the downloaded file
  jq --arg dd "$d" '.features[].properties |= . + {download_date: $dd}' /tmp/neighbourhoods/neighbourhoods-$d.geojson > \
  /tmp/neighbourhoods/v2neighbourhoods-$d.geojson
  ogr2ogr -f "PostgreSQL" PG:"dbname=insideairbnb user=postgres password=postgres host=localhost port=5431" \
  /tmp/neighbourhoods/v2neighbourhoods-$d.geojson  -nln neighbourhoods -nlt PROMOTE_TO_MULTI
  psql -h localhost -p 5431 -U $user -d insideairbnb -c "UPDATE neighbourhoods SET geometry = ST_SetSRID(geometry, 4326);"
done

function grant_all() {
	local database=$1
	psql -v ON_ERROR_STOP=1 -p "$PG_PORT" --username "$user" $database <<-EOSQL
    ALTER SCHEMA public OWNER TO "$user";
    GRANT USAGE ON SCHEMA public TO "$user";
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "$user";
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "$user";
    GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO "$user";
EOSQL
}

# Somehow the database-specific privileges must be set AFTERWARDS
grant_all "postgres"

pg_ctl stop -p "$PG_PORT" -m fast -w  # -m fast: fast shutdown, -w: wait for shutdown to complete
