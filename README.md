Playground for Insideairbnb data engineering
============================================

This repository contains a set of scripts to download and process the [Insideairbnb](http://insideairbnb.com/get-the-data.html) data hosted on FastAPI.

The database is hosted on a PostgreSQL server. Used [bash](https://github.com/laszlocsunderlik/insideairbnb/blob/main/postgres-init.sh) script for database setup and data load.

On top of the database, there is a FastAPI server that provides a REST API to query the data. Manages user registration and login. Properly setup GeoJSON schemas are used to provide spatial data as responses.

The scripts are written in Python and use the [Airflow](https://airflow.apache.org/) library for ETl orchestration. Monthly updated Airbnb listings and neighbourhoods (both geopsatial and non-geospatial) data are downloaded as geojson format.

The geojson datasets were analysed with [geopandas](https://geopandas.org/en/stable/index.html) to have an update about changes of lsitings per neighbourhoods and their prices.