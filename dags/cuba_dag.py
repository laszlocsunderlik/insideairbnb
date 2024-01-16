import csv
import json
import geopandas as gpd
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_CONN = "postgres_local"


@task
def building_reader():
    with open("insideairbnb/data/csv-buildings/88d_buildings.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        data = [row for row in reader]
    return data


@task(multiple_outputs=True)
def landuse_reader():
    gdf = gpd.read_file("insideairbnb/data/landuse/landuse.shp")
    gdf_json = gdf.to_json()
    data = json.loads(gdf_json)
    return {"one": data, "two": int(gdf.crs.srs.split(":")[-1])}


@task(multiple_outputs=True)
def adm0_reader():
    gdf = gpd.read_file("insideairbnb/data/cub_adma_2019_shp/cub_admbnda_adm0_2019.shp")
    gdf_json = gdf.to_json()
    data = json.loads(gdf_json)
    return {"one": data, "two": int(gdf.crs.srs.split(":")[-1])}

@task(multiple_outputs=True)
def adm1_reader():
    gdf = gpd.read_file("insideairbnb/data/cub_adma_2019_shp/cub_admbnda_adm1_2019.shp")
    gdf_json = gdf.to_json()
    data = json.loads(gdf_json)
    return {"one": data, "two": int(gdf.crs.srs.split(":")[-1])}

@task(multiple_outputs=True)
def adm2_reader():
    gdf = gpd.read_file("insideairbnb/data/cub_adma_2019_shp/cub_admbnda_adm2_2019.shp")
    gdf_json = gdf.to_json()
    data = json.loads(gdf_json)
    return {"one": data, "two": int(gdf.crs.srs.split(":")[-1])}


@task
def insert_adm0(json_data, crs, postgres_conn_id):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    cur = connection.cursor()

    insert_query = """
    insert into cuba_adm0 (adm0_pcode, adm0_es, geometry)
    values (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), %s));
    """

    for row in json_data["features"]:
        adm0_pcode = row["properties"].get("ADM0_PCODE", None)
        adm0_es = row["properties"].get("ADM0_ES", None)
        geometry = json.dumps(row["geometry"])
        print(geometry)
        cur.execute(insert_query,
                    (str(adm0_pcode),
                     str(adm0_es),
                     geometry,
                     crs))

    connection.commit()

@task
def insert_adm1(json_data, crs, postgres_conn_id):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    cur = connection.cursor()

    insert_query = """
    insert into cuba_adm1 (adm1_pcode, adm1_es, adm1_ref, geometry)
    values (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), %s));
    """

    for row in json_data["features"]:
        adm1_pcode = row["properties"].get("ADM1_PCODE", None)
        adm1_es = row["properties"].get("ADM1_ES", None)
        adm1_ref = row["properties"].get("ADM1_REF", None)
        geometry = json.dumps(row["geometry"])
        cur.execute(insert_query,
                    (str(adm1_pcode),
                     str(adm1_es),
                     str(adm1_ref),
                     geometry,
                     crs))

    connection.commit()

@task
def insert_adm2(json_data, crs, postgres_conn_id):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    cur = connection.cursor()

    insert_query = """
    insert into cuba_adm2 (adm2_pcode, adm2_es, adm2_ref, geometry)
    values (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), %s));
    """

    for row in json_data["features"]:
        adm2_pcode = row["properties"].get("ADM2_PCODE", None)
        adm2_es = row["properties"].get("ADM2_ES", None)
        adm2_ref = row["properties"].get("ADM2_REF", None)
        geometry = json.dumps(row["geometry"])
        cur.execute(insert_query,
                    (str(adm2_pcode),
                     str(adm2_es),
                     str(adm2_ref),
                     geometry,
                     crs))

    connection.commit()


@task
def insert_landuse(json_data, crs, postgres_conn_id):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    cur = connection.cursor()

    insert_query = """
    insert into cuba_landuse (osm_id, name, landuse_type, geometry)
    values (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), %s));
    """

    for row in json_data["features"]:
        osm_id = row["properties"].get("osm_id", None)
        name = row["properties"].get("name", None)
        landuse_type = row["properties"].get("type", None)
        geometry = json.dumps(row["geometry"])
        print(geometry)
        cur.execute(insert_query,
                    (int(osm_id),
                     str(name),
                     str(landuse_type),
                     geometry,
                     crs))

    connection.commit()


@task
def insert_buildings(data, postgres_conn_id):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    cur = connection.cursor()
    insert_query = """
        insert into buildings (latitude, longitude, area_in_meters, confidence, geometry, full_plus_code)
        values (%s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s)
    """
    for row in data:
        latitude, longitude, area_in_meters, confidence, wkt_geometry, full_plus_code = row

        cur.execute(insert_query,
                    (float(latitude),
                     float(longitude),
                     float(area_in_meters),
                     float(confidence),
                     wkt_geometry,
                     str(full_plus_code)
                     ))
    connection.commit()


with DAG(
        dag_id="inserting_GIS_data_into_PostGIS",
        description="Inserting_GIS_data_into_PostGIS.",
        start_date=airflow.utils.dates.days_ago(1),
        schedule="@hourly",
        catchup=False,
        default_args={'provide_context': True}
) as dag:
    start = EmptyOperator(task_id="start")

    drop_cuba_adm0 = PostgresOperator(
        task_id="drop_cuba_adm0",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            drop table if exists cuba_adm0 cascade;
            """
    )
    drop_cuba_adm1 = PostgresOperator(
        task_id="drop_cuba_adm1",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            drop table if exists cuba_adm1 cascade;
            """
    )
    drop_cuba_adm2 = PostgresOperator(
        task_id="drop_cuba_adm2",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            drop table if exists cuba_adm2 cascade;
            """
    )
    drop_cuba_landuse = PostgresOperator(
        task_id="drop_cuba_landuse",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
        drop table if exists cuba_landuse cascade;
        """
    )
    drop_buildings = PostgresOperator(
        task_id="drop_buildings",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
                drop table if exists buildings cascade;
                """
    )
    create_cuba_adm0 = PostgresOperator(
        task_id="create_cuba_adm0",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            create table if not exists cuba_adm0(
            id serial primary key,
            adm0_pcode varchar(2),
            adm0_es varchar(4),
            geometry geometry not null
            )
            """
    )
    gist_cuba_adm0 = PostgresOperator(
        task_id="gist_cuba_adm0",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            CREATE INDEX gist_cuba_adm0 ON cuba_adm0 USING GIST (geometry);
            """
    )
    create_cuba_adm1 = PostgresOperator(
        task_id="create_cuba_adm1",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
                create table if not exists cuba_adm1(
                id serial primary key,
                adm1_pcode varchar(4),
                adm1_es varchar,
                adm1_ref varchar,
                geometry geometry not null
                )
                """
    )
    gist_cuba_adm1 = PostgresOperator(
        task_id="gist_cuba_adm1",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
                CREATE INDEX gist_cuba_adm1 ON cuba_adm1 USING GIST (geometry);
                """
    )
    create_cuba_adm2 = PostgresOperator(
        task_id="create_cuba_adm2",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            create table if not exists cuba_adm2(
            id serial primary key,
            adm2_pcode varchar(6),
            adm2_es varchar,
            adm2_ref varchar,
            geometry geometry not null
            )
            """
    )
    gist_cuba_adm2 = PostgresOperator(
        task_id="gist_cuba_adm2",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            CREATE INDEX gist_cuba_adm2 ON cuba_adm2 USING GIST (geometry);
            """
    )
    create_cuba_landuse = PostgresOperator(
        task_id="create_cuba_landuse",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            create table if not exists cuba_landuse(
            id serial primary key,
            osm_id integer,
            name varchar,
            landuse_type varchar,
            geometry geometry not null
            )
            """
    )
    gist_cuba_landuse = PostgresOperator(
        task_id="gist_cuba_landuse",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
        CREATE INDEX gist_cuba_landuse ON cuba_landuse USING GIST (geometry);
        """
    )
    create_buildings = PostgresOperator(
        task_id="create_buildings",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
            create table if not exists buildings(
            building_id serial primary key,
            latitude float,
            longitude float,
            area_in_meters float,
            confidence float,
            geometry geometry not null,
            full_plus_code varchar
            )
            """
    )
    gist_buildings = PostgresOperator(
        task_id="gist_buildings",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
                CREATE INDEX gist_buildings ON buildings USING GIST (geometry);
                """
    )
    spatial_join = PostgresOperator(
        task_id="spatial_join",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
        CREATE VIEW view_building_sj AS
        SELECT
            buildings.building_id,
            buildings.geometry,
            COALESCE(c2.adm2_pcode, '') AS adm2_pcode,
            COALESCE(c2.adm2_es, '') AS adm2_es,
            COALESCE(c2.adm2_ref, '') AS adm2_ref,
            COALESCE(landuse.name, '') AS name,
            COALESCE(landuse.landuse_type, '') AS landuse_type
        FROM
            buildings
        LEFT JOIN LATERAL (
            SELECT
                adm2.adm2_pcode,
                adm2.adm2_es,
                adm2.adm2_ref
            FROM
                cuba_adm2 adm2
            WHERE
                ST_Contains(adm2.geometry, buildings.geometry)
            LIMIT 1
        ) AS c2 ON true
        LEFT JOIN LATERAL (
            SELECT
                lu.name,
                lu.landuse_type
            FROM
                cuba_landuse lu
            WHERE
                ST_Contains(lu.geometry, buildings.geometry)
            LIMIT 1
        ) AS landuse ON true;
        """
    )
    analysis = PostgresOperator(
        task_id="analysis",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
        select
            *
        from
            view_building_sj
        WHERE
            building_id = '254412';
        """
    )
    analysis2 = PostgresOperator(
        task_id="analysis2",
        postgres_conn_id=POSTGRES_CONN,
        sql="""
        select count(*) from view_building_sj
        where adm2_pcode = 'CU0913'
        ;
        """
    )

    adm0 = adm0_reader()
    insert_adm0_task = insert_adm0(adm0["one"], adm0["two"], postgres_conn_id=POSTGRES_CONN)

    adm1 = adm1_reader()
    insert_adm1_task = insert_adm1(adm1["one"], adm1["two"], postgres_conn_id=POSTGRES_CONN)

    adm2 = adm2_reader()
    insert_adm2_task = insert_adm2(adm2["one"], adm2["two"], postgres_conn_id=POSTGRES_CONN)

    landuse = landuse_reader()
    insert_landuse_taks = insert_landuse(landuse["one"], landuse["two"], postgres_conn_id=POSTGRES_CONN)

    building_data = building_reader()
    insert_buildings_task = insert_buildings(building_data, postgres_conn_id=POSTGRES_CONN)

    start >> [drop_cuba_adm0, drop_cuba_adm1, drop_cuba_adm2, drop_cuba_landuse, drop_buildings]
    drop_cuba_adm0 >> create_cuba_adm0 >> gist_cuba_adm0 >> adm0 >> insert_adm0_task
    drop_cuba_adm1 >> create_cuba_adm1 >> gist_cuba_adm1 >> adm1 >> insert_adm1_task
    drop_cuba_adm2 >> create_cuba_adm2 >> gist_cuba_adm2 >> adm2 >> insert_adm2_task
    drop_cuba_landuse >> create_cuba_landuse >> gist_cuba_landuse >> landuse >> insert_landuse_taks
    drop_buildings >> create_buildings >> gist_buildings >> building_data >> insert_buildings_task
    [insert_adm0_task, insert_adm1_task, insert_adm2_task, insert_landuse_taks, insert_buildings_task] >> spatial_join
    spatial_join >> analysis >> analysis2
