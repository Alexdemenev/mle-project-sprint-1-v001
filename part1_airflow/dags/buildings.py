# dags/churn.py

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL", "buildings"]
)
def prepare_buildings_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    
    @task()
    def create_table() -> None:

        import sqlalchemy
        from sqlalchemy import inspect, MetaData, Table, Column, String, Integer, Float, Boolean, UniqueConstraint # дополните импорты необходимых типов колонок
        """
        id                     int64
        build_year             int64
        building_type_int      int64
        latitude             float64
        longitude            float64
        ceiling_height       float64
        flats_count            int64
        floors_total           int64
        has_elevator            bool
        floor                  int64
        is_apartment            bool
        kitchen_area         float64
        living_area          float64
        rooms                  int64
        studio                  bool
        total_area           float64
        price                  int64
        building_id            int64
        """
        
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        churn_table = Table(
            'buildings_full',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            Column('floor', Integer),
            Column('is_apartment', Boolean),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Integer),
            Column('building_id', Integer),
            UniqueConstraint('building_id', name='building_id_constraint')
        ) 

        if not inspect(conn).has_table(churn_table.name): 
            metadata.create_all(conn)


    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select * from buildings b 
        left join flats f on b.id = f.building_id
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data = data[(data['living_area'] + data['kitchen_area']) <= data['total_area']]
        data.drop_duplicates(subset=list(data.drop(columns=['id']).columns), keep='first', inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="buildings_full",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id'],
            rows=data.values.tolist()
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_buildings_dataset()
