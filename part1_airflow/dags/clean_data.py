import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_failure_message, send_telegram_success_message # импортируем функции для отправки сообщений


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["clean_data", "buildings"],
    on_failure_callback=send_telegram_failure_message,
    on_success_callback=send_telegram_success_message
)
def clean_buildings_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        from sqlalchemy import Table, Column, Float, Integer, Boolean, MetaData, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        buildings_table = Table(
            'buildings_clean',
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
            Column('price', Float),
            Column('building_id', Integer),
            UniqueConstraint('building_id', name='building_id_clean_constraint')
        ) 
        if not inspect(db_engine).has_table(buildings_table.name):
            metadata.create_all(db_engine)
    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select * from buildings_full
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        
        def remove_duplicates(data):
            feature_cols = data.columns.drop('building_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data 
        
        def fill_missing_values(data):

            cols_with_nans = data.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0].index

            for col in cols_with_nans:

                if data[col].dtype in [float, int]:
                    fill_value = data[col].mean()
                elif data[col].dtype == 'object':
                    fill_value = data[col].mode().iloc[0]

                data[col] = data[col].fillna(fill_value)

            return data 
        
        # удаляем дубликаты
        data = remove_duplicates(data)
        
        # заполняем пропущенные значения
        data = fill_missing_values(data)
        
        # удаляем выбросы
        num_cols = data.drop(columns=['building_type_int', 'building_id']).select_dtypes(['float', 'int']).columns
        threshold = 3
        potential_outliers = pd.DataFrame()

        for col in num_cols:
            Q1 = data[col].quantile(0.25)
            Q3 = data[col].quantile(0.75)
            IQR = Q3 - Q1
            margin = threshold*IQR
            lower = Q1 - margin
            upper = Q3 + margin 
            potential_outliers[col] = ~data[col].between(lower, upper)

        outliers = potential_outliers.any(axis=1)

        data = data[~outliers]
        data = data[data['building_type_int'] != 5] # удаляем, т.к. таких типов всего несколько
        
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')

        hook.insert_rows(
            table='buildings_clean',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_buildings_dataset()