from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import create_engine

import pandas as pd
from preprocessing import DataPreprocessing


def get_latest_entry():
    hook = MySqlHook(mysql_conn_id="ds-cei-mysql")
    sql = """SELECT MAX(T.Dia) AS Dia FROM ceiTables.transformed T"""
    latest_record = hook.get_pandas_df(sql)
    latest_record = latest_record['Dia'].iloc[0]
    
    return latest_record


def get_src_tables(latest_record: pd.Timestamp):
    hook = MySqlHook(mysql_conn_id="cei-mysql")
    
    sql = f"SELECT * FROM comiteec_aire.PurpleAirData P WHERE P.Dia > '{latest_record}' LIMIT 100"
    purple_df = hook.get_pandas_df(sql)

    sql = f"SELECT * FROM comiteec_aire.Registros R WHERE R.Dia > '{latest_record}' LIMIT 100"
    aire_df = hook.get_pandas_df(sql)

    return purple_df, aire_df


def transform_tables(purple_df: pd.DataFrame, aire_df: pd.DataFrame):
    conn = MySqlHook.get_connection("ds-cei-mysql")
    engine = create_engine(f'mysql://{conn.login}:{conn.password}@{conn.host}')

    dp = DataPreprocessing()
    dp.purple, dp.aire = purple_df, aire_df
    dp.preprocess()
    joined_df = dp.get_data()

    joined_columns =  ['Log_id', 'Dia', 'PM25_A', 'PM25_B', 'PM25_Corregido',
                        'Humedad_Relativa', 'Temperatura', 'Presion',
                        'Sensor_id_x', 'PM25_Promedio', 'Registros_id',
                        'PM25', 'Sensor_id_y']

    joined_df = joined_df[joined_columns]

    joined_df.to_sql(name='transformed',schema='ceiTables', con=engine, if_exists='replace', index=False)

    return 'tables added'


def etl():
    latest_record = get_latest_entry()
    latest_record = '2022-08-27 23:00:00'
    purple, aire = get_src_tables(latest_record)
    transform_tables(purple, aire)
