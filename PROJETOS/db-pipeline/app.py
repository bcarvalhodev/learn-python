import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime
import numpy as np

load_dotenv()


def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']])
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:, "datetime_partida"] = df.loc[:, "datetime_partida"].str.replace('.0', '')
    df.loc[:, "datetime_chegada"] = df.loc[:, "datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:, col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:, 'data_voo'].astype(str) + " " + lst_col)

    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df


def feat_eng(df):
    '''
    Função criar as colunas para análise de dados
    INPUT: dataframe com os dados para criar colunas com tratamento dos dados
    OUTPUT: dataframe com as colunas criadas com tratamentos dos dados
    '''
    logger.info('montando coluna com calculo previsao_tempo_voo')
    df["previsao_tempo_voo"] = utils.retornar_previsao_de_tempo_de_voo(df)

    logger.info('montando coluna com calculo tempo_voo_em_horas')
    df["tempo_voo_em_horas"] = df["tempo_voo"] / 60

    logger.info('montando coluna com calculo tempo_de_atraso')
    df["tempo_de_atraso"] = df["tempo_voo_em_horas"] - df["previsao_tempo_voo"]

    logger.info('montando coluna  com logica de voo_atrasou')
    df["voo_atrasou"] = np.where(df['tempo_de_atraso'] > 0.6, True, False)

    logger.info('montando coluna  com logica de voo_adiantado')
    df["voo_adiantado"] = np.where(df['tempo_de_atraso'] < -0.5, True, False)

    logger.info('montando coluna  com logica de dia_semana')
    df["dia_semana"] = df["data_voo"].dt.day_of_week.apply(lambda x: utils.retorna_dia_da_semana(x))

    logger.info('montando coluna  com logica de turno_partida')
    df["turno_partida"] = df.loc[:, "datetime_partida_formatted"].dt.hour.apply(
        lambda x: utils.retornar_horario_partida(x))

    logger.info(f'Tratamento concluído; {datetime.datetime.now()}')
    return df


def save_data_sqlite(df):
    conn = None
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()


def fetch_sqlite_data(table):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'), index_col=0)
    df = data_clean(df, metadados)
    logger.info(f"dataframe carregado com arquivo csv do {os.getenv('DATA_PATH')}")
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves_validacao"])
    df = feat_eng(df)
    save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')
