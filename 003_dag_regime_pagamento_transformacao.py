
#-- importações
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

#-- argumentos padão da dag ----
default_args = {
    'owner':'Nziko Denise Kissoka',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=3)
}

#-- função transformação ----
def func_transformacao():
    
    ##- imports ----
    import pandas as pd
    import pyodbc
    import os
    
    ## path
    CAMINHO_PASTA_PAQUET          = r"/opt/airflow/data/parquet"
    FICHEIRO_EXTRAIDO_PARQUET     = CAMINHO_PASTA_PAQUET+"/regime_contratacao_extraido.parquet"
    FICHEIRO_TRANSFORMADO_PARQUET = CAMINHO_PASTA_PAQUET+"/regime_contratacao_transformado.parquet"
    
    ## funções auxiliares    
    ## eliminar ficheirp parquet
    def func_elimina_ficheiro_parquet(caminho:str):
        os.remove(caminho)
        print("---- Ficheiro extraido Parquet removido.")
    
    ## verifica a existencia do ficheiro extraido no parquet
    if os.path.exists(FICHEIRO_EXTRAIDO_PARQUET):
        
        # ler ficheiro do parquet
        df_regime_contratacao_extraido = pd.read_parquet(FICHEIRO_EXTRAIDO_PARQUET)
        
        # remover duplicados
        df_regime_contratacao_extraido = df_regime_contratacao_extraido.drop_duplicates()
        
        # eliminar ficheiro extraido
        func_elimina_ficheiro_parquet(FICHEIRO_EXTRAIDO_PARQUET)
        
        # adiciona campo id no dataframe df_regime_contratacao_extraido
        df_regime_contratacao_extraido["id"] = range(1, len(df_regime_contratacao_extraido)+1)
        
        # renomear os campos
        novos_campos = {"Regime Contratação":"descricao", "id":"id"}
        df_regime_contratacao_extraido = df_regime_contratacao_extraido.rename(columns=novos_campos)
        
        # reorganizar os campos
        df_regime_contratacao_extraido = df_regime_contratacao_extraido[["id", "descricao"]]
        
        # gravar no parquet
        df_regime_contratacao_extraido.to_parquet(FICHEIRO_TRANSFORMADO_PARQUET, engine="pyarrow")
        
        # verfica o sucesso na gravação do parquet
        if os.path.exists(FICHEIRO_TRANSFORMADO_PARQUET):
            print("---- Ficheiro gravado no Parquet com Sucesso.")
        else:
            print("---- Ficheiro não gravado no Parquet.")
            
    else:
        
        print(f"---- O ficheiro no caminho {FICHEIRO_EXTRAIDO_PARQUET}, não existe.")

# declaração da dag
with DAG(
    dag_id='003_dag_regime_pagamento_transformacao',
    default_args=default_args,
    description='DAG para transformação dos dados de Regime de Contratação, depois de serem extraídos.',
    schedule=None,
    start_date=pendulum.datetime(2025, 7, 28, tz="Africa/Luanda"),
    tags=['etl_transformacao'],
) as dag:
    
    executa_transformacao = PythonOperator(
        task_id = "transformar_dataframe",
        python_callable = func_transformacao
    )
    
    executa_transformacao