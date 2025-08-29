
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

#-- função extract ----
def func_extracao():
    
    ##- imports ----
    import pandas as pd
    import pyodbc
    import os
        
    from pathlib import Path
    
    ##- variaveis ----
    CAMINHO_PASTA_FICHEIROS = r"/opt/airflow/data/xlsx"
    CAMINHO_PASTA_PAQUET    = r"/opt/airflow/data/parquet"
    NOME_FICHEIRO           = "02-01 - Planilha Folha de Pagamentos.xlsx"
    
    ##- funções auxiliares -----
    ###- pega o caminho completo do ficheiro
    def func_pega_caminho_ficheiro_extensao(caminho_pasta:str, nome_ficheiro:str) -> None | str:
        
        resultado = None
        
        ### varre toda pasta dos ficheiros
        for item in os.listdir(caminho_pasta):
            
            ### verifica a existencia do ficheiro:nome_ficheiro
            if item.startswith(nome_ficheiro):
                resultado = os.path.join(caminho_pasta, item)
                break
                
        return resultado
            
    ###- pega dataframe de nome_ficheiro
    def func_pega_dataframe(caminho_pasta:str, nome_ficheiro:str) -> None | pd.DataFrame:
        
        ### variaveis
        resultado = None
        caminho_pasta_ficheiro = func_pega_caminho_ficheiro_extensao(caminho_pasta, nome_ficheiro)
        
        ### verifica a nulidade de caminho_pasta_ficheiro
        if caminho_pasta_ficheiro is not None:
            
            ### defineo dataFrame
            df_extracao = pd.read_excel(
                caminho_pasta_ficheiro, 
                engine="openpyxl",
                sheet_name="Funcionários",
                skiprows=2,
                usecols="F"
            )
            
            resultado = df_extracao
            print("---- Dataframe definido")
        
        return resultado
    
    ### verifica nulidade do dataFrame
    def func_verifica_dataframe(df:pd.DataFrame) -> None | pd.DataFrame:
        return isinstance(df, pd.DataFrame) and not df.empty
    
    ##- definir o dataframe extraido
    df_regime_contratacao_extraido = func_pega_dataframe(CAMINHO_PASTA_FICHEIROS, NOME_FICHEIRO)
    
    ##- verifica nulidade de df_regime_contratacao_extraido
    if func_verifica_dataframe(df_regime_contratacao_extraido) is not None:
        
        ##- definir a nomeclatura do ficheiro parquet
        ficheiro_regime_contratacao_parquet = CAMINHO_PASTA_PAQUET+"/regime_contratacao_extraido.parquet"
        
        ##- inserir no parquet
        df_regime_contratacao_extraido.to_parquet(ficheiro_regime_contratacao_parquet, engine="pyarrow")
        print("---- Ficheiro extraido foi inserido no Parquet")

with DAG(
    dag_id='002_dag_regime_pagamento_extracao',
    default_args=default_args,
    description='DAG para extração de Regime de Contratação',
    schedule=None,
    start_date=pendulum.datetime(2025, 7, 28, tz="Africa/Luanda"),
    tags=['extração_regime_pagamento'],
) as dag:   
    
    executa_extracao = PythonOperator(
        task_id = 'extrair_dataframe',
        python_callable = func_extracao
    )
    
    executa_extracao
       
        
        
        
    
    