
# impoortações
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta 

import pandas as pd
import os
import pendulum

dag_owner = 'Nziko Denise Kissoka'

default_args = {
        'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }

# definição de caminhos
#CAMINHO_FICHEIRO = (r"C:\desenvolvimento\engenharia-dados\dados\xlsx\02-01 - Planilha Folha de Pagamentos.xlsx")
#CAMINHO_FICHEIRO_REGIME_CONTRATACAO_EXTRAIDO = (r"C:\desenvolvimento\engenharia-dados\dados\parquet\regime_contratacao_extraido.parquet")
#CAMINHO_FICHEIRO_REGIME_CONTRATACAO_TRANSFORMADO = "C:\desenvolvimento\engenharia-dados\dados\parquet\regime_contratacao_transformado.parquet"

#CAMINHO_XLSX = r"C:\desenvolvimento\engenharia-dados\dados\xlsx\02-01 - Planilha Folha de Pagamentos.xlsx"
#CAMINHO_EXTRAIDO = r"C:\desenvolvimento\engenharia-dados\dados\parquet\regime_contratacao_extraido.parquet"
#CAMINHO_TRANSFORMADO = r"C:\desenvolvimento\engenharia-dados\dados\parquet\regime_contratacao_transformado.parquet"

CAMINHO_XLSX = r"/opt/airflow/data/xlsx/02-01 - Planilha Folha de Pagamentos.xlsx"
CAMINHO_EXTRAIDO = r"/opt/airflow/data/parquet/regime_contratacao_extraido.parquet"
CAMINHO_TRANSFORMADO = r"/opt/airflow/data/parquet/regime_contratacao_transformado.parquet"

# função verificar existencia de ficheiro
def verificar_ficheiro_xlsx():
    
    if os.path.exists(CAMINHO_XLSX):
        return 'executa_dag_extracao'
    else:
        return 'fim'
    



# função verificar existencia e tamanho de dataframe extraido
def verificar_extraido():
    
    if not os.path.exists(CAMINHO_EXTRAIDO):
        return 'fim'
    else: 
        df = pd.read_parquet(CAMINHO_EXTRAIDO)
        return 'executa_dag_transformacao' if not df.empty else 'fim'


# função verificar existencia e tamanho de dataframe transformado
def verificar_transformado():
    
    if not os.path.exists(CAMINHO_TRANSFORMADO):
        return 'fim'
    else: 
        df = pd.read_parquet(CAMINHO_TRANSFORMADO)
        return 'executa_dag_carga' if not df.empty else 'fim'
        
        
        
with DAG(dag_id='001_dag_orquestrador_folha_pagamento', 
        default_args=default_args,
        description='DAG orquestradora de folha de pagamento',
        start_date=pendulum.datetime(2025, 7, 28, tz="Africa/Luanda"),
        schedule=None,
        catchup=False,
        tags=['']
) as dag:

    # tasks inicio e fim
    inicio = EmptyOperator(task_id='inicio')
    fim    = EmptyOperator(task_id='fim')
    
    # tasks de extração
    verificar_xlsx = BranchPythonOperator(
        task_id='verificar_existencia_ficheiro_xlsx',
        python_callable=verificar_ficheiro_xlsx
    )
    
    trigger_extracao = TriggerDagRunOperator(
        task_id='executa_dag_extracao',
        trigger_dag_id='002_dag_regime_pagamento_extracao',
        wait_for_completion=True
    )
    
    # tasks de transformação
    
    # tasks de carregamento
    
    
    # ETAPA 1 - Verifica se existe o ficheiro xlsx
    # se sim, extrai e grava no parquet
    # se não, fim
    inicio >> verificar_xlsx
    verificar_xlsx >> trigger_extracao 
    verificar_xlsx >> fim

    