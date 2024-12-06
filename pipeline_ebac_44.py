from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from script_etl import etl_process
from script_sql_athena import ensure_table_exists, execute_queries

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Funções para tarefas do DAG
def task_001():
    url = "https://1d3kewipla.execute-api.us-east-1.amazonaws.com/api/ingest"
    try:
        logger.info("Executando Task 001: Chamada para a API /ingest")
        response = requests.post(url, timeout=30)
        response.raise_for_status()
        logger.info(f"Task 001 concluída com sucesso. Resposta: {response.json()}")
    except requests.RequestException as e:
        logger.error(f"Erro na Task 001: {e}")
        raise

def task_002():
    try:
        logger.info("Executando Task 002: Iniciando Processo de ETL")
        result = etl_process()
        logger.info(f"Task 002 concluída com sucesso. Resultado: {result}")
    except Exception as e:
        logger.error(f"Erro na Task 002: {e}")
        raise

def task_003():
    try:
        logger.info("Executando Task 003: Verificação e criação da tabela no Athena")
        ensure_table_exists()
        logger.info("Task 003 concluída com sucesso: Tabela verificada/criada.")
    except Exception as e:
        logger.error(f"Erro na Task 003: {e}")
        raise

def task_004():
    try:
        logger.info("Executando Task 004: Execução de consultas no Athena")
        execute_queries()
        logger.info("Task 004 concluída com sucesso: Consultas executadas.")
    except Exception as e:
        logger.error(f"Erro na Task 004: {e}")
        raise

# Criação do DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_bot_telegram',
    default_args=default_args,
    description='Pipeline usando Airflow',
    schedule_interval="*/2 * * * *",  # Rodar a cada 2 minutos
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='call_api_ingest',
        python_callable=task_001,
    )

    task2 = PythonOperator(
        task_id='run_etl_process',
        python_callable=task_002,
    )

    task3 = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=task_003,
    )

    task4 = PythonOperator(
        task_id='execute_queries',
        python_callable=task_004,
    )

    # Definindo a sequência das tarefas
    task1 >> task2 >> task3 >> task4
