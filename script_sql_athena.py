import boto3
import logging
import time

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de Bucket e Athena
ENRICHED_BUCKET = "debora-ebac-modulo-44-enriched"
ATHENA_OUTPUT_LOCATION = f"s3://{ENRICHED_BUCKET}/athena-query-results/"
ATHENA_DATABASE = "default"
TELEGRAM_TABLE = "telegram"

# Cliente Boto3
athena_client = boto3.client("athena")

def run_query(query: str) -> str:
    """
    Executa uma consulta no Athena e retorna o QueryExecutionId.
    """
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    query_execution_id = response["QueryExecutionId"]
    logger.info(f"Consulta iniciada: {query_execution_id}")
    return query_execution_id

def wait_for_query(query_execution_id: str):
    """
    Aguarda a execução da consulta ser concluída.
    """
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state
        logger.info(f"Aguardando conclusão da consulta: {query_execution_id}...")
        time.sleep(5)

def get_query_results(query_execution_id: str):
    """
    Obtém os resultados de uma consulta bem-sucedida.
    """
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    results = []
    for row in response["ResultSet"]["Rows"]:
        results.append([col.get("VarCharValue", "") for col in row["Data"]])
    return results

def ensure_table_exists():
    """
    Verifica se a tabela existe e cria caso não exista.
    """
    try:
        query = f"SHOW TABLES LIKE '{TELEGRAM_TABLE}'"
        query_execution_id = run_query(query)
        state = wait_for_query(query_execution_id)

        if state == "SUCCEEDED":
            results = get_query_results(query_execution_id)
            if len(results) > 1:  # A primeira linha é o cabeçalho
                logger.info(f"Tabela '{TELEGRAM_TABLE}' já existe.")
            else:
                logger.info(f"Tabela '{TELEGRAM_TABLE}' não encontrada. Criando...")
                create_table()
        else:
            logger.error(f"Erro ao verificar tabela '{TELEGRAM_TABLE}'.")
    except Exception as e:
        logger.error(f"Erro ao verificar ou criar tabela: {e}")

def create_table():
    """
    Cria a tabela no Athena.
    """
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `{TELEGRAM_TABLE}`(
      `message_id` bigint,
      `user_id` bigint,
      `user_is_bot` boolean,
      `user_first_name` string,
      `chat_id` bigint,
      `chat_type` string,
      `text` string,
      `date` bigint)
    PARTITIONED BY (
      `context_date` date)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      's3://{ENRICHED_BUCKET}/telegram';
    """
    query_execution_id = run_query(query)
    state = wait_for_query(query_execution_id)
    if state == "SUCCEEDED":
        logger.info(f"Tabela '{TELEGRAM_TABLE}' criada com sucesso.")
        run_query(f"MSCK REPAIR TABLE `{TELEGRAM_TABLE}`;")
    else:
        logger.error(f"Erro ao criar tabela '{TELEGRAM_TABLE}'.")

def execute_queries():
    """
    Executa as consultas SQL desejadas no Athena.
    """
    queries = [
        f"SELECT * FROM `{TELEGRAM_TABLE}` LIMIT 10;",
        f"""
        SELECT
          context_date,
          count(1) AS "message_amount"
        FROM `{TELEGRAM_TABLE}`
        GROUP BY context_date
        ORDER BY context_date DESC
        """,
        f"""
        SELECT
          user_id,
          user_first_name,
          context_date,
          count(1) AS "message_amount"
        FROM `{TELEGRAM_TABLE}`
        GROUP BY
          user_id,
          user_first_name,
          context_date
        ORDER BY context_date DESC
        """,
        f"""
        SELECT
          user_id,
          user_first_name,
          context_date,
          CAST(AVG(length(text)) AS INT) AS "average_message_length"
        FROM `{TELEGRAM_TABLE}`
        GROUP BY
          user_id,
          user_first_name,
          context_date
        ORDER BY context_date DESC
        """,
        f"""
        WITH
        parsed_date_cte AS (
            SELECT
                *,
                CAST(date_format(from_unixtime("date"),'%Y-%m-%d %H:%i:%s') AS timestamp) AS parsed_date
            FROM `{TELEGRAM_TABLE}`
        ),
        hour_week_cte AS (
            SELECT
                *,
                EXTRACT(hour FROM parsed_date) AS parsed_date_hour,
                EXTRACT(dow FROM parsed_date) AS parsed_date_weekday,
                EXTRACT(week FROM parsed_date) AS parsed_date_weeknum
            FROM parsed_date_cte
        )
        SELECT
            parsed_date_hour,
            parsed_date_weekday,
            parsed_date_weeknum,
            count(1) AS "message_amount"
        FROM hour_week_cte
        GROUP BY
            parsed_date_hour,
            parsed_date_weekday,
            parsed_date_weeknum
        ORDER BY
            parsed_date_weeknum,
            parsed_date_weekday
        """
    ]

    for query in queries:
        try:
            query_execution_id = run_query(query)
            state = wait_for_query(query_execution_id)
            if state == "SUCCEEDED":
                results = get_query_results(query_execution_id)
                logger.info(f"Resultados da consulta:\n{results}")
            else:
                logger.error(f"Erro ao executar consulta: {query}")
        except Exception as e:
            logger.error(f"Erro durante a execução da consulta: {e}")

if __name__ == "__main__":
    ensure_table_exists()
    execute_queries()
