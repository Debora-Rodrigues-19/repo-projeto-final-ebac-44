import json
import logging
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta, timezone

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração dos buckets (hardcoded)
RAW_BUCKET = "debora-ebac-modulo-44-raw"
ENRICHED_BUCKET = "debora-ebac-modulo-44-enriched"

class S3Service:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3_client = boto3.client("s3")

    def list_objects(self, prefix: str):
        """Lista objetos em um bucket com um prefixo."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return response.get("Contents", [])

    def download_file(self, key: str, local_path: str):
        """Faz download de um arquivo do S3."""
        self.s3_client.download_file(self.bucket, key, local_path)

    def upload_parquet(self, local_path: str, prefix: str):
        """Faz upload de um arquivo Parquet para o S3."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        remote_path = f"{prefix}/{timestamp}.parquet"
        self.s3_client.upload_file(local_path, ENRICHED_BUCKET, remote_path)
        return remote_path

def parse_messages(data: list) -> list:
    """
    Função para percorrer uma lista de mensagens e extrair informações relevantes.
    
    :param data: Lista de dicionários contendo mensagens.
    :return: Lista de dicionários com os dados estruturados.
    """
    parsed_messages = []
    for item in data:
        try:
            message = item.get("message", {})
            parsed_messages.append({
                "message_id": message.get("message_id"),
                "user_id": message.get("from", {}).get("id"),
                "user_first_name": message.get("from", {}).get("first_name"),
                "is_bot": message.get("from", {}).get("is_bot"),
                "chat_id": message.get("chat", {}).get("id"),
                "chat_type": message.get("chat", {}).get("type"),
                "text": message.get("text"),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            logger.error(f"Erro ao processar a mensagem: {item}. Detalhes: {e}")
    return parsed_messages

def etl_process():
    """Executa o processo ETL."""
    tzinfo = timezone(offset=timedelta(hours=3))
    date = (datetime.now(tzinfo) - timedelta(days=1)).strftime("%Y-%m-%d")
    prefix = f"telegram/context_date={date}"
    s3_service = S3Service(bucket=RAW_BUCKET)
    files = s3_service.list_objects(prefix=prefix)

    if not files:
        logger.info("Nenhum arquivo encontrado para processar.")
        return {"message": "Nenhum arquivo encontrado para o ETL."}

    table = None
    for file in files:
        key = file["Key"]
        local_file = f"/tmp/{key.split('/')[-1]}"
        s3_service.download_file(key=key, local_path=local_file)

        with open(local_file, mode="r", encoding="utf8") as fp:
            try:
                data = json.load(fp)
                logger.info(f"Processando arquivo: {local_file}")
                parsed_data = parse_messages(data["result"])
                if not parsed_data:
                    logger.warning(f"Nenhuma mensagem válida encontrada no arquivo: {local_file}")
                    continue
                
                # Converter dados para PyArrow
                iter_table = pa.Table.from_pylist(parsed_data)
                table = pa.concat_tables([table, iter_table]) if table else iter_table

            except json.JSONDecodeError as e:
                logger.error(f"Erro ao carregar JSON do arquivo {local_file}: {e}")
            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {local_file}: {e}")

    if table:
        local_parquet = f"/tmp/etl_output.parquet"
        pq.write_table(table=table, where=local_parquet)
        uploaded_path = s3_service.upload_parquet(local_path=local_parquet, prefix=prefix)
        logger.info(f"Arquivo Parquet enviado para: {uploaded_path}")
        return {"message": "ETL executado com sucesso.", "file": uploaded_path}
    else:
        logger.warning("Nenhum dado foi processado durante o ETL.")
        return {"message": "ETL concluído, mas nenhum dado foi processado."}

if __name__ == "__main__":
    result = etl_process()
    print(result)
