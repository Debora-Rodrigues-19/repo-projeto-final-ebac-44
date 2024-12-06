import json
import logging
import boto3
import requests
from datetime import datetime, timedelta, timezone
from chalice import Chalice, Response

# Inicializa a aplicação Chalice
app = Chalice(app_name='api-telegram-etl')

# Configuração de logs
logging.basicConfig(level=logging.INFO)

# Configurações de ambiente e buckets hardcoded
RAW_BUCKET = "debora-ebac-modulo-44-raw"
TELEGRAM_TOKEN = "XXXXXXXX" # <- remover isso
BASE_URL = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates'

class TelegramService:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_updates(self) -> dict:
        """Busca atualizações da API do Telegram."""
        response = requests.get(self.base_url)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar API do Telegram: {response.status_code}")
        
        data = response.json()
        if not data.get("ok"):
            raise Exception("Resposta inválida da API do Telegram.")
        
        return data

class S3Service:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3_client = boto3.client('s3')

    def upload_json(self, data: dict, folder: str = "telegram"):
        """Faz upload de um arquivo JSON para o bucket S3."""
        tzinfo = timezone(offset=timedelta(hours=-3))
        date = datetime.now(tzinfo).strftime('%Y-%m-%d')
        timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')
        filename = f'{folder}/context_date={date}/{timestamp}.json'

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=filename,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        return filename

@app.route('/ingest', methods=['POST'], cors=True)
def ingest_telegram_updates():
    """Endpoint para consumir a API do Telegram e salvar os dados no S3."""
    telegram_service = TelegramService(base_url=BASE_URL)
    s3_service = S3Service(bucket=RAW_BUCKET)

    try:
        updates = telegram_service.fetch_updates()
        file_path = s3_service.upload_json(data=updates)
        return Response(body={"message": f"Dados enviados ao S3 com sucesso!", "file": file_path},
                        status_code=200)
    except Exception as e:
        logging.error(f"Erro ao processar os dados: {e}")
        return Response(body={"error": "Erro interno do servidor", "details": str(e)},
                        status_code=500)
