# Pipeline de Dados do Telegram

Este projeto implementa um pipeline de dados que integra mensagens do Telegram com serviços da AWS, incluindo S3, Lambda, API Gateway, Athena e AirFlow. 

O objetivo é criar um pipeline orientado a eventos agendados realizar a ingestão, transformação e apresentação dos dados de mensagens enviadas ao bot do Telegram.

## Arquitetura do Pipeline

![Arquitetura do Pipeline](https://github.com/Debora-Rodrigues-19/repo-projeto-final-ebac-44/blob/main/arquitetura-ebac-44.jpeg?raw=true)

O pipeline é dividido em três etapas principais: Ingestão, Transformação e Apresentação.


1. **Ingestão**:
   - Mensagens do bot do Telegram que são capturadas via API e armazenadas no formato JSON em um bucket S3 com a camada "raw".
2. **Transformação (ETL)**:
   - Dados JSON são transformados em formato Parquet e armazenados em outro bucket S3 com a camada "enriched" para análise.
3. **Apresentação**:
   - Os dados enriquecidos são consultados usando AWS Athena com SQL.
  
## 1. Etapa de Ingestão 
As mensagens serão captadas as por um bot podem ser acessadas via API, no formato JSON. 
Através do API Gateway que foi acessado pelo AWS CLI no terminal.
Com o seguinte código: XXX 



### 1. Bucket AWS S3
- s3://debora-ebac-modulo-44-raw
- s3://debora-ebac-modulo-44-enriched


