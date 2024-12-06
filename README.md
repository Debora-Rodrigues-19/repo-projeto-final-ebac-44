# 🚀 **Pipeline de Dados do Telegram**  

Este projeto implementa um pipeline de dados robusto que integra mensagens enviadas ao bot do Telegram com diversos serviços da AWS, incluindo **S3**, **Lambda**, **API Gateway**, **Athena** e **Airflow**.  

O objetivo é criar um pipeline orientado a eventos e agendado, capaz de realizar a ingestão, transformação e análise de mensagens enviadas ao bot.  

---
## 🏗️ **Arquitetura do Pipeline**  
![Arquitetura do Pipeline](https://github.com/Debora-Rodrigues-19/repo-projeto-final-ebac-44/blob/main/arquitetura-ebac-44.jpeg?raw=true)  

---
O pipeline é dividido em três etapas principais:  
1. **Ingestão**  
2. **Transformação (ETL)**  
3. **Análises**  
---

## 1️⃣ **Ingestão**  
 
- **Integração com o Telegram**: Foi criada uma API utilizando **AWS Lambda** e **API Gateway** para integrar com a API REST do Telegram.  
- **Agendamento**: Configuração de uma **DAG no Airflow** para acionar a API em micro-batches a cada **2 minutos**.  
- **Captura de Mensagens**: A API é responsável por coletar as mensagens enviadas ao bot do Telegram.  
- **Armazenamento**: As mensagens capturadas são armazenadas no formato **JSON** na camada "raw" de um bucket S3 (`s3://debora-ebac-modulo-44-raw`).

- Link do código da API para capturar as mensagens e salvá-las no bucket: [🔗 Código API](#)  

- 🗂️ Amostra dos dados e arquivos Armazenados:
   - Acesso ao bot do telegram:
     ![acesso bot telegram]()
     
   - Bucket Raw e seus arquivos:
     ![telegram bucket raw]() 
     ![arquivo bucket raw]()
     
   - Mensagens coletadas são salvas no formato JSON:  
     ![Mensagens salvas](https://github.com/user-attachments/assets/74826335-edcc-4e2c-ad70-3956cfb41af9)
      
   - Exemplo de JSON capturado:
     ![Exemplo JSON](https://github.com/user-attachments/assets/8e92efe5-a186-4d23-a84e-84addebeaa67)

---

## 2️⃣ **Transformação (ETL)**  

- **Processamento dos Dados**: Uso de **script Python** para processar e transformar os arquivos JSON, armazenados na camada "raw", no formato **Parquet**.  
- **Armazenamento Enriquecido**: Armazenamento dos dados transformados no formato Parquet na camada "enriched" de um bucket **S3** (`s3://debora-ebac-modulo-44-enriched`).  
- **Orquestração**: Gerenciamento desse processo por meio de uma **task encadeada** no Airflow, garantindo uma execução fluida e automatizada.

- Função Lambda para processar os dados: [🔗 Código Lambda](#)
    - Bucket dos dados enriquecidos:
      ![bucket enriched]()

---

### 3️⃣ **Análises**  

- **Criação da Tabela no Athena**: Uso do **AWS Athena** para criar uma tabela chamada **telegram**, permitindo consultas SQL diretamente sobre os dados estruturados no bucket "enriched".  
- **Geração de Resultados**: Execução de queries no Athena para gerar resultados e insights, armazenados na pasta `athena-query-results` dentro do bucket enriquecido.  


   - Athena query results:
     ![athena query results]()
     
   - Athena consultas sql:
     ![consulta sql]()

   - Athena query results:
     ![count by day]()

   - Athena query results:
     ![mensagem by user by day]()


     tamanho-mensagem-by-user-by-day-ebac-44.png
     

---

### **Orquestração com Airflow**  
Para gerenciar todo o pipeline, utilizamos o **Apache Airflow**, uma ferramenta consolidada no ecossistema de Big Data.  

   - Pipeline no airflow
     ![imagem pipeline airflow]()  

