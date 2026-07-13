"""Ingestao Bronze: puxa as tabelas do BigQuery e grava no S3 em Parquet, particionado por ano.

Roda local (auth via gcloud) ou como job Glue Python Shell (auth via chave de
service account guardada no Secrets Manager).
"""

import io
import os
import sys
import time
import json
import logging

import boto3
from google.cloud import bigquery

GCP_BILLING_PROJECT = "aist-tech-challenge-2"
S3_BUCKET = "fiap-tc2-286958704145"
S3_PROFILE = "fiap-tech-challenge"
BRONZE_PREFIX = "bronze"
GCP_SECRET_NAME = "gcp-service-account-bronze"
AWS_REGION = "us-east-1"

# Fontes a ingerir. partition=None para tabelas sem coluna de ano.
SOURCES = [
    {"name": "alunos",         "dataset": "br_inep_avaliacao_alfabetizacao", "table": "alunos",                        "partition": "ano"},
    {"name": "municipio",      "dataset": "br_inep_avaliacao_alfabetizacao", "table": "municipio",                     "partition": "ano"},
    {"name": "uf",             "dataset": "br_inep_avaliacao_alfabetizacao", "table": "uf",                            "partition": "ano"},
    {"name": "meta_brasil",    "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_brasil",     "partition": "ano"},
    {"name": "meta_uf",        "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_uf",         "partition": "ano"},
    {"name": "meta_municipio", "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_municipio",  "partition": "ano"},
    {"name": "diretorio_municipio", "dataset": "br_bd_diretorios_brasil", "table": "municipio", "partition": None},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze")


def _rodando_no_glue():
    return "GLUE_VERSION" in os.environ or "GLUE_INSTALLATION" in os.environ


def configurar_credenciais_gcp():
    """No Glue, busca a chave da service account no Secrets Manager e aponta a
    variavel que o cliente BigQuery usa. Local, o gcloud ja cuida disso."""
    if not _rodando_no_glue():
        return

    secret = boto3.client("secretsmanager", region_name=AWS_REGION)
    chave = secret.get_secret_value(SecretId=GCP_SECRET_NAME)["SecretString"]

    caminho = "/tmp/gcp-key.json"
    with open(caminho, "w") as f:
        f.write(chave)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = caminho
    log.info("Credenciais GCP configuradas via Secrets Manager")


def read_source(bq, source):
    full_table = f"basedosdados.{source['dataset']}.{source['table']}"
    log.info(f"Lendo {full_table} ...")
    df = bq.query(f"SELECT * FROM `{full_table}`").to_dataframe()
    log.info(f"  {len(df):,} linhas lidas de {source['name']}")
    return df


def upload_parquet(df, s3, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, key)
    log.info(f"  -> s3://{S3_BUCKET}/{key} ({len(df):,} linhas)")


def ingest_source(source, bq, s3):
    df = read_source(bq, source)
    base = f"{BRONZE_PREFIX}/{source['name']}"

    if source["partition"] is None:
        upload_parquet(df, s3, f"{base}/{source['name']}.parquet")
        return

    # Uma pasta por ano (ano=2023/, ano=2024/) - particionamento Hive-style
    col = source["partition"]
    for value in sorted(df[col].dropna().unique()):
        partition_df = df[df[col] == value]
        upload_parquet(partition_df, s3, f"{base}/{col}={value}/{source['name']}.parquet")


def main(only=None):
    """Ingere as fontes. `only` restringe a uma lista de nomes (via argumento
    de linha de comando), util para testar com uma tabela pequena antes de
    rodar a alunos (3.8M linhas)."""
    sources = SOURCES
    if only:
        sources = [s for s in SOURCES if s["name"] in only]
        if not sources:
            log.error(f"Nenhuma fonte corresponde a {only}. Validas: {[s['name'] for s in SOURCES]}")
            return
        log.info(f"Modo seletivo: {[s['name'] for s in sources]}")

    log.info("Iniciando ingestao Bronze")
    configurar_credenciais_gcp()
    bq = bigquery.Client(project=GCP_BILLING_PROJECT)
    # No Glue as credenciais AWS vem do papel do job; local usa o profile
    if _rodando_no_glue():
        s3 = boto3.client("s3")
    else:
        s3 = boto3.Session(profile_name=S3_PROFILE).client("s3")

    start = time.time()
    for source in sources:
        try:
            ingest_source(source, bq, s3)
        except Exception as exc:
            log.error(f"Falha ao ingerir {source['name']}: {exc}")
            raise

    log.info(f"Ingestao Bronze concluida em {time.time() - start:.1f}s")


if __name__ == "__main__":
    # No Glue nao ha argumentos de linha de comando (roda todas as fontes);
    # local aceita nomes de fonte para teste seletivo.
    main(only=None if _rodando_no_glue() else (sys.argv[1:] or None))
