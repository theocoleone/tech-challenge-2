"""
Ingestao Bronze - Tech Challenge Fase 2

Puxa as tabelas do dataset publico de alfabetizacao no BigQuery (via Base dos Dados)
e grava na camada Bronze do data lake (S3), em formato Parquet, particionado por ano.

A camada Bronze preserva os dados brutos, sem transformacao. O objetivo e ter uma
copia fiel da fonte, com historico, sobre a qual as camadas Silver e Gold vao operar.
"""

import io
import sys
import time
import logging

import boto3
import basedosdados as bd

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

# Projeto GCP que "paga" as queries no BigQuery (dentro do free tier, custo zero)
GCP_BILLING_PROJECT = "aist-tech-challenge-2"

# Destino no S3
S3_BUCKET = "fiap-tc2-286958704145"
S3_PROFILE = "fiap-tech-challenge"
BRONZE_PREFIX = "bronze"

# Fontes a ingerir. Cada entrada define:
#   - name:      nome logico da fonte (vira a "pasta" na Bronze)
#   - dataset:   dataset no BigQuery
#   - table:     tabela no dataset
#   - partition: coluna usada para particionar (ou None se a tabela nao tem)
SOURCES = [
    {"name": "alunos",         "dataset": "br_inep_avaliacao_alfabetizacao", "table": "alunos",                        "partition": "ano"},
    {"name": "municipio",      "dataset": "br_inep_avaliacao_alfabetizacao", "table": "municipio",                     "partition": "ano"},
    {"name": "uf",             "dataset": "br_inep_avaliacao_alfabetizacao", "table": "uf",                            "partition": "ano"},
    {"name": "meta_brasil",    "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_brasil",     "partition": "ano"},
    {"name": "meta_uf",        "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_uf",         "partition": "ano"},
    {"name": "meta_municipio", "dataset": "br_inep_avaliacao_alfabetizacao", "table": "meta_alfabetizacao_municipio",  "partition": "ano"},
    # Fonte auxiliar: diretorio de municipios (fornece nome e sigla_uf).
    # Nao tem coluna de ano, entao nao particionamos.
    {"name": "diretorio_municipio", "dataset": "br_bd_diretorios_brasil", "table": "municipio", "partition": None},
]

# ---------------------------------------------------------------------------
# Logging simples e estruturado
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze")


# ---------------------------------------------------------------------------
# Funcoes
# ---------------------------------------------------------------------------

def read_source(source):
    """Le uma tabela do BigQuery e devolve um DataFrame."""
    full_table = f"basedosdados.{source['dataset']}.{source['table']}"
    query = f"SELECT * FROM `{full_table}`"
    log.info(f"Lendo {full_table} ...")
    df = bd.read_sql(query=query, billing_project_id=GCP_BILLING_PROJECT)
    log.info(f"  {len(df):,} linhas lidas de {source['name']}")
    return df


def upload_parquet(df, s3, key):
    """Escreve um DataFrame como Parquet em memoria e faz upload para o S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, key)
    log.info(f"  -> s3://{S3_BUCKET}/{key} ({len(df):,} linhas)")


def ingest_source(source, s3):
    """Ingere uma fonte na Bronze, particionando por ano quando aplicavel."""
    df = read_source(source)
    base = f"{BRONZE_PREFIX}/{source['name']}"

    if source["partition"] is None:
        # Sem particao: grava um unico arquivo
        key = f"{base}/{source['name']}.parquet"
        upload_parquet(df, s3, key)
    else:
        # Particionamento Hive-style: uma pasta por valor do ano (ano=2023/, ano=2024/)
        col = source["partition"]
        for value in sorted(df[col].dropna().unique()):
            partition_df = df[df[col] == value]
            key = f"{base}/{col}={value}/{source['name']}.parquet"
            upload_parquet(partition_df, s3, key)


def main(only=None):
    """Ingere as fontes na Bronze.

    only: lista opcional de nomes de fonte para ingerir apenas essas.
          Passada via linha de comando, ex: `python bronze_ingestion.py uf`.
          Util para testar a conexao ponta a ponta com uma tabela pequena
          antes de rodar as fontes maiores (como alunos, com 3.8M linhas).
          Sem argumento, ingere todas as fontes.
    """
    sources = SOURCES
    if only:
        sources = [s for s in SOURCES if s["name"] in only]
        if not sources:
            log.error(f"Nenhuma fonte corresponde a {only}. Fontes validas: "
                      f"{[s['name'] for s in SOURCES]}")
            return
        log.info(f"Modo seletivo: ingerindo apenas {[s['name'] for s in sources]}")

    log.info("Iniciando ingestao Bronze")
    session = boto3.Session(profile_name=S3_PROFILE)
    s3 = session.client("s3")

    start = time.time()
    for source in sources:
        try:
            ingest_source(source, s3)
        except Exception as exc:
            log.error(f"Falha ao ingerir {source['name']}: {exc}")
            raise

    elapsed = time.time() - start
    log.info(f"Ingestao Bronze concluida em {elapsed:.1f}s")


if __name__ == "__main__":
    # Nomes de fonte passados como argumentos rodam so essas fontes.
    # Ex: python bronze_ingestion.py uf
    # Sem argumentos, roda todas.
    selected = sys.argv[1:] if len(sys.argv) > 1 else None
    main(only=selected)
