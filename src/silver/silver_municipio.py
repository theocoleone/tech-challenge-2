"""Silver da tabela municipio: enriquece com nome e UF, limpa, decodifica e valida."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, decode

BUCKET = "fiap-tc2-286958704145"
BRONZE_MUNICIPIO = f"s3a://{BUCKET}/bronze/municipio/"
BRONZE_DIRETORIO = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/municipio/"


def main():
    spark = get_spark("silver-municipio")

    print(f"Lendo {BRONZE_MUNICIPIO}")
    df = spark.read.parquet(BRONZE_MUNICIPIO)
    print(f"  {df.count()} linhas na entrada")

    # Diretorio de municipios: so as colunas que faltam na tabela de indicadores
    diretorio = (
        spark.read.parquet(BRONZE_DIRETORIO)
        .select("id_municipio", "nome", "sigla_uf")
    )

    df = df.dropDuplicates()

    # Enriquece com nome e sigla_uf. left join preserva todo indicador,
    # mesmo que falte par no diretorio (a validacao abaixo detecta orfaos).
    df = df.join(diretorio, on="id_municipio", how="left")

    df = df.withColumn("rede_desc", decode("rede", REDE))
    df = df.withColumn("serie_desc", decode("serie", SERIE))

    nulos_chave = df.filter(
        F.col("ano").isNull() | F.col("id_municipio").isNull() | F.col("rede").isNull()
    ).count()
    fora_faixa = df.filter(
        (F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)
    ).count()
    sem_uf = df.filter(F.col("sigla_uf").isNull()).count()
    print(f"  Validacao | nulos em chave: {nulos_chave} | taxa fora de 0-100: {fora_faixa} | municipios sem UF: {sem_uf}")

    if nulos_chave > 0 or fora_faixa > 0:
        raise ValueError("Falha de qualidade na municipio: nulos em chave ou taxa fora de faixa.")

    print(f"  {df.count()} linhas na saida -> {SILVER}")
    df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    print("Silver municipio concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
