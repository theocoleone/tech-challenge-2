"""Silver da tabela uf: limpeza, decodificacao e validacao."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, decode

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/uf/"
SILVER = f"s3a://{BUCKET}/silver/uf/"


def main():
    spark = get_spark("silver-uf")

    print(f"Lendo {BRONZE}")
    df = spark.read.parquet(BRONZE)
    print(f"  {df.count()} linhas na entrada")

    df = df.dropDuplicates()

    # Mantem os codigos originais e adiciona colunas de descricao
    df = df.withColumn("rede_desc", decode("rede", REDE))
    df = df.withColumn("serie_desc", decode("serie", SERIE))

    nulos_chave = df.filter(
        F.col("ano").isNull() | F.col("sigla_uf").isNull() | F.col("rede").isNull()
    ).count()
    fora_faixa = df.filter(
        (F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)
    ).count()
    print(f"  Validacao | nulos em chave: {nulos_chave} | taxa fora de 0-100: {fora_faixa}")

    # Nao grava se a qualidade falhar
    if nulos_chave > 0 or fora_faixa > 0:
        raise ValueError("Falha de qualidade na uf: nulos em chave ou taxa fora de faixa.")

    print(f"  {df.count()} linhas na saida -> {SILVER}")
    df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    print("Silver uf concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
