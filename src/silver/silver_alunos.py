"""Silver dos microdados de alunos: limpa, decodifica e enriquece com UF.

Mantem o grao de aluno (uma linha por criança). Serve de base para modelos
de ML e para validar a taxa oficial das tabelas agregadas. A agregacao em si
fica para a Gold.
"""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, PRESENCA, PREENCHIMENTO_CADERNO, ALFABETIZADO, decode

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/alunos/"
BRONZE_DIRETORIO = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/alunos/"

# Ponto de corte oficial do SAEB (Pesquisa Alfabetiza Brasil 2023): a partir de
# 743 a crianca e considerada alfabetizada. Usado na Gold para recalcular a taxa.
CORTE_ALFABETIZACAO = 743


def main():
    spark = get_spark("silver-alunos")

    print(f"Lendo {BRONZE}")
    df = spark.read.parquet(BRONZE)
    print(f"  {df.count()} linhas na entrada")

    diretorio = (
        spark.read.parquet(BRONZE_DIRETORIO)
        .select("id_municipio", "nome", "sigla_uf")
    )

    df = df.dropDuplicates()

    df = df.withColumn("rede_desc", decode("rede", REDE))
    df = df.withColumn("serie_desc", decode("serie", SERIE))
    df = df.withColumn("presenca_desc", decode("presenca", PRESENCA))
    df = df.withColumn("preenchimento_caderno_desc", decode("preenchimento_caderno", PREENCHIMENTO_CADERNO))
    df = df.withColumn("alfabetizado_desc", decode("alfabetizado", ALFABETIZADO))

    df = df.join(diretorio, on="id_municipio", how="left")

    nulos_chave = df.filter(
        F.col("ano").isNull() | F.col("id_municipio").isNull() | F.col("id_aluno").isNull()
    ).count()
    # alfabetizado so pode ser 0 ou 1
    alfabetizado_invalido = df.filter(~F.col("alfabetizado").isin("0", "1")).count()
    sem_uf = df.filter(F.col("sigla_uf").isNull()).count()
    print(f"  Validacao | nulos em chave: {nulos_chave} | alfabetizado invalido: {alfabetizado_invalido} | sem UF: {sem_uf}")

    if nulos_chave > 0 or alfabetizado_invalido > 0:
        raise ValueError("Falha de qualidade em alunos: nulos em chave ou alfabetizado invalido.")

    print(f"  {df.count()} linhas na saida -> {SILVER}")
    df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    print("Silver alunos concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
