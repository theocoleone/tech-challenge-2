"""Camada Gold: datasets analiticos por municipio e UF.

Foca na rede Municipal, alvo do Compromisso Nacional Crianca Alfabetizada.
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa

BUCKET = "fiap-tc2-286958704145"
SILVER = f"s3a://{BUCKET}/silver"
GOLD = f"s3a://{BUCKET}/gold"

REDE_MUNICIPAL = "3"


def gold_indicador_municipio(spark):
    with Etapa("gold_indicador_municipio", camada="gold") as etapa:
        indicador = (
            spark.read.parquet(f"{SILVER}/municipio/")
            .filter(F.col("rede") == REDE_MUNICIPAL)
            .select("id_municipio", "nome", "sigla_uf", "ano", "taxa_alfabetizacao")
        )
        meta = (
            spark.read.parquet(f"{SILVER}/meta_municipio/")
            .select("id_municipio", "ano", F.col("meta_alfabetizacao_2030").alias("meta_2030"))
        )

        df = indicador.join(meta, on=["id_municipio", "ano"], how="left")
        df = df.withColumn("gap_para_meta_2030", F.round(F.col("meta_2030") - F.col("taxa_alfabetizacao"), 2))

        janela = Window.partitionBy("sigla_uf", "ano").orderBy(F.col("taxa_alfabetizacao").desc())
        df = df.withColumn("ranking_na_uf", F.row_number().over(janela))

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD}/indicador_municipio/")


def gold_metas_vs_resultados_uf(spark):
    with Etapa("gold_metas_vs_resultados_uf", camada="gold") as etapa:
        indicador = (
            spark.read.parquet(f"{SILVER}/uf/")
            .filter(F.col("rede") == REDE_MUNICIPAL)
            .select("sigla_uf", "ano", "taxa_alfabetizacao")
        )
        meta = (
            spark.read.parquet(f"{SILVER}/meta_uf/")
            .select("sigla_uf", "ano", F.col("meta_alfabetizacao_2030").alias("meta_2030"))
        )

        df = indicador.join(meta, on=["sigla_uf", "ano"], how="left")
        df = df.withColumn(
            "atingimento_meta_2030_pct",
            F.round(F.col("taxa_alfabetizacao") / F.col("meta_2030") * 100, 2),
        )

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD}/metas_vs_resultados_uf/")


def gold_evolucao_uf(spark):
    with Etapa("gold_evolucao_uf", camada="gold") as etapa:
        df = (
            spark.read.parquet(f"{SILVER}/uf/")
            .filter(F.col("rede") == REDE_MUNICIPAL)
            .select("sigla_uf", "ano", "taxa_alfabetizacao")
        )

        janela = Window.partitionBy("sigla_uf").orderBy("ano")
        df = df.withColumn("taxa_ano_anterior", F.lag("taxa_alfabetizacao").over(janela))
        df = df.withColumn("variacao_pp", F.round(F.col("taxa_alfabetizacao") - F.col("taxa_ano_anterior"), 2))

        etapa.linhas = df.count()
        df.write.mode("overwrite").parquet(f"{GOLD}/evolucao_uf/")


def main():
    spark = get_spark("gold-alfabetizacao")
    gold_indicador_municipio(spark)
    gold_metas_vs_resultados_uf(spark)
    gold_evolucao_uf(spark)
    spark.stop()


if __name__ == "__main__":
    main()
