"""Investiga por que 242 municipios tem indicador mas nao tem meta 2030.

Le a Silver (dado ja limpo) e cruza o indicador municipal com a meta e com
os microdados de alunos. Embasa a secao "Por que faltam metas" da exploracao.
"""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark

BUCKET = "fiap-tc2-286958704145"
SILVER = f"s3a://{BUCKET}/silver"
REDE_MUNICIPAL = "3"


def montar_base(spark):
    indicador = (
        spark.read.parquet(f"{SILVER}/municipio/")
        .filter(F.col("rede") == REDE_MUNICIPAL)
        .select("id_municipio", "nome", "sigla_uf", "ano",
                F.col("taxa_alfabetizacao").alias("taxa_oficial"))
    )
    meta = spark.read.parquet(f"{SILVER}/meta_municipio/").select("id_municipio", "ano").distinct()

    indicador = (
        indicador
        .join(meta.withColumn("tem_meta", F.lit(1)), on=["id_municipio", "ano"], how="left")
        .withColumn("orfao", F.col("tem_meta").isNull())
    )

    alunos = spark.read.parquet(f"{SILVER}/alunos/").filter(F.col("rede") == REDE_MUNICIPAL)
    contagem = alunos.groupBy("id_municipio", "ano").agg(
        F.count("*").alias("n_matric"),
        F.sum(F.when(F.col("proficiencia").isNotNull(), 1).otherwise(0)).alias("n_prova"),
        F.sum(F.when(F.col("alfabetizado") == "1", 1).otherwise(0)).alias("n_alfab"),
    )

    return (
        indicador.join(contagem, on=["id_municipio", "ano"], how="left")
        .withColumn("participacao", F.round(F.col("n_prova") / F.col("n_matric") * 100, 1))
    )


def cobertura(base):
    orfaos = base.filter(F.col("orfao"))
    print("Municipio-anos com indicador:", base.count())
    print("Municipio-anos orfaos (sem meta):", orfaos.count())
    print("Municipios distintos orfaos:", orfaos.select("id_municipio").distinct().count())
    print("Persistencia (em quantos anos o municipio ficou orfao):")
    (orfaos.groupBy("id_municipio").count()
        .groupBy(F.col("count").alias("anos_como_orfao")).count()
        .orderBy("anos_como_orfao").show())


def padrao_participacao(base):
    print("Participacao na prova por grupo:")
    (base.groupBy("orfao").agg(
        F.round(F.avg("participacao"), 1).alias("media_pct"),
        F.expr("percentile_approx(participacao, 0.5)").alias("mediana_pct"),
        F.round(F.avg("taxa_oficial"), 1).alias("media_taxa_oficial"),
    ).orderBy("orfao").show())

    print("Taxa de orfandade por faixa de participacao:")
    faixa = base.filter(F.col("participacao").isNotNull()).withColumn(
        "faixa",
        F.when(F.col("participacao") < 60, "1: <60%")
         .when(F.col("participacao") < 70, "2: 60-70%")
         .when(F.col("participacao") < 80, "3: 70-80%")
         .when(F.col("participacao") < 90, "4: 80-90%")
         .otherwise("5: >=90%"),
    )
    (faixa.groupBy("faixa").agg(
        F.count("*").alias("total"),
        F.sum(F.col("orfao").cast("int")).alias("orfaos"),
        F.round(F.avg(F.col("orfao").cast("int")) * 100, 1).alias("pct_orfao"),
    ).orderBy("faixa").show())


def contraexemplos(base):
    print("Orfaos com rede grande (50+ provas) - cidades grandes ficam sem meta se a participacao e baixa:")
    (base.filter(F.col("orfao") & (F.col("n_prova") >= 50))
        .select("nome", "sigla_uf", "ano", "n_matric", "n_prova", "taxa_oficial", "participacao")
        .orderBy(F.col("n_prova").desc()).show(15, truncate=False))


def validar_denominador(base):
    print("Denominador da taxa oficial (municipios com meta): bate com o calculo sobre quem fez a prova:")
    (base.filter(~F.col("orfao") & (F.col("n_prova") > 0))
        .withColumn("taxa_sobre_prova", F.round(F.col("n_alfab") / F.col("n_prova") * 100, 1))
        .withColumn("taxa_sobre_matric", F.round(F.col("n_alfab") / F.col("n_matric") * 100, 1))
        .select("nome", "sigla_uf", "ano", "taxa_oficial", "taxa_sobre_prova", "taxa_sobre_matric")
        .show(8, truncate=False))


def main():
    spark = get_spark("analise-metas-ausentes")
    base = montar_base(spark).cache()

    cobertura(base)
    padrao_participacao(base)
    contraexemplos(base)
    validar_denominador(base)

    spark.stop()


if __name__ == "__main__":
    main()
