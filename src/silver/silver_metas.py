"""Silver das metas (brasil, uf, municipio). As tres compartilham estrutura,
entao um unico script parametrizado processa todas, evitando codigo repetido."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark

BUCKET = "fiap-tc2-286958704145"

# Cada meta so difere na chave geografica. brasil nao tem (e nacional).
METAS = [
    {"name": "meta_brasil",    "chave_geo": None},
    {"name": "meta_uf",        "chave_geo": "sigla_uf"},
    {"name": "meta_municipio", "chave_geo": "id_municipio"},
]


def processar_meta(spark, meta):
    nome = meta["name"]
    bronze = f"s3a://{BUCKET}/bronze/{nome}/"
    silver = f"s3a://{BUCKET}/silver/{nome}/"

    print(f"Lendo {bronze}")
    df = spark.read.parquet(bronze)
    print(f"  {df.count()} linhas na entrada")

    df = df.dropDuplicates()
    # Diferente de uf/municipio, aqui rede ja vem como texto ("Publica"), nao codigo

    # Chaves a validar: ano e rede sempre; a chave geografica so quando existe
    chaves = [F.col("ano").isNull(), F.col("rede").isNull()]
    if meta["chave_geo"]:
        chaves.append(F.col(meta["chave_geo"]).isNull())
    condicao_nula = chaves[0]
    for c in chaves[1:]:
        condicao_nula = condicao_nula | c

    nulos_chave = df.filter(condicao_nula).count()
    fora_faixa = df.filter(
        (F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)
    ).count()
    print(f"  Validacao | nulos em chave: {nulos_chave} | taxa fora de 0-100: {fora_faixa}")

    if nulos_chave > 0 or fora_faixa > 0:
        raise ValueError(f"Falha de qualidade em {nome}: nulos em chave ou taxa fora de faixa.")

    print(f"  {df.count()} linhas na saida -> {silver}")
    df.write.mode("overwrite").partitionBy("ano").parquet(silver)
    print(f"  {nome} concluida.\n")


def main():
    spark = get_spark("silver-metas")
    for meta in METAS:
        processar_meta(spark, meta)
    print("Silver das metas concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
