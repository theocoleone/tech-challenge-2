"""Silver das metas: um script parametrizado para brasil, uf e municipio."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa

BUCKET = "fiap-tc2-286958704145"

# brasil nao tem chave geografica (e nacional)
METAS = [
    {"name": "meta_brasil",    "chave_geo": None},
    {"name": "meta_uf",        "chave_geo": "sigla_uf"},
    {"name": "meta_municipio", "chave_geo": "id_municipio"},
]


def processar_meta(spark, meta):
    nome = meta["name"]
    bronze = f"s3a://{BUCKET}/bronze/{nome}/"
    silver = f"s3a://{BUCKET}/silver/{nome}/"

    with Etapa(f"silver_{nome}", camada="silver") as etapa:
        df = spark.read.parquet(bronze)
        df = df.dropDuplicates()
        # Aqui rede ja vem como texto, nao codigo (nao precisa decodificar)

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

        if nulos_chave > 0 or fora_faixa > 0:
            raise ValueError(f"Falha de qualidade em {nome}: nulos em chave ou taxa fora de faixa.")

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(silver)


def main():
    spark = get_spark("silver-metas")
    for meta in METAS:
        processar_meta(spark, meta)
    print("Silver das metas concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
