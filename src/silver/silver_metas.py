"""Silver das metas: um script parametrizado para brasil, uf e municipio."""

import sys

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, fora_da_faixa, checar

BUCKET = "fiap-tc2-286958704145"

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

        chaves = ["ano", "rede"]
        if meta["chave_geo"]:
            chaves.append(meta["chave_geo"])

        checar(nome, {
            "nulos em chave": nulos_em(df, chaves),
            "duplicidade de chave": duplicados_em(df, chaves),
            "taxa fora de 0-100": fora_da_faixa(df, "taxa_alfabetizacao", 0, 100),
        })

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
