"""Silver das metas: um script parametrizado para brasil, uf e municipio."""

import sys

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, fora_da_faixa, checar, avisar

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

        regras = {
            "nulos em chave": nulos_em(df, chaves),
            "duplicidade de chave": duplicados_em(df, chaves),
            "taxa fora de 0-100": fora_da_faixa(df, "taxa_alfabetizacao", 0, 100),
            "meta 2030 fora de 0-100": fora_da_faixa(
                df, "meta_alfabetizacao_2030", 0, 100
            ),
        }

        avisar(nome, {
            # A taxa realizada vem das tabelas de indicador na integração.
            "taxa ausente na fonte": nulos_em(df, ["taxa_alfabetizacao"]),
        })

        if nome == "meta_uf":
            # RR não possui meta 2030 em 2023/2024 e também não possui
            # indicador correspondente para avançar à Gold.
            avisar(nome, {
                "meta 2030 ausente na fonte": nulos_em(
                    df, ["meta_alfabetizacao_2030"]
                ),
            })
        else:
            regras["nulos em meta 2030"] = nulos_em(
                df, ["meta_alfabetizacao_2030"]
            )

        checar(nome, regras)

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
