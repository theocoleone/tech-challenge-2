"""Silver da tabela uf: limpeza, decodificacao e validacao."""

import sys

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, decode
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, fora_da_faixa, checar

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/uf/"
SILVER = f"s3a://{BUCKET}/silver/uf/"


def main():
    spark = get_spark("silver-uf")

    with Etapa("silver_uf", camada="silver") as etapa:
        df = spark.read.parquet(BRONZE)
        df = df.dropDuplicates()

        df = df.withColumn("rede_desc", decode("rede", REDE))
        df = df.withColumn("serie_desc", decode("serie", SERIE))

        checar("silver_uf", {
            "nulos em chave": nulos_em(df, ["ano", "sigla_uf", "rede"]),
            "duplicidade de chave": duplicados_em(df, ["ano", "sigla_uf", "rede"]),
            "taxa fora de 0-100": fora_da_faixa(df, "taxa_alfabetizacao", 0, 100),
        })

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    spark.stop()


if __name__ == "__main__":
    main()
