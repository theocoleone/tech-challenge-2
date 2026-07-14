"""Silver da tabela municipio: enriquece com nome e UF, limpa, decodifica e valida."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, decode
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, chaves_orfas, fora_da_faixa, checar

BUCKET = "fiap-tc2-286958704145"
BRONZE_MUNICIPIO = f"s3a://{BUCKET}/bronze/municipio/"
BRONZE_DIRETORIO = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/municipio/"


def main():
    spark = get_spark("silver-municipio")

    with Etapa("silver_municipio", camada="silver") as etapa:
        df = spark.read.parquet(BRONZE_MUNICIPIO)

        diretorio = (
            spark.read.parquet(BRONZE_DIRETORIO)
            .select("id_municipio", "nome", "sigla_uf")
        )

        df = df.dropDuplicates()
        df = df.join(diretorio, on="id_municipio", how="left")

        df = df.withColumn("rede_desc", decode("rede", REDE))
        df = df.withColumn("serie_desc", decode("serie", SERIE))

        checar("silver_municipio", {
            "nulos em chave": nulos_em(df, ["ano", "id_municipio", "rede"]),
            "nulos em campos obrigatorios": nulos_em(
                df, ["serie", "taxa_alfabetizacao"]
            ),
            "duplicidade de chave": duplicados_em(df, ["ano", "id_municipio", "rede"]),
            "taxa fora de 0-100": fora_da_faixa(df, "taxa_alfabetizacao", 0, 100),
            "municipios sem UF": df.filter(F.col("sigla_uf").isNull()).count(),
            "municipios orfaos do diretorio": chaves_orfas(df, diretorio, "id_municipio"),
        })

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    spark.stop()


if __name__ == "__main__":
    main()
