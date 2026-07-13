"""Silver do diretorio de municipios: tabela de referencia geografica, enxuta."""

import sys

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, checar

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/diretorio_municipio/"

# So as colunas com valor analitico; o diretorio bruto tem 27
COLUNAS = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao", "capital_uf", "amazonia_legal"]


def main():
    spark = get_spark("silver-diretorio-municipio")

    with Etapa("silver_diretorio_municipio", camada="silver") as etapa:
        df = spark.read.parquet(BRONZE).select(*COLUNAS)
        df = df.dropDuplicates()

        checar("silver_diretorio_municipio", {
            "nulos em id_municipio": nulos_em(df, ["id_municipio"]),
            "id_municipio duplicado": duplicados_em(df, "id_municipio"),
        })

        # Sem particao por ano: e um cadastro de referencia
        etapa.linhas = df.count()
        df.write.mode("overwrite").parquet(SILVER)

    spark.stop()


if __name__ == "__main__":
    main()
