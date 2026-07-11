"""Silver do diretorio de municipios: tabela de referencia geografica, enxuta."""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/diretorio_municipio/"

# So as colunas com valor analitico; o diretorio bruto tem 27
COLUNAS = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao", "capital_uf", "amazonia_legal"]


def main():
    spark = get_spark("silver-diretorio-municipio")

    print(f"Lendo {BRONZE}")
    df = spark.read.parquet(BRONZE).select(*COLUNAS)
    print(f"  {df.count()} linhas na entrada")

    df = df.dropDuplicates()

    nulos_chave = df.filter(F.col("id_municipio").isNull()).count()
    duplicados = df.count() - df.select("id_municipio").distinct().count()
    print(f"  Validacao | nulos em id_municipio: {nulos_chave} | id_municipio duplicado: {duplicados}")

    if nulos_chave > 0 or duplicados > 0:
        raise ValueError("Falha de qualidade no diretorio: chave nula ou duplicada.")

    # Sem particao por ano: e um cadastro de referencia, nao tem dimensao temporal
    print(f"  {df.count()} linhas na saida -> {SILVER}")
    df.write.mode("overwrite").parquet(SILVER)

    print("Silver diretorio_municipio concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
