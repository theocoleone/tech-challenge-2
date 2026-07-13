"""Silver de integracao: unifica indicadores com metas por territorio.

O PDF pede que a integracao das bases ocorra na Silver; a Gold so consome.
"""

import sys

from pyspark.sql import functions as F

sys.path.append("src")
from spark_session import get_spark
from monitoramento import Etapa
from qualidade import nulos_em, chaves_orfas, checar, avisar

BUCKET = "fiap-tc2-286958704145"
SILVER = f"s3a://{BUCKET}/silver"

REDE_MUNICIPAL = "3"


def integrar_municipio(spark):
    with Etapa("silver_integracao_municipio", camada="silver") as etapa:
        indicador = (
            spark.read.parquet(f"{SILVER}/municipio/")
            .filter(F.col("rede") == REDE_MUNICIPAL)
            .select(
                "id_municipio", "nome", "sigla_uf", "ano",
                "taxa_alfabetizacao", "media_portugues",
            )
        )

        meta = (
            spark.read.parquet(f"{SILVER}/meta_municipio/")
            .select(
                "id_municipio", "ano",
                "meta_alfabetizacao_2024", "meta_alfabetizacao_2026",
                "meta_alfabetizacao_2028", "meta_alfabetizacao_2030",
            )
        )

        checar("integracao_municipio", {
            "nulos na chave do indicador": nulos_em(indicador, ["id_municipio", "ano"]),
            "nulos na chave da meta": nulos_em(meta, ["id_municipio", "ano"]),
        })
        # Municipios com indicador mas sem meta projetada pelo INEP: mantidos (left join)
        avisar("integracao_municipio", {
            "municipios sem meta": chaves_orfas(indicador, meta, ["id_municipio", "ano"]),
        })

        df = indicador.join(meta, on=["id_municipio", "ano"], how="left")
        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER}/municipio_integrado/")


def integrar_uf(spark):
    with Etapa("silver_integracao_uf", camada="silver") as etapa:
        indicador = (
            spark.read.parquet(f"{SILVER}/uf/")
            .filter(F.col("rede") == REDE_MUNICIPAL)
            .select("sigla_uf", "ano", "taxa_alfabetizacao", "media_portugues")
        )

        meta = (
            spark.read.parquet(f"{SILVER}/meta_uf/")
            .select(
                "sigla_uf", "ano",
                "meta_alfabetizacao_2024", "meta_alfabetizacao_2026",
                "meta_alfabetizacao_2028", "meta_alfabetizacao_2030",
            )
        )

        checar("integracao_uf", {
            "nulos na chave do indicador": nulos_em(indicador, ["sigla_uf", "ano"]),
            "nulos na chave da meta": nulos_em(meta, ["sigla_uf", "ano"]),
        })
        avisar("integracao_uf", {
            "UFs sem meta": chaves_orfas(indicador, meta, ["sigla_uf", "ano"]),
        })

        df = indicador.join(meta, on=["sigla_uf", "ano"], how="left")
        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER}/uf_integrado/")


def main():
    spark = get_spark("silver-integracao")
    integrar_municipio(spark)
    integrar_uf(spark)
    print("Silver integracao concluida.")
    spark.stop()


if __name__ == "__main__":
    main()
