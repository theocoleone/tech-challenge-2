"""Silver dos microdados de alunos: limpa, decodifica e enriquece com UF."""

import sys

sys.path.append("src")
from spark_session import get_spark
from dicionario import REDE, SERIE, PRESENCA, PREENCHIMENTO_CADERNO, ALFABETIZADO, decode
from monitoramento import Etapa
from qualidade import nulos_em, duplicados_em, fora_do_conjunto, chaves_orfas, checar

BUCKET = "fiap-tc2-286958704145"
BRONZE = f"s3a://{BUCKET}/bronze/alunos/"
BRONZE_DIRETORIO = f"s3a://{BUCKET}/bronze/diretorio_municipio/"
SILVER = f"s3a://{BUCKET}/silver/alunos/"

CORTE_ALFABETIZACAO = 743


def main():
    spark = get_spark("silver-alunos")

    with Etapa("silver_alunos", camada="silver") as etapa:
        df = spark.read.parquet(BRONZE)

        diretorio = (
            spark.read.parquet(BRONZE_DIRETORIO)
            .select("id_municipio", "nome", "sigla_uf")
        )

        df = df.dropDuplicates()

        df = df.withColumn("rede_desc", decode("rede", REDE))
        df = df.withColumn("serie_desc", decode("serie", SERIE))
        df = df.withColumn("presenca_desc", decode("presenca", PRESENCA))
        df = df.withColumn("preenchimento_caderno_desc", decode("preenchimento_caderno", PREENCHIMENTO_CADERNO))
        df = df.withColumn("alfabetizado_desc", decode("alfabetizado", ALFABETIZADO))

        df = df.join(diretorio, on="id_municipio", how="left")

        checar("silver_alunos", {
            "nulos em chave": nulos_em(df, ["ano", "id_municipio", "id_aluno"]),
            "nulos em campos obrigatorios": nulos_em(
                df, ["serie", "rede", "presenca", "preenchimento_caderno", "alfabetizado"]
            ),
            "duplicidade de chave": duplicados_em(df, ["ano", "id_aluno"]),
            "alfabetizado invalido": fora_do_conjunto(df, "alfabetizado", ["0", "1"]),
            "municipios orfaos do diretorio": chaves_orfas(df, diretorio, "id_municipio"),
        })

        etapa.linhas = df.count()
        df.write.mode("overwrite").partitionBy("ano").parquet(SILVER)

    spark.stop()


if __name__ == "__main__":
    main()
