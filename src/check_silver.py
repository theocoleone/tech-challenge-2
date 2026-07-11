"""Verificacao rapida de uma tabela da Silver. Uso: python src/check_silver.py <tabela>"""

import sys

sys.path.append("src")
from spark_session import get_spark

BUCKET = "fiap-tc2-286958704145"

tabela = sys.argv[1] if len(sys.argv) > 1 else "municipio"
spark = get_spark("check-silver")

df = spark.read.parquet(f"s3a://{BUCKET}/silver/{tabela}/")
print(f"\n=== silver/{tabela} ===")
print(f"Total de linhas: {df.count()}")
if "sigla_uf" in df.columns:
    print(f"Linhas sem UF: {df.filter(df.sigla_uf.isNull()).count()}")

cols = [c for c in ["id_municipio", "nome", "sigla_uf", "rede", "rede_desc", "taxa_alfabetizacao"] if c in df.columns]
df.select(*cols).show(5, truncate=False)

spark.stop()
