"""Mapeamentos do dicionario oficial e helper para decodificar codigos."""

from pyspark.sql import functions as F

# Codigos sao STRING na fonte, entao as chaves aqui tambem sao strings
REDE = {
    "0": "Total (Federal, Estadual, Municipal e Privada)",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Publica (Estadual e Municipal)",
    "6": "Publica (Federal, Estadual e Municipal)",
}

SERIE = {
    "2": "2o ano do Ensino Fundamental",
}

# Colunas exclusivas dos microdados de alunos
PRESENCA = {
    "0": "Ausente",
    "1": "Presente",
}

PREENCHIMENTO_CADERNO = {
    "0": "Prova nao preenchida",
    "1": "Prova preenchida",
}

ALFABETIZADO = {
    "0": "Nao",
    "1": "Sim",
}


def decode(source_col, mapping, default=None):
    """Expressao de coluna que traduz codigos em descricoes.
    Uso: df.withColumn("rede_desc", decode("rede", REDE))
    """
    expr = F.lit(default)
    for code, description in mapping.items():
        expr = F.when(F.col(source_col) == code, F.lit(description)).otherwise(expr)
    return expr
