"""Validacoes de qualidade reutilizaveis pelas camadas Silver."""

from pyspark.sql import functions as F


def nulos_em(df, colunas):
    condicao = None
    for c in colunas:
        cond_c = F.col(c).isNull()
        condicao = cond_c if condicao is None else (condicao | cond_c)
    return df.filter(condicao).count() if condicao is not None else 0


def duplicados_em(df, colunas):
    if isinstance(colunas, str):
        colunas = [colunas]
    return df.count() - df.dropDuplicates(colunas).count()


def chaves_orfas(df, df_referencia, colunas):
    # Linhas de df cuja chave nao existe na tabela de referencia
    if isinstance(colunas, str):
        colunas = [colunas]
    ref = df_referencia.select(*colunas).distinct()
    return df.join(ref, on=colunas, how="left_anti").count()


def fora_da_faixa(df, coluna, minimo, maximo):
    return df.filter((F.col(coluna) < minimo) | (F.col(coluna) > maximo)).count()


def fora_do_conjunto(df, coluna, permitidos):
    return df.filter(~F.col(coluna).isin(*permitidos)).count()


def checar(nome, regras):
    resumo = " | ".join(f"{k}: {v}" for k, v in regras.items())
    print(f"  Qualidade [{nome}] {resumo}")
    falhas = {k: v for k, v in regras.items() if v > 0}
    if falhas:
        raise ValueError(f"Falha de qualidade em {nome}: {falhas}")


def avisar(nome, regras):
    # Regras informativas: registram violacoes sem interromper (dado esperado)
    resumo = " | ".join(f"{k}: {v}" for k, v in regras.items())
    print(f"  Aviso [{nome}] {resumo}")
