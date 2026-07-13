"""Gera um dashboard HTML estatico a partir dos datasets Gold no S3."""

import io
import os
import re

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

BUCKET = "fiap-tc2-286958704145"
PROFILE = "fiap-tech-challenge"
GOLD_PREFIX = "gold"
OUTPUT = os.path.join(os.path.dirname(__file__), "index.html")

AZUL = "#2b6cb0"
CINZA = "#cbd5e0"
PALETA = px.colors.qualitative.Safe


def br(n, casas=2):
    return f"{n:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def read_gold(s3, dataset):
    prefix = f"{GOLD_PREFIX}/{dataset}/"
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            df = pd.read_parquet(io.BytesIO(body))
            # colunas de particao (ano=2024/) ficam no caminho, nao no arquivo
            for coluna, valor in re.findall(r"/(\w+)=([^/]+)/", key):
                if coluna not in df.columns:
                    df[coluna] = int(valor) if valor.isdigit() else valor
            frames.append(df)
    if not frames:
        raise FileNotFoundError(f"Nenhum parquet em s3://{BUCKET}/{prefix}")
    return pd.concat(frames, ignore_index=True)


def estilizar(fig, altura):
    fig.update_layout(
        template="plotly_white",
        height=altura,
        font=dict(family="Inter, -apple-system, Segoe UI, Roboto, sans-serif", size=13, color="#2d3748"),
        margin=dict(l=70, r=40, t=20, b=45),
        paper_bgcolor="white",
        plot_bgcolor="white",
        colorway=PALETA,
        legend=dict(bgcolor="rgba(255,255,255,0.6)"),
    )
    return fig


def fig_ranking_uf(metas_uf, ano):
    df = metas_uf[metas_uf["ano"] == ano].sort_values("taxa_alfabetizacao")
    fig = px.bar(
        df, x="taxa_alfabetizacao", y="sigla_uf", orientation="h",
        labels={"taxa_alfabetizacao": "Taxa de alfabetização (%)", "sigla_uf": ""},
        color="atingimento_meta_2030_pct",
        color_continuous_scale="RdYlGn",
        text="taxa_alfabetizacao",
    )
    fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
    fig.update_coloraxes(colorbar_title="Atingimento<br>da meta (%)")
    return estilizar(fig, 720)


def fig_meta_vs_realizado(metas_uf, ano):
    df = metas_uf[metas_uf["ano"] == ano].sort_values("taxa_alfabetizacao", ascending=False)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Resultado 2024", x=df["sigla_uf"], y=df["taxa_alfabetizacao"], marker_color=AZUL,
    ))
    fig.add_trace(go.Bar(
        name="Meta 2030", x=df["sigla_uf"], y=df["meta_alfabetizacao_2030"], marker_color=CINZA,
    ))
    fig.update_layout(barmode="group", yaxis_title="Taxa de alfabetização (%)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    return estilizar(fig, 480)


def fig_evolucao(evolucao):
    fig = px.line(
        evolucao.sort_values(["sigla_uf", "ano"]),
        x="ano", y="taxa_alfabetizacao", color="sigla_uf", markers=True,
        labels={"taxa_alfabetizacao": "Taxa de alfabetização (%)", "ano": "Ano", "sigla_uf": "UF"},
    )
    fig.update_traces(line=dict(width=1.5))
    fig.update_layout(xaxis=dict(tickvals=sorted(evolucao["ano"].unique())))
    return estilizar(fig, 520)


def fig_top_municipios(indicador_mun, ano, n=20):
    df = (
        indicador_mun[indicador_mun["ano"] == ano]
        .dropna(subset=["gap_para_meta_2030"])
        .nlargest(n, "gap_para_meta_2030")
    )
    fig = px.bar(
        df, x="gap_para_meta_2030", y="nome", orientation="h",
        labels={"gap_para_meta_2030": "Distância da meta (pontos)", "nome": ""},
        color="sigla_uf",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, legend_title="Estado")
    return estilizar(fig, 620)


def bloco_kpis(metas_uf, evolucao, indicador_mun, ano):
    muf = metas_uf[metas_uf["ano"] == ano]
    melhor = muf.loc[muf["taxa_alfabetizacao"].idxmax()]
    n_uf = muf["sigla_uf"].nunique()
    atingiram = int((muf["atingimento_meta_2030_pct"] >= 100).sum())
    ev = evolucao.dropna(subset=["variacao_pp"])
    maior = ev.loc[ev["variacao_pp"].idxmax()]
    n_mun = indicador_mun[indicador_mun["ano"] == ano]["id_municipio"].nunique()

    cartoes = [
        (f"{atingiram} de {n_uf}", f"estados com dado municipal atingiram a meta de 2030 (em {ano})", "#38a169"),
        (f"{melhor['sigla_uf']} · {br(melhor['taxa_alfabetizacao'])}%", "maior taxa entre os estados", "#2b6cb0"),
        (f"{maior['sigla_uf']} · +{br(maior['variacao_pp'])} p.p.", "maior evolução de 2023 para 2024", "#dd6b20"),
        (br(n_mun, 0), f"municípios analisados em {ano}", "#805ad5"),
    ]
    itens = "".join(
        f'<div class="kpi" style="border-left-color:{cor}">'
        f'<div class="kpi-valor">{valor}</div><div class="kpi-rotulo">{rotulo}</div></div>'
        for valor, rotulo, cor in cartoes
    )
    return f'<div class="kpis">{itens}</div>'


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #eef2f6; color: #2d3748; line-height: 1.5; padding: 36px 16px; }
.container { max-width: 1080px; margin: 0 auto; }
header.topo { text-align: center; margin-bottom: 32px; }
header.topo h1 { font-size: 28px; color: #1a365d; margin-bottom: 8px; }
header.topo p { color: #5a6a7d; font-size: 15px; max-width: 700px; margin: 0 auto; }
.kpis { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 28px; }
.kpi { flex: 1 1 210px; background: #fff; border-radius: 12px; padding: 20px 22px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-left: 4px solid #2b6cb0; }
.kpi-valor { font-size: 24px; font-weight: 700; color: #1a365d; }
.kpi-rotulo { font-size: 13px; color: #5a6a7d; margin-top: 6px; }
.card { background: #fff; border-radius: 12px; padding: 24px 26px; margin-bottom: 22px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.card h2 { font-size: 19px; color: #1a365d; margin-bottom: 4px; }
.card .legenda { font-size: 14px; color: #5a6a7d; margin-bottom: 14px; }
footer { text-align: center; color: #8a97a8; font-size: 13px; margin-top: 28px; }
footer .autoria { font-weight: 600; color: #5a6a7d; margin-bottom: 4px; }
"""


def main():
    s3 = boto3.Session(profile_name=PROFILE).client("s3")

    metas_uf = read_gold(s3, "metas_vs_resultados_uf")
    evolucao = read_gold(s3, "evolucao_uf")
    indicador_mun = read_gold(s3, "indicador_municipio")
    ano = int(metas_uf["ano"].max())
    ano_min = int(evolucao["ano"].min())

    secoes = [
        ("Taxa de alfabetização por estado",
         "Rede municipal, {ano}. A cor mostra o quanto cada estado já alcançou da meta projetada para 2030.".format(ano=ano),
         fig_ranking_uf(metas_uf, ano)),
        ("Meta 2030 versus resultado alcançado",
         "Comparação entre a taxa alcançada e a meta projetada para 2030, por estado.",
         fig_meta_vs_realizado(metas_uf, ano)),
        ("Evolução da taxa por estado",
         "Variação da taxa entre 2023 e 2024. Clique na legenda para isolar estados.",
         fig_evolucao(evolucao)),
        ("Municípios mais distantes da meta",
         "Os 20 municípios com maior distância entre a meta de 2030 e a taxa alcançada.",
         fig_top_municipios(indicador_mun, ano)),
    ]

    partes = [
        "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Alfabetização no Brasil - Rede Municipal</title>",
        "<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap' rel='stylesheet'>",
        f"<style>{CSS}</style></head><body><div class='container'>",
        f"<header class='topo'><h1>Alfabetização no Brasil - Rede Municipal ({ano})</h1>",
        "<p>Acompanhamento do Indicador Criança Alfabetizada (2º ano do Ensino Fundamental) "
        "frente às metas do Compromisso Nacional Criança Alfabetizada. "
        f"Dados de {ano_min} e {ano} (indicadores) e metas projetadas até 2030.</p></header>",
        bloco_kpis(metas_uf, evolucao, indicador_mun, ano),
    ]

    for i, (titulo, legenda, fig) in enumerate(secoes):
        grafico = fig.to_html(full_html=False, include_plotlyjs="cdn" if i == 0 else False)
        partes.append(
            f"<section class='card'><h2>{titulo}</h2>"
            f"<p class='legenda'>{legenda}</p>{grafico}</section>"
        )

    partes.append(
        "<footer>"
        "<div class='autoria'>Theo Coleone · Tech Challenge Fase 2 · FIAP Pós-Graduação em AI Scientist</div>"
        "Fonte: INEP / Saeb, via Base dos Dados. "
        "Gerado a partir da camada Gold da pipeline de dados."
        "</footer>"
    )
    partes.append("</div></body></html>")

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(partes))

    print(f"Dashboard gerado em {OUTPUT}")


if __name__ == "__main__":
    main()
