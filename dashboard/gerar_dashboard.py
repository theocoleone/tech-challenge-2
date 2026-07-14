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

# Paleta editorial "papel e tinta" (mesma da apresentacao)
PAPEL = "#f2ebdb"
PAPEL_CARD = "#faf5e9"
TINTA = "#1c1b2a"
TINTA_SUAVE = "#565266"
FERRUGEM = "#b8431d"
OCRE = "#c4881a"
FLORESTA = "#3a7048"
TEAL = "#2c6864"
FIO = "#cbbfa6"
# sequencia categorica de tons terrosos/editoriais
PALETA = ["#b8431d", "#c4881a", "#3a7048", "#2c6864", "#9c2f18", "#a3652f", "#6a6b7a", "#bd8a1f"]


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
        font=dict(family="Archivo, -apple-system, Segoe UI, sans-serif", size=13, color=TINTA),
        margin=dict(l=70, r=40, t=20, b=45),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=PALETA,
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor=FIO, zerolinecolor=FIO, linecolor=FIO)
    fig.update_yaxes(gridcolor=FIO, zerolinecolor=FIO, linecolor=FIO)
    return fig


def fig_ranking_uf(metas_uf, ano):
    df = metas_uf[metas_uf["ano"] == ano].sort_values("taxa_alfabetizacao")
    fig = px.bar(
        df, x="taxa_alfabetizacao", y="sigla_uf", orientation="h",
        labels={"taxa_alfabetizacao": "Taxa de alfabetização (%)", "sigla_uf": ""},
        color="atingimento_meta_2030_pct",
        color_continuous_scale=[[0, "#9c2f18"], [0.5, "#c4881a"], [1, "#3a7048"]],
        text="taxa_alfabetizacao",
    )
    fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
    fig.update_coloraxes(colorbar_title="Atingimento<br>da meta (%)")
    return estilizar(fig, 720)


def fig_meta_vs_realizado(metas_uf, ano):
    df = metas_uf[metas_uf["ano"] == ano].sort_values("taxa_alfabetizacao", ascending=False)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Resultado 2024", x=df["sigla_uf"], y=df["taxa_alfabetizacao"], marker_color=FERRUGEM,
    ))
    fig.add_trace(go.Bar(
        name="Meta 2030", x=df["sigla_uf"], y=df["meta_alfabetizacao_2030"], marker_color=FIO,
    ))
    fig.update_layout(barmode="group", yaxis_title="Taxa de alfabetização (%)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    return estilizar(fig, 480)


def fig_evolucao(evolucao):
    df = evolucao.sort_values(["sigla_uf", "ano"]).copy()
    # colore cada estado conforme subiu ou caiu no periodo (sinal da variacao)
    var = df.dropna(subset=["variacao_pp"]).groupby("sigla_uf")["variacao_pp"].last()
    df["tendencia"] = df["sigla_uf"].map(lambda uf: "Subiu" if var.get(uf, 0) >= 0 else "Caiu")
    fig = px.line(
        df, x="ano", y="taxa_alfabetizacao", line_group="sigla_uf", color="tendencia",
        markers=True, hover_data={"sigla_uf": True, "tendencia": False},
        color_discrete_map={"Subiu": FLORESTA, "Caiu": FERRUGEM},
        labels={"taxa_alfabetizacao": "Taxa de alfabetização (%)", "ano": "Ano", "tendencia": "Tendência"},
    )
    fig.update_traces(line=dict(width=1.5), opacity=0.75)
    fig.update_layout(xaxis=dict(tickvals=sorted(df["ano"].unique())))
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
        color="sigla_uf", color_discrete_sequence=PALETA,
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
        (f"{atingiram} de {n_uf}", f"estados com dado municipal atingiram a meta de 2030 (em {ano})", FLORESTA),
        (f"{melhor['sigla_uf']} · {br(melhor['taxa_alfabetizacao'])}%", "maior taxa entre os estados", FERRUGEM),
        (f"{maior['sigla_uf']} · +{br(maior['variacao_pp'])} p.p.", "maior evolução de 2023 para 2024", OCRE),
        (br(n_mun, 0), f"municípios analisados em {ano}", TEAL),
    ]
    itens = "".join(
        f'<div class="kpi" style="border-left-color:{cor}">'
        f'<div class="kpi-valor">{valor}</div><div class="kpi-rotulo">{rotulo}</div></div>'
        for valor, rotulo, cor in cartoes
    )
    return f'<div class="kpis">{itens}</div>'


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Archivo', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #f2ebdb; color: #1c1b2a; line-height: 1.5; padding: 0;
  -webkit-font-smoothing: antialiased;
}
body::after {
  content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 100; opacity: 0.5;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.05'/%3E%3C/svg%3E");
  mix-blend-mode: multiply;
}
.container { max-width: 1120px; margin: 0 auto; padding: 3.5rem 2rem 2rem; }

/* masthead editorial */
.masthead {
  display: flex; justify-content: space-between; align-items: center;
  font-size: 0.78rem; font-weight: 600; letter-spacing: 0.22em; text-transform: uppercase;
  color: #565266; padding-bottom: 0.9rem; border-bottom: 1px solid #cbbfa6; margin-bottom: 2.6rem;
}
header.topo { margin-bottom: 2.8rem; text-align: center; }
header.topo .kicker {
  font-size: 0.9rem; font-weight: 700; letter-spacing: 0.26em; text-transform: uppercase;
  color: #b8431d; display: flex; align-items: center; justify-content: center;
  gap: 0.8rem; margin-bottom: 1.1rem;
}
header.topo .kicker::before, header.topo .kicker::after { content: ""; width: 2.2rem; height: 2px; background: #b8431d; }
header.topo h1 {
  font-family: 'Fraunces', Georgia, serif; font-weight: 900; font-size: 3.4rem;
  line-height: 1.05; letter-spacing: -0.02em; color: #1c1b2a; margin-bottom: 1rem;
}
header.topo h1 .sub-titulo {
  display: block; font-size: 2.3rem; font-weight: 700; color: #565266; margin-top: 0.2rem;
}
header.topo p {
  font-family: 'Fraunces', Georgia, serif; font-size: 1.3rem; font-weight: 400;
  color: #1c1b2a; max-width: 68ch; line-height: 1.5; margin: 0 auto;
}

/* KPIs */
.kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0;
  border-top: 1px solid #cbbfa6; border-bottom: 1px solid #cbbfa6; margin-bottom: 3rem; }
.kpi { padding: 1.7rem 1.6rem; border-left: 3px solid #b8431d; }
.kpi + .kpi { border-top: none; }
.kpi:not(:last-child) { border-right: 1px solid #cbbfa6; }
.kpi-valor { font-family: 'Fraunces', Georgia, serif; font-weight: 900; font-size: 1.9rem;
  color: #1c1b2a; line-height: 1; letter-spacing: -0.01em; }
.kpi-rotulo { font-size: 0.9rem; color: #565266; margin-top: 0.7rem; line-height: 1.35; }

/* cards de grafico */
.card { background: #faf5e9; border: 1px solid #cbbfa6; border-radius: 8px;
  padding: 1.8rem 2rem; margin-bottom: 1.8rem; box-shadow: 0 6px 22px rgba(28,27,42,0.06); }
.card h2 { font-family: 'Fraunces', Georgia, serif; font-weight: 700; font-size: 1.7rem;
  color: #1c1b2a; margin-bottom: 0.3rem; letter-spacing: -0.01em; }
.card .legenda { font-size: 0.98rem; color: #565266; margin-bottom: 1.2rem; }

footer { text-align: center; color: #8a8577; font-size: 0.9rem; margin-top: 2.5rem;
  padding-top: 1.4rem; border-top: 1px solid #cbbfa6; }
footer .autoria { font-family: 'Fraunces', Georgia, serif; font-weight: 600; font-style: italic;
  font-size: 1.05rem; color: #1c1b2a; margin-bottom: 0.4rem; }
@media (max-width: 820px) { .kpis { grid-template-columns: 1fr 1fr; } header.topo h1 { font-size: 2.4rem; } }
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
         "Variação da taxa entre 2023 e 2024. Em verde os estados que subiram, em vermelho os que caíram. Passe o mouse para ver cada estado.",
         fig_evolucao(evolucao)),
        ("Municípios mais distantes da meta",
         "Os 20 municípios com maior distância entre a meta de 2030 e a taxa alcançada.",
         fig_top_municipios(indicador_mun, ano)),
    ]

    partes = [
        "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Alfabetização no Brasil: Rede Municipal</title>",
        "<link rel='preconnect' href='https://fonts.googleapis.com'>",
        "<link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>",
        "<link href='https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,400;0,9..144,600;0,9..144,700;0,9..144,900;1,9..144,500&family=Archivo:wght@400;500;600;700&display=swap' rel='stylesheet'>",
        f"<style>{CSS}</style></head><body><div class='container'>",
        "<div class='masthead'><span>Tech Challenge · Fase 2</span><span>FIAP · AI Scientist</span></div>",
        "<header class='topo'>",
        "<div class='kicker'>Painel de Indicadores</div>",
        f"<h1>Alfabetização no Brasil<span class='sub-titulo'>Rede Municipal ({ano})</span></h1>",
        "<p>Acompanhamento do Indicador Criança Alfabetizada (2º ano do Ensino Fundamental) "
        "frente às metas do Compromisso Nacional Criança Alfabetizada. "
        f"Dados de {ano_min} e {ano}, com metas projetadas até 2030.</p></header>",
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
