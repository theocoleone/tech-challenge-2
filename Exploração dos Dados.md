# Exploração dos Dados

Dataset: `basedosdados.br_inep_avaliacao_alfabetizacao`
Data: 2025-07-02



---

## Tabelas disponíveis


```sql
SELECT table_name
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.INFORMATION_SCHEMA.TABLES`;
```

| Tabela | Rows | Tamanho |
|--------|------|---------|
| alunos | 3.867.999 | 268 MB |
| municipio | 23.995 | 1.8 MB |
| meta_alfabetizacao_municipio | 10.704 | 1.1 MB |
| uf | 145 | 10 KB |
| meta_alfabetizacao_uf | 81 | 7 KB |
| meta_alfabetizacao_brasil | 3 | 270 B |
| dicionario | 27 | 1 KB |



```sql
SELECT table_id, row_count, size_bytes
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.__TABLES__`;
```

-> nao seria necessario usar usar PySpark

---

## Schemas

```sql
SELECT table_name, column_name, data_type
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.INFORMATION_SCHEMA.COLUMNS`
ORDER BY table_name, ordinal_position;
```

### alunos (microdados individuais)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| ano | INT64 | |
| id_municipio | STRING | Código IBGE |
| id_escola | STRING | |
| id_aluno | STRING | |
| caderno | STRING | |
| serie | STRING | 2 = 2 ano EF |
| rede | STRING | 1 a 6 (ver dicionário) |
| presenca | STRING | 0=Ausente, 1=Presente |
| preenchimento_caderno | STRING | 0=Não preenchida, 1=Preenchida |
| alfabetizado | STRING | 0=Não, 1=Sim |
| proficiencia | FLOAT64 | Score na escala SAEB |
| peso_aluno | FLOAT64 | Peso amostral |

### municipio (indicadores agregados)

| Coluna | Tipo |
|--------|------|
| ano | INT64 |
| id_municipio | STRING |
| serie | STRING |
| rede | STRING |
| taxa_alfabetizacao | FLOAT64 |
| media_portugues | FLOAT64 |
| proporcao_aluno_nivel_0 a _8 | FLOAT64 (9 colunas) |

### uf (indicadores agregados)

| Coluna | Tipo |
|--------|------|
| ano | INT64 |
| sigla_uf | STRING |
| serie | STRING |
| rede | STRING |
| taxa_alfabetizacao | FLOAT64 |
| media_portugues | FLOAT64 |
| proporcao_aluno_nivel_0 a _8 | FLOAT64 (9 colunas) |


### meta_alfabetizacao_brasil

| Coluna | Tipo |
|--------|------|
| ano | INT64 |
| rede | STRING |
| taxa_alfabetizacao | FLOAT64 |
| meta_alfabetizacao_2024 a _2030 | FLOAT64 (7 colunas) |
| percentual_participacao | FLOAT64 |

### meta_alfabetizacao_uf

| Coluna | Tipo |
|--------|------|
| ano | INT64 |
| sigla_uf | STRING |
| rede | STRING |
| taxa_alfabetizacao | FLOAT64 |
| meta_alfabetizacao_2024 a _2030 | FLOAT64 (7 colunas) |
| percentual_participacao | FLOAT64 |

### meta_alfabetizacao_municipio

| Coluna | Tipo |
|--------|------|
| ano | INT64 |
| id_municipio | STRING |
| rede | STRING |
| taxa_alfabetizacao | FLOAT64 |
| meta_alfabetizacao_2024 a _2030 | FLOAT64 (7 colunas) |
| nivel_alfabetizacao | INT64 |
| percentual_participacao | FLOAT64 |

---

## Dicionário de valores codificados

```sql
SELECT *
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`;
```

**rede:**
- 0 = Total (Federal, Estadual, Municipal e Privada)
- 1 = Federal
- 2 = Estadual
- 3 = Municipal
- 4 = Privada
- 5 = Pública (Estadual e Municipal)
- 6 = Pública (Federal, Estadual e Municipal)

**serie:**
- 2 = 2 ano do Ensino Fundamental

**presenca (alunos):**
- 0 = Ausente
- 1 = Presente

**preenchimento_caderno (alunos):**
- 0 = Prova não preenchida
- 1 = Prova preenchida

**alfabetizado (alunos):**
- 0 = Não
- 1 = Sim


---

## Mapa de joins

```
alunos (id_municipio + ano) --> municipio
municipio (id_municipio + ano + rede) --> meta_alfabetizacao_municipio
uf (sigla_uf + ano + rede) --> meta_alfabetizacao_uf
meta_alfabetizacao_brasil --> referência nacional (ano + rede)
```

OBS: `municipio` não tem `sigla_uf`

- A: derivar a UF dos 2 primeiros dígitos do `id_municipio` (o código IBGE carrega o estado no prefixo)
- B: achar uma tabela auxiliar que fizesse esse de-para

---

## Validação dos joins

### Link município com UF

diretório de municípios do IBGE na Base dos Dados tem  `id_municipio`, `nome` e `sigla_uf` :

```sql
SELECT id_municipio, nome, sigla_uf
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
LIMIT 20;
```

### Anos disponíveis por tabela

```sql
SELECT 'alunos' as tabela, ARRAY_AGG(DISTINCT ano ORDER BY ano) as anos
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`
UNION ALL
SELECT 'municipio', ARRAY_AGG(DISTINCT ano ORDER BY ano)
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`
UNION ALL
SELECT 'uf', ARRAY_AGG(DISTINCT ano ORDER BY ano)
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
UNION ALL
SELECT 'meta_brasil', ARRAY_AGG(DISTINCT ano ORDER BY ano)
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`
UNION ALL
SELECT 'meta_uf', ARRAY_AGG(DISTINCT ano ORDER BY ano)
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`
UNION ALL
SELECT 'meta_municipio', ARRAY_AGG(DISTINCT ano ORDER BY ano)
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`;
```

| Tabela | Anos |
|--------|------|
| alunos | 2023, 2024 |
| municipio | 2023, 2024 |
| uf | 2023, 2024 |
| meta_brasil | 2023, 2024, 2025 |
| meta_uf | 2023, 2024, 2025 |
| meta_municipio | 2023, 2024 |

OBS: metas vão até 2030 no schema (colunas `meta_alfabetizacao_2026` a `_2030`), mas só têm valor preenchido até 2025. A parte de "evolução temporal" da Gold so tera dois pontos no tempo.

### Nulos nas chaves de join

```sql
SELECT
  COUNT(*) as total,
  COUNTIF(id_municipio IS NULL) as id_municipio_null,
  COUNTIF(ano IS NULL) as ano_null
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`;
```

---

## Insights

1. **Dados pequenos.** Fora a `alunos` (3,8M linhas, 268 MB), tudo é menor que 2 MB. Spark é mais pra mostrar que sei fazer em escala do que por precisão; pandas daria conta.
2. **Os joins fecham.** Chaves consistentes (`id_municipio`, `sigla_uf`, `ano`, `rede`) e zero nulo em PK.
3. **O diretório resolve o gap município/UF.** ingerir a `br_bd_diretorios_brasil.municipio` junto, como fonte auxiliar.
4. **Só dois anos de dado real.** Evolução temporal limitada a 2023 e 2024.
5. **`rede` é a dimensão que atravessa tudo.** Todas as agregadas têm breakdown por rede, então os joins precisam de `ano + chave_geo + rede`.
6. **Alunos é grão individual.** Tem `id_aluno`, o que me deixa recalcular a taxa por conta própria e comparar com o número oficial. 
7. **Fontes pra Bronze:** as seis de alfabetização (`alunos`, `municipio`, `uf`, `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`) mais o diretório `br_bd_diretorios_brasil.municipio`.

---

## Building Silver


### `rede` vem codificada numas tabelas e por extenso em outras


### Proficiência nula em quem não fez a prova

Achei 513.338 registros (13,3% dos 3,87M) com `proficiencia` nula na `alunos`. 

- 100% deles têm `preenchimento_caderno = "Prova nao preenchida"`
- 512.153 estavam ausentes; 1.185 estavam presentes mas não preencheram

Ou seja, não é sujeira. É o esperado pra quem não fez a avaliação, e esses alunos aparecem com `alfabetizado = "Nao"`.

Na Gold: quando recalcular a taxa com o corte de 743, qual denominador uso? 

### A `serie` só tem um valor (2º ano do EF) - recorte da pesquisa


### 242 municípios com indicador mas sem meta 

- os anos batem (indicador e meta cobrem 2023 e 2024) e as chaves são consistentes
- 10.896 município-anos com indicador contra 10.704 com meta
- `meta_municipio` tem exatamente uma linha por `(id_municipio, ano)`, toda "Municipal", então o left join é 1:1 e não multiplica as linhas do indicador 

### Faltam metas?



| Grupo | Participação mediana | Participação (p05 a p95) |
|-------|---------------------|--------------------------|
| Com meta | 91,2% | 77,6% a 100% |
| Sem meta (órfão) | 71,1% | 50,0% a 96,8% |

taxa de "orfandade" por faixa de participação:

| Participação | Municípios | Órfãos | % órfão |
|--------------|-----------|--------|---------|
| < 60% | 56 | 39 | 69,6% |
| 60 a 70% | 129 | 78 | 60,5% |
| 70 a 80% | 760 | 42 | 5,5% |
| 80 a 90% | 3.513 | 48 | 1,4% |
| >= 90% | 5.811 | 32 | 0,6% |

-> só projeta meta quando a medição é representativa

-> baixa participação anda junto com ausência de meta. Não que uma cause a outra, e nem que seja o único motivo. (OBS: não confundir correlação com causa.)

-> a falta de meta não é aleatória, se concentra onde a avaliação teve baixa cobertura. Isso é relevante pra política pública. Municípios sem meta são justamente os de menor participação, que também são os que mais precisam de atenção. -> Não dá pra acompanhar o que não se mede.

O código dessa análise está em `analises/analise_metas_ausentes.py`.

---