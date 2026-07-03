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

Volumes obtidos com:

```sql
SELECT table_id, row_count, size_bytes
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.__TABLES__`;
```

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

Problema identificado: a tabela `municipio` não tem `sigla_uf`. Para conectar município com UF temos duas opções:
- Opção A: derivar dos 2 primeiros dígitos do `id_municipio` (código IBGE)
- Opção B: buscar tabela auxiliar `basedosdados.br_bd_diretorios_brasil.municipio`

---

## Validação dos joins

### Link município com UF

```sql
SELECT id_municipio, nome, sigla_uf
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
LIMIT 20;
```

Resultado: tabela auxiliar existe e funciona. Contém `id_municipio`, `nome` e `sigla_uf`. Podemos usar essa tabela para enriquecer `municipio` e `alunos` com a sigla da UF.

Exemplo: `5101837 = Boa Esperança do Norte, MT` | `1100205 = Porto Velho, RO`


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

Resultado:

| Tabela | Anos |
|--------|------|
| alunos | 2023, 2024 |
| municipio | 2023, 2024 |
| uf | 2023, 2024 |
| meta_brasil | 2023, 2024, 2025 |
| meta_uf | 2023, 2024, 2025 |
| meta_municipio | 2023, 2024 |

Observação: dataset recente (só 2023 e 2024 para dados reais, metas projetadas até 2025). As colunas `meta_alfabetizacao_2026` a `_2030` existem no schema mas os dados só vão até 2025.


### Nulos nas chaves de join

```sql
SELECT
  COUNT(*) as total,
  COUNTIF(id_municipio IS NULL) as id_municipio_null,
  COUNTIF(ano IS NULL) as ano_null
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`;
```

Resultado: zero nulos. 23.995 registros, todos com `id_municipio` e `ano` preenchidos.

---

## Conclusões da exploração

1. **Dados pequenos.** Exceto `alunos` (3.8M rows, 268MB), tudo é menor que 2MB. PySpark é justificável para demonstrar escalabilidade, mas pandas daria conta.

2. **Joins são viáveis.** Chaves consistentes (`id_municipio`, `sigla_uf`, `ano`, `rede`), zero nulos em PKs.

3. **Tabela auxiliar resolve o gap município/UF.** A tabela `basedosdados.br_bd_diretorios_brasil.municipio` fornece `sigla_uf` e `nome` do município. Deve ser ingerida junto como fonte auxiliar.

4. **Período curto (2023 e 2024).** Não há série histórica longa. A evolução temporal será limitada a 2 anos. Metas projetam até 2030 (colunas no schema), mas dados reais só existem para 2023 e 2024.

5. **Campo `rede` é a dimensão principal.** Todas as tabelas agregadas (municipio, uf, metas) têm breakdown por rede. Os joins devem considerar `ano + chave_geo + rede`.

6. **Microdados de alunos são individuais (id_aluno).** Isso permite agregação custom na Silver/Gold. Exemplo: calcular taxa de alfabetização por escola e comparar com o agregado oficial.

7. **Fontes para ingestão Bronze:**
   - `br_inep_avaliacao_alfabetizacao.alunos`
   - `br_inep_avaliacao_alfabetizacao.municipio`
   - `br_inep_avaliacao_alfabetizacao.uf`
   - `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`
   - `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`
   - `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`
   - `br_bd_diretorios_brasil.municipio` (auxiliar para enriquecer com nome e UF)

---

## Conexões

- [[Timeline - Tech Challenge 2]]
