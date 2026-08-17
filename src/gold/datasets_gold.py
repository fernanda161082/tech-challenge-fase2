"""
Camada Gold - datasets analiticos.

Constroi, com SQL sobre a camada Silver, os quatro conjuntos de dados
prontos para consumo por dashboards, analise estatistica e modelos.

Datasets produzidos:
  1. indicador_municipio  - indicador de alfabetizacao por municipio
  2. indicador_uf         - indicador agregado por unidade da federacao
  3. meta_vs_resultado    - comparacao entre meta e resultado observado
  4. evolucao_temporal    - variacao do indicador entre 2023 e 2024

Por que SQL nesta camada:
  a Gold e feita de agregacoes e juncoes, que SQL expressa de forma mais
  legivel do que codigo imperativo. Alem disso, e a linguagem que qualquer
  analista consegue ler sem saber Python.

Por que DuckDB:
  e um banco analitico que roda dentro do proprio processo, sem servidor,
  e le Parquet diretamente do disco. Alternativas descartadas: PostgreSQL
  (exige servidor) e SQLite (nao e orientado a analise).

A unidade da federacao e derivada dos dois primeiros digitos do codigo
IBGE do municipio, o que dispensa uma tabela de dimensao externa.

Execucao:
    python src/gold/datasets_gold.py
"""

from pathlib import Path
import shutil

import duckdb

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_SILVER = RAIZ_PROJETO / "data" / "silver"
PASTA_GOLD = RAIZ_PROJETO / "data" / "gold"

# Ponto de corte nacional da escala Saeb a partir do qual uma crianca
# e considerada alfabetizada (Pesquisa Alfabetiza Brasil, 2023).
PONTO_CORTE = 743.0

REDE_MUNICIPAL = 3
REDE_PUBLICA = 5

# Dimensao territorial derivada do codigo IBGE.
# Os dois primeiros digitos do id_municipio identificam a UF.
DIMENSAO_UF = """
    SELECT * FROM (VALUES
        ('11','RO','Rondonia','Norte'),
        ('12','AC','Acre','Norte'),
        ('13','AM','Amazonas','Norte'),
        ('14','RR','Roraima','Norte'),
        ('15','PA','Para','Norte'),
        ('16','AP','Amapa','Norte'),
        ('17','TO','Tocantins','Norte'),
        ('21','MA','Maranhao','Nordeste'),
        ('22','PI','Piaui','Nordeste'),
        ('23','CE','Ceara','Nordeste'),
        ('24','RN','Rio Grande do Norte','Nordeste'),
        ('25','PB','Paraiba','Nordeste'),
        ('26','PE','Pernambuco','Nordeste'),
        ('27','AL','Alagoas','Nordeste'),
        ('28','SE','Sergipe','Nordeste'),
        ('29','BA','Bahia','Nordeste'),
        ('31','MG','Minas Gerais','Sudeste'),
        ('32','ES','Espirito Santo','Sudeste'),
        ('33','RJ','Rio de Janeiro','Sudeste'),
        ('35','SP','Sao Paulo','Sudeste'),
        ('41','PR','Parana','Sul'),
        ('42','SC','Santa Catarina','Sul'),
        ('43','RS','Rio Grande do Sul','Sul'),
        ('50','MS','Mato Grosso do Sul','Centro-Oeste'),
        ('51','MT','Mato Grosso','Centro-Oeste'),
        ('52','GO','Goias','Centro-Oeste'),
        ('53','DF','Distrito Federal','Centro-Oeste')
    ) AS t(codigo_uf, sigla_uf, nome_uf, regiao)
"""


def caminho_silver(tabela: str) -> str:
    """Devolve o caminho do Parquet de uma tabela Silver, em formato SQL."""
    return str((PASTA_SILVER / tabela / "dados.parquet").as_posix())


def preparar_conexao() -> duckdb.DuckDBPyConnection:
    """Abre a conexao e registra as tabelas Silver como views."""
    con = duckdb.connect()

    con.execute(f"CREATE OR REPLACE VIEW dim_uf AS {DIMENSAO_UF}")

    for tabela in (
        "resultado_municipio",
        "resultado_uf",
        "meta_municipio",
        "meta_uf",
        "meta_brasil",
        "municipio_integrado",
        "uf_integrado",
    ):
        con.execute(
            f"CREATE OR REPLACE VIEW {tabela} AS "
            f"SELECT * FROM read_parquet('{caminho_silver(tabela)}')"
        )
    return con


def gravar(con: duckdb.DuckDBPyConnection, consulta: str,
           nome: str, particionar_por_ano: bool) -> int:
    """Executa a consulta e grava o resultado em Parquet."""
    destino = PASTA_GOLD / nome
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True, exist_ok=True)

    if particionar_por_ano:
        alvo = f"'{destino.as_posix()}'"
        opcoes = "FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (ano), OVERWRITE_OR_IGNORE"
    else:
        alvo = f"'{(destino / 'dados.parquet').as_posix()}'"
        opcoes = "FORMAT PARQUET, COMPRESSION ZSTD"

    con.execute(f"COPY ({consulta}) TO {alvo} ({opcoes})")

    linhas = con.execute(f"SELECT count(*) FROM ({consulta})").fetchone()[0]
    kb = sum(p.stat().st_size for p in destino.rglob("*.parquet")) / 1024
    marca = "particionado" if particionar_por_ano else "arquivo unico"
    print(f"  {nome:22} {linhas:>6} linhas  {kb:>8.1f} KB  [{marca}]")
    return linhas


# =====================================================================
# 1. Indicador por municipio
# =====================================================================
SQL_INDICADOR_MUNICIPIO = f"""
SELECT
    r.ano,
    r.id_municipio,
    d.sigla_uf,
    d.nome_uf,
    d.regiao,
    r.rede_codigo,
    r.rede_nome,
    r.taxa_alfabetizacao_observada        AS taxa_alfabetizacao,
    r.media_portugues,
    CASE
        WHEN r.media_portugues >= {PONTO_CORTE} THEN TRUE
        WHEN r.media_portugues IS NULL THEN NULL
        ELSE FALSE
    END                                   AS media_acima_do_corte,
    r.proporcao_aluno_nivel_0,
    r.proporcao_aluno_nivel_1,
    r.proporcao_aluno_nivel_2,
    r.proporcao_aluno_nivel_3,
    r.proporcao_aluno_nivel_4,
    r.proporcao_aluno_nivel_5,
    r.proporcao_aluno_nivel_6,
    r.proporcao_aluno_nivel_7,
    r.proporcao_aluno_nivel_8
FROM resultado_municipio AS r
LEFT JOIN dim_uf AS d
       ON d.codigo_uf = substr(r.id_municipio, 1, 2)
ORDER BY r.ano, d.sigla_uf, r.id_municipio, r.rede_codigo
"""

# =====================================================================
# 2. Indicador por UF
#
# Combina o valor publicado pelo INEP no nivel estadual com uma
# agregacao independente calculada a partir dos municipios. A presenca
# das duas permite conferir uma contra a outra.
# =====================================================================
SQL_INDICADOR_UF = f"""
WITH agregado_municipal AS (
    SELECT
        r.ano,
        substr(r.id_municipio, 1, 2)      AS codigo_uf,
        count(*)                          AS municipios_com_dado,
        round(avg(r.taxa_alfabetizacao_observada), 2)
                                          AS taxa_media_dos_municipios,
        round(avg(r.media_portugues), 2)  AS media_portugues_municipios
    FROM resultado_municipio AS r
    WHERE r.rede_codigo = {REDE_MUNICIPAL}
    GROUP BY 1, 2
)
SELECT
    d.sigla_uf,
    d.nome_uf,
    d.regiao,
    a.ano,
    u.taxa_alfabetizacao_observada        AS taxa_publicada_inep,
    u.media_portugues                     AS media_portugues_inep,
    a.municipios_com_dado,
    a.taxa_media_dos_municipios,
    a.media_portugues_municipios,
    rank() OVER (
        PARTITION BY a.ano
        ORDER BY a.taxa_media_dos_municipios DESC
    )                                     AS posicao_nacional
FROM agregado_municipal AS a
JOIN dim_uf AS d
  ON d.codigo_uf = a.codigo_uf
LEFT JOIN resultado_uf AS u
       ON u.sigla_uf = d.sigla_uf
      AND u.ano = a.ano
      AND u.rede_codigo = {REDE_PUBLICA}
ORDER BY a.ano, posicao_nacional
"""

# =====================================================================
# 3. Meta versus resultado
# =====================================================================
SQL_META_VS_RESULTADO = """
SELECT
    m.ano,
    m.id_municipio,
    d.sigla_uf,
    d.nome_uf,
    d.regiao,
    m.taxa_alfabetizacao_observada        AS taxa_observada,
    m.meta_do_ano,
    m.diferenca_para_meta,
    m.situacao,
    m.tem_meta,
    m.meta_alfabetizacao_2030,
    round(m.meta_alfabetizacao_2030 - m.taxa_alfabetizacao_observada, 2)
                                          AS distancia_para_2030,
    CASE
        WHEN m.situacao NOT IN ('atingiu', 'abaixo') THEN 'nao_comparavel'
        WHEN m.diferenca_para_meta >= 10  THEN 'muito_acima'
        WHEN m.diferenca_para_meta >= 0   THEN 'acima'
        WHEN m.diferenca_para_meta >= -10 THEN 'pouco_abaixo'
        WHEN m.diferenca_para_meta >= -25 THEN 'abaixo'
        ELSE 'muito_abaixo'
    END                                   AS faixa_desempenho,
    m.percentual_participacao
FROM municipio_integrado AS m
LEFT JOIN dim_uf AS d
       ON d.codigo_uf = substr(m.id_municipio, 1, 2)
ORDER BY m.ano, m.diferenca_para_meta
"""

# =====================================================================
# 4. Evolucao temporal
#
# Formato largo: uma linha por municipio, com os dois anos lado a lado.
# E o formato que dashboards de variacao consomem diretamente.
# =====================================================================
SQL_EVOLUCAO_TEMPORAL = f"""
WITH por_ano AS (
    SELECT
        r.id_municipio,
        substr(r.id_municipio, 1, 2)      AS codigo_uf,
        max(CASE WHEN r.ano = 2023 THEN r.taxa_alfabetizacao_observada END)
                                          AS taxa_2023,
        max(CASE WHEN r.ano = 2024 THEN r.taxa_alfabetizacao_observada END)
                                          AS taxa_2024,
        max(CASE WHEN r.ano = 2023 THEN r.media_portugues END)
                                          AS media_2023,
        max(CASE WHEN r.ano = 2024 THEN r.media_portugues END)
                                          AS media_2024
    FROM resultado_municipio AS r
    WHERE r.rede_codigo = {REDE_MUNICIPAL}
    GROUP BY 1, 2
)
SELECT
    p.id_municipio,
    d.sigla_uf,
    d.nome_uf,
    d.regiao,
    p.taxa_2023,
    p.taxa_2024,
    round(p.taxa_2024 - p.taxa_2023, 2)   AS variacao_pontos,
    round(p.media_2024 - p.media_2023, 2) AS variacao_media_portugues,
    CASE
        WHEN p.taxa_2023 IS NULL OR p.taxa_2024 IS NULL THEN 'serie_incompleta'
        WHEN p.taxa_2024 - p.taxa_2023 > 0 THEN 'melhorou'
        WHEN p.taxa_2024 - p.taxa_2023 < 0 THEN 'piorou'
        ELSE 'estavel'
    END                                   AS tendencia
FROM por_ano AS p
LEFT JOIN dim_uf AS d
       ON d.codigo_uf = p.codigo_uf
ORDER BY variacao_pontos DESC
"""


def resumo_analitico(con: duckdb.DuckDBPyConnection) -> None:
    """Imprime alguns numeros da Gold, para conferencia."""
    print("\n  Conferencia:")

    linhas = con.execute("""
        SELECT tendencia, count(*) AS n
        FROM read_parquet($1)
        GROUP BY 1 ORDER BY n DESC
    """, [str((PASTA_GOLD / "evolucao_temporal" / "dados.parquet").as_posix())]
    ).fetchall()
    for tendencia, n in linhas:
        print(f"    {tendencia:20} {n:>6}")

    media = con.execute("""
        SELECT round(avg(variacao_pontos), 2)
        FROM read_parquet($1)
        WHERE variacao_pontos IS NOT NULL
    """, [str((PASTA_GOLD / "evolucao_temporal" / "dados.parquet").as_posix())]
    ).fetchone()[0]
    print(f"    variacao media       {media:>6} pontos")


def main() -> None:
    print("\nCamada Gold - datasets analiticos")
    print("-" * 72)

    con = preparar_conexao()

    gravar(con, SQL_INDICADOR_MUNICIPIO, "indicador_municipio", True)
    gravar(con, SQL_INDICADOR_UF, "indicador_uf", False)
    gravar(con, SQL_META_VS_RESULTADO, "meta_vs_resultado", True)
    gravar(con, SQL_EVOLUCAO_TEMPORAL, "evolucao_temporal", False)

    print("-" * 72)
    resumo_analitico(con)
    con.close()

    print("\nCamada Gold concluida.\n")


if __name__ == "__main__":
    main()