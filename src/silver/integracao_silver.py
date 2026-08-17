"""
Camada Silver - etapa 2: integracao.

Junta os resultados observados com as metas do Compromisso Nacional
Crianca Alfabetizada e calcula a distancia entre um e outro.

Le de data/silver/ (tabelas ja padronizadas) e grava de volta em
data/silver/, em tabelas novas com sufixo _integrado.

Decisoes aplicadas aqui:
  - o join e LEFT: municipios sem meta permanecem na base, marcados
    com tem_meta = False. Nunca sao descartados nem preenchidos com zero.
  - a trajetoria de metas comeca em 2024. Para o ano de 2023 nao existe
    meta comparavel, entao meta_do_ano fica nula por definicao.
  - no nivel municipal comparamos a rede municipal (codigo 3), que e a
    rede a que as metas municipais se referem.
  - no nivel estadual comparamos a rede publica (codigo 5), que e a
    rede a que as metas estaduais se referem.

Execucao:
    python src/silver/integracao_silver.py
"""

from pathlib import Path
import shutil

import pandas as pd

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_SILVER = RAIZ_PROJETO / "data" / "silver"

# Rede usada em cada nivel de comparacao.
REDE_MUNICIPAL = 3
REDE_PUBLICA = 5

# Primeiro ano da trajetoria de metas do Compromisso.
PRIMEIRO_ANO_META = 2024

COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]


def ler(nome_tabela: str) -> pd.DataFrame:
    """Le uma tabela ja padronizada da camada Silver."""
    caminho = PASTA_SILVER / nome_tabela / "dados.parquet"
    if not caminho.exists():
        raise FileNotFoundError(
            f"Tabela '{nome_tabela}' nao encontrada na Silver. "
            "Rode padronizacao_silver.py antes deste script."
        )
    return pd.read_parquet(caminho, engine="pyarrow")


def gravar(df: pd.DataFrame, nome_tabela: str) -> None:
    """Grava uma tabela integrada na camada Silver."""
    destino = PASTA_SILVER / nome_tabela
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        destino / "dados.parquet",
        engine="pyarrow",
        compression="snappy",
        index=False,
    )


def meta_do_ano(linha: pd.Series) -> float:
    """Devolve o valor da meta correspondente ao ano da propria linha.

    A trajetoria e armazenada em colunas largas (uma por ano ate 2030).
    Esta funcao escolhe a coluna certa para cada linha. Antes de 2024
    nao ha meta definida, entao o retorno e nulo.
    """
    ano = linha["ano"]
    if pd.isna(ano) or int(ano) < PRIMEIRO_ANO_META:
        return pd.NA
    coluna = f"meta_alfabetizacao_{int(ano)}"
    return linha.get(coluna, pd.NA)


def integrar(
    resultado: pd.DataFrame,
    meta: pd.DataFrame,
    chave: list,
    rede: int,
    rotulo: str,
) -> pd.DataFrame:
    """Junta resultado e meta pela chave informada, preservando orfaos."""
    esquerda = resultado[resultado["rede_codigo"] == rede].copy()

    colunas_meta = chave + ["taxa_alfabetizacao_referencia"]
    colunas_meta += [c for c in COLUNAS_META if c in meta.columns]
    if "percentual_participacao" in meta.columns:
        colunas_meta.append("percentual_participacao")
    if "nivel_alfabetizacao" in meta.columns:
        colunas_meta.append("nivel_alfabetizacao")

    direita = meta[colunas_meta].copy()

    df = esquerda.merge(direita, on=chave, how="left", indicator=True)

    df["tem_meta"] = df["_merge"] == "both"
    df = df.drop(columns=["_merge"])

    df["meta_do_ano"] = df.apply(meta_do_ano, axis=1)
    df["meta_do_ano"] = pd.to_numeric(df["meta_do_ano"], errors="coerce")

    # Diferenca positiva = acima da meta; negativa = abaixo.
    df["diferenca_para_meta"] = (
        df["taxa_alfabetizacao_observada"] - df["meta_do_ano"]
    ).round(2)

    # A situacao distingue tres motivos diferentes de ausencia de meta,
    # porque eles significam coisas distintas para um gestor publico.
    df["situacao"] = "avaliado"
    df.loc[~df["tem_meta"], "situacao"] = "municipio_sem_meta"
    df.loc[df["tem_meta"] & (df["ano"] < PRIMEIRO_ANO_META), "situacao"] = (
        "ano_anterior_a_trajetoria"
    )
    df.loc[
        df["tem_meta"]
        & (df["ano"] >= PRIMEIRO_ANO_META)
        & df["meta_do_ano"].isna(),
        "situacao",
    ] = "meta_do_ano_ausente"

    comparavel = df["situacao"] == "avaliado"
    df.loc[comparavel & (df["diferenca_para_meta"] >= 0), "situacao"] = "atingiu"
    df.loc[comparavel & (df["diferenca_para_meta"] < 0), "situacao"] = "abaixo"

    orfaos = (~df["tem_meta"]).sum()
    comparaveis = int(comparavel.sum())
    print(
        f"  {rotulo:24} {len(df):>6} linhas  "
        f"{orfaos:>4} sem meta  {comparaveis:>6} comparaveis"
    )
    return df


def main() -> None:
    print("\nSilver - etapa 2: integracao")
    print("-" * 76)

    # ---------------- nivel municipal ----------------
    municipio = integrar(
        resultado=ler("resultado_municipio"),
        meta=ler("meta_municipio"),
        chave=["ano", "id_municipio", "rede_codigo"],
        rede=REDE_MUNICIPAL,
        rotulo="municipio_integrado",
    )
    gravar(municipio, "municipio_integrado")

    # ---------------- nivel estadual ----------------
    uf = integrar(
        resultado=ler("resultado_uf"),
        meta=ler("meta_uf"),
        chave=["ano", "sigla_uf", "rede_codigo"],
        rede=REDE_PUBLICA,
        rotulo="uf_integrado",
    )
    gravar(uf, "uf_integrado")

    print("-" * 76)

    # ---------------- avisos de cobertura ----------------
    ufs_com_resultado = uf["sigla_uf"].nunique()
    if ufs_com_resultado < 27:
        faltantes = sorted(
            set(ler("meta_uf")["sigla_uf"]) - set(uf["sigla_uf"])
        )
        print(f"  [COBERTURA] {ufs_com_resultado} de 27 UFs com resultado.")
        print(f"              Sem resultado publicado: {', '.join(faltantes)}")

    resumo = municipio[municipio["situacao"].isin(["atingiu", "abaixo"])]
    if len(resumo):
        atingiram = (resumo["situacao"] == "atingiu").mean() * 100
        print(
            f"  [RESUMO]    {atingiram:.1f}% dos municipios comparaveis "
            "atingiram a meta do ano."
        )

    print("\nIntegracao concluida.\n")


if __name__ == "__main__":
    main()