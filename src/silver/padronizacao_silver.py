"""
Camada Silver - etapa 1: padronizacao.

Le os Parquet da camada Bronze, aplica tipos e nomes consistentes,
e grava o resultado em data/silver/.

Esta etapa NAO integra tabelas. Ela apenas deixa cada tabela limpa
e com o mesmo vocabulario, para que a integracao (etapa 2) seja simples.

Decisoes aplicadas aqui:
  - 'rede' passa a ter duas colunas: rede_codigo (int) e rede_nome (texto)
  - 'id_municipio' vira texto de 7 caracteres, para nao perder zeros
  - 'taxa_alfabetizacao' e renomeada conforme o papel da tabela:
        resultado -> taxa_alfabetizacao_observada
        meta      -> taxa_alfabetizacao_referencia
  - valores ausentes sao mantidos como nulos, nunca preenchidos com zero

Execucao:
    python src/silver/padronizacao_silver.py
"""

from pathlib import Path
import shutil

import pandas as pd

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_BRONZE = RAIZ_PROJETO / "data" / "bronze"
PASTA_SILVER = RAIZ_PROJETO / "data" / "silver"

# ---------------------------------------------------------------------
# Mapa da coluna 'rede'
#
# Os resultados usam codigos numericos; as metas usam texto.
# O significado dos codigos foi inferido a partir dos proprios dados:
#   - a rede 5 e sempre um agregado das redes 2 e 3  -> publica
#   - a taxa da meta 'Municipal' bate 100% com a rede 3 -> municipal
#   - a rede 0 aparece so na Bahia em 2024 e duplica a rede 3 -> anomalia
# ---------------------------------------------------------------------
MAPA_REDE = {
    "0": (0, "nao_identificada"),
    "2": (2, "estadual"),
    "3": (3, "municipal"),
    "5": (5, "publica"),
    "Estadual": (2, "estadual"),
    "Municipal": (3, "municipal"),
    "Pública": (5, "publica"),
    "Publica": (5, "publica"),
}

# Colunas que devem virar numero decimal, quando existirem na tabela.
COLUNAS_DECIMAIS = (
    ["taxa_alfabetizacao", "media_portugues", "percentual_participacao"]
    + [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    + [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]
    + ["nivel_alfabetizacao"]
)

# Configuracao de cada tabela: papel e chave natural.
TABELAS = {
    "resultado_municipio": {
        "papel": "resultado",
        "chave": ["ano", "id_municipio", "serie", "rede_codigo"],
    },
    "resultado_uf": {
        "papel": "resultado",
        "chave": ["ano", "sigla_uf", "serie", "rede_codigo"],
    },
    "meta_municipio": {
        "papel": "meta",
        "chave": ["ano", "id_municipio", "rede_codigo"],
    },
    "meta_uf": {
        "papel": "meta",
        "chave": ["ano", "sigla_uf", "rede_codigo"],
    },
    "meta_brasil": {
        "papel": "meta",
        "chave": ["ano", "rede_codigo"],
    },
}


def padronizar_rede(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma a coluna 'rede' em rede_codigo (int) e rede_nome (texto)."""
    if "rede" not in df.columns:
        return df

    bruto = df["rede"].astype(str).str.strip()
    desconhecidos = sorted(set(bruto.unique()) - set(MAPA_REDE))
    if desconhecidos:
        print(f"      [ATENCAO] valores de rede nao mapeados: {desconhecidos}")

    df["rede_codigo"] = bruto.map(lambda v: MAPA_REDE.get(v, (-1, "desconhecida"))[0])
    df["rede_nome"] = bruto.map(lambda v: MAPA_REDE.get(v, (-1, "desconhecida"))[1])
    return df.drop(columns=["rede"])


def padronizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Converte cada coluna para o tipo correto."""
    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "serie" in df.columns:
        df["serie"] = pd.to_numeric(df["serie"], errors="coerce").astype("Int64")

    # id_municipio como texto de 7 caracteres: preserva zeros a esquerda
    # e garante compatibilidade com as bases territoriais do IBGE.
    if "id_municipio" in df.columns:
        df["id_municipio"] = (
            df["id_municipio"].astype(str).str.strip().str.zfill(7)
        )

    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].astype(str).str.strip().str.upper()

    for coluna in COLUNAS_DECIMAIS:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    return df


def renomear_taxa(df: pd.DataFrame, papel: str) -> pd.DataFrame:
    """Desfaz a ambiguidade da coluna 'taxa_alfabetizacao'.

    Ela existe nos dois tipos de tabela com significados diferentes:
    nos resultados e o valor observado; nas metas e o ponto de partida
    usado como referencia para a trajetoria ate 2030.
    """
    if "taxa_alfabetizacao" not in df.columns:
        return df

    novo_nome = (
        "taxa_alfabetizacao_observada"
        if papel == "resultado"
        else "taxa_alfabetizacao_referencia"
    )
    return df.rename(columns={"taxa_alfabetizacao": novo_nome})


def ordenar_colunas(df: pd.DataFrame, chave: list) -> pd.DataFrame:
    """Coloca as colunas de chave na frente e as de auditoria no fim."""
    auditoria = [c for c in df.columns if c.startswith("_")]
    presentes = [c for c in chave if c in df.columns]
    demais = [c for c in df.columns if c not in presentes and c not in auditoria]
    return df[presentes + demais + auditoria]


def processar(nome_tabela: str, config: dict) -> bool:
    """Le uma tabela da Bronze, padroniza e grava na Silver."""
    origem = PASTA_BRONZE / nome_tabela

    if not origem.exists():
        print(f"  [ERRO] tabela ausente na Bronze: {nome_tabela}")
        return False

    df = pd.read_parquet(origem, engine="pyarrow")
    linhas_entrada = len(df)

    df = padronizar_rede(df)
    df = padronizar_tipos(df)
    df = renomear_taxa(df, config["papel"])
    df = ordenar_colunas(df, config["chave"])

    # Ordena pela chave natural: facilita leitura e melhora a compressao.
    chave_presente = [c for c in config["chave"] if c in df.columns]
    df = df.sort_values(chave_presente).reset_index(drop=True)

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

    duplicadas = df.duplicated(chave_presente).sum()
    aviso = "" if duplicadas == 0 else f"  [{duplicadas} chaves duplicadas]"

    print(
        f"  {nome_tabela:22} {linhas_entrada:>6} linhas  "
        f"{len(df.columns):>2} colunas{aviso}"
    )
    return True


def main() -> None:
    print("\nSilver - etapa 1: padronizacao")
    print("-" * 72)

    sucessos = 0
    for nome_tabela, config in TABELAS.items():
        if processar(nome_tabela, config):
            sucessos += 1

    print("-" * 72)
    print(f"Concluido: {sucessos} de {len(TABELAS)} tabelas padronizadas.\n")


if __name__ == "__main__":
    main()