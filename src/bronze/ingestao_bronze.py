"""
Camada Bronze - ingestao dos dados brutos do INEP.

Le os CSVs de data/raw/ e grava em Parquet dentro de data/bronze/,
sem alterar o conteudo original.

Principios desta camada:
  - nenhuma transformacao de dado e aplicada
  - tudo e lido como texto, para nao perder informacao na conversao
  - o processo e idempotente: rodar varias vezes gera o mesmo resultado
  - o particionamento so e aplicado quando compensa

Execucao:
    python src/bronze/ingestao_bronze.py
"""

from datetime import datetime, timezone
from pathlib import Path
import shutil

import pandas as pd

# ---------------------------------------------------------------------
# Caminhos do projeto
# ---------------------------------------------------------------------
RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_RAW = RAIZ_PROJETO / "data" / "raw"
PASTA_BRONZE = RAIZ_PROJETO / "data" / "bronze"

# ---------------------------------------------------------------------
# Mapa: nome logico -> arquivo de origem
# ---------------------------------------------------------------------
FONTES = {
    "resultado_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "resultado_uf": "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_municipio": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    "meta_uf": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_brasil": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
}

COLUNA_PARTICAO = "ano"
LIMITE_PARTICAO = 1_000  # abaixo disso, particionar custa mais do que rende


def tamanho_kb(caminho: Path) -> float:
    """Retorna o tamanho em KB de um arquivo ou de uma pasta de Parquet."""
    if caminho.is_dir():
        return sum(p.stat().st_size for p in caminho.rglob("*.parquet")) / 1024
    return caminho.stat().st_size / 1024


def ingerir(nome_logico: str, nome_arquivo: str) -> bool:
    """Le um CSV de data/raw e grava em Parquet dentro de data/bronze.

    Retorna True se a ingestao ocorreu, False se o arquivo nao foi encontrado.
    """
    caminho_csv = PASTA_RAW / nome_arquivo

    if not caminho_csv.exists():
        print(f"  [ERRO] arquivo nao encontrado: {nome_arquivo}")
        return False

    # dtype=str preserva o dado exatamente como esta no arquivo.
    # A conversao de tipos e responsabilidade da camada Silver.
    df = pd.read_csv(caminho_csv, dtype=str)

    # Colunas de auditoria (governanca): rastreabilidade da origem.
    df["_arquivo_origem"] = nome_arquivo
    df["_ingerido_em"] = datetime.now(timezone.utc).isoformat()

    destino = PASTA_BRONZE / nome_logico

    # Idempotencia: limpa o destino antes de gravar, para que rodar
    # o script duas vezes nao acumule arquivos duplicados.
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True, exist_ok=True)

    # Particionar so compensa em tabelas grandes: cada particao gera um
    # arquivo com rodape de metadados proprio. Em tabelas pequenas esse
    # rodape custa mais que o dado em si.
    particionar = len(df) >= LIMITE_PARTICAO and COLUNA_PARTICAO in df.columns

    if particionar:
        df.to_parquet(
            destino,
            engine="pyarrow",
            compression="snappy",
            partition_cols=[COLUNA_PARTICAO],
            index=False,
        )
    else:
        df.to_parquet(
            destino / "dados.parquet",
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

    kb_csv = tamanho_kb(caminho_csv)
    kb_parquet = tamanho_kb(destino)
    reducao = (1 - kb_parquet / kb_csv) * 100 if kb_csv else 0.0

    if reducao >= 0:
        nota = f"{reducao:5.1f}% menor"
    else:
        nota = f"{-reducao:5.1f}% maior"

    marca = "particionado" if particionar else "arquivo unico"

    print(
        f"  {nome_logico:22} {len(df):>6} linhas  "
        f"{kb_csv:>8.1f} KB -> {kb_parquet:>7.1f} KB  "
        f"({nota})  [{marca}]"
    )
    return True


def main() -> None:
    print("\nIngestao Bronze")
    print("-" * 88)

    sucessos = 0
    for nome_logico, nome_arquivo in FONTES.items():
        if ingerir(nome_logico, nome_arquivo):
            sucessos += 1

    print("-" * 88)
    print(f"Concluido: {sucessos} de {len(FONTES)} fontes ingeridas.\n")


if __name__ == "__main__":
    main()
