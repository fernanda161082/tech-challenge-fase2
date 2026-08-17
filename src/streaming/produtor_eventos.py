"""
Produtor de eventos - simulacao de ingestao em streaming.

Gera eventos de atualizacao do indicador de alfabetizacao e os publica
em um topico, que aqui e um arquivo em modo append.

Por que um arquivo e nao o Kafka:
  o Kafka exige um cluster (brokers, coordenacao, topicos configurados)
  e o servico gerenciado mais barato passa de US$ 100 por mes. Para
  demonstrar o modelo com o volume deste projeto, um arquivo em append
  oferece as mesmas primitivas essenciais: ordenacao, persistencia,
  offset e leitura independente do produtor. O que muda e a escala e a
  tolerancia a falhas, nao o modelo de programacao.

Formato do topico: JSON Lines (um evento JSON por linha). E o formato
usado por praticamente todo sistema de eventos, porque permite anexar
um registro sem reescrever o arquivo e ler linha a linha sem carregar
tudo na memoria.

Execucao:
    python src/streaming/produtor_eventos.py
    python src/streaming/produtor_eventos.py --quantidade 50 --intervalo 0.2
"""

from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import random
import sys
import time
import uuid

import pandas as pd

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_SILVER = RAIZ_PROJETO / "data" / "silver"
PASTA_STREAMING = RAIZ_PROJETO / "data" / "streaming"
ARQUIVO_TOPICO = PASTA_STREAMING / "eventos.jsonl"

TIPOS_EVENTO = [
    "atualizacao_indicador",
    "nova_medicao",
    "revisao_resultado",
]

# Variacao maxima aplicada ao valor de referencia, em pontos percentuais.
VARIACAO_MAXIMA = 8.0


def carregar_municipios() -> pd.DataFrame:
    """Le municipios reais da Silver, para gerar eventos plausiveis."""
    caminho = PASTA_SILVER / "resultado_municipio" / "dados.parquet"
    if not caminho.exists():
        print(
            "ERRO: camada Silver nao encontrada.\n"
            "      Rode o pipeline batch antes: python src/pipeline_batch.py",
            file=sys.stderr,
        )
        sys.exit(1)

    df = pd.read_parquet(caminho, engine="pyarrow")
    df = df[(df["ano"] == 2024) & (df["rede_codigo"] == 3)]
    return df[
        ["id_municipio", "taxa_alfabetizacao_observada", "media_portugues"]
    ].dropna()


def gerar_evento(base: pd.DataFrame) -> dict:
    """Monta um evento a partir de um municipio real, com valor perturbado."""
    linha = base.sample(1).iloc[0]

    delta = random.uniform(-VARIACAO_MAXIMA, VARIACAO_MAXIMA)
    taxa = max(0.0, min(100.0, float(linha["taxa_alfabetizacao_observada"]) + delta))
    media = float(linha["media_portugues"]) + delta * 2

    return {
        "evento_id": str(uuid.uuid4()),
        "publicado_em": datetime.now(timezone.utc).isoformat(),
        "tipo": random.choice(TIPOS_EVENTO),
        "ano": 2025,
        "id_municipio": str(linha["id_municipio"]),
        "rede_codigo": 3,
        "taxa_alfabetizacao": round(taxa, 2),
        "media_portugues": round(media, 2),
        "origem": "simulador",
    }


def publicar(evento: dict) -> None:
    """Anexa um evento ao topico.

    O modo 'a' (append) garante que a escrita nao sobrescreve o que ja
    existe, e cada linha e um registro completo e independente.
    """
    PASTA_STREAMING.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_TOPICO.open("a", encoding="utf-8") as arquivo:
        arquivo.write(json.dumps(evento, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Produtor de eventos simulados")
    parser.add_argument(
        "--quantidade", type=int, default=30,
        help="numero de eventos a publicar (0 = continuo ate Ctrl+C)",
    )
    parser.add_argument(
        "--intervalo", type=float, default=0.5,
        help="segundos entre eventos",
    )
    parser.add_argument(
        "--limpar", action="store_true",
        help="apaga o topico antes de comecar",
    )
    args = parser.parse_args()

    if args.limpar and ARQUIVO_TOPICO.exists():
        ARQUIVO_TOPICO.unlink()
        print("Topico limpo.")

    base = carregar_municipios()

    print(f"\nProdutor de eventos")
    print(f"Topico: {ARQUIVO_TOPICO.relative_to(RAIZ_PROJETO)}")
    print(f"Base:   {len(base)} municipios reais")
    print(f"Ritmo:  1 evento a cada {args.intervalo}s")
    print("-" * 72)

    publicados = 0
    try:
        while args.quantidade == 0 or publicados < args.quantidade:
            evento = gerar_evento(base)
            publicar(evento)
            publicados += 1
            print(
                f"  [{publicados:>4}] {evento['tipo']:22} "
                f"municipio {evento['id_municipio']}  "
                f"taxa {evento['taxa_alfabetizacao']:>6.2f}"
            )
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n  Interrompido pelo usuario.")

    print("-" * 72)
    print(f"{publicados} eventos publicados no topico.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
