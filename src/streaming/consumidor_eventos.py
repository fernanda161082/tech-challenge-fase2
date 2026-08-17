"""
Consumidor de eventos - simulacao de ingestao em streaming.

Le eventos do topico de forma continua, valida cada um e grava os
aprovados na camada Bronze, em micro-lotes.

Conceitos implementados:

  offset e checkpoint
    o consumidor guarda em disco a posicao da ultima linha lida. Ao ser
    reiniciado, retoma exatamente de onde parou, sem reprocessar nem
    perder eventos.

  micro-lote
    gravar um arquivo Parquet por evento seria desastroso: cada arquivo
    carrega um rodape de metadados de alguns KB. O consumidor acumula
    eventos e grava em lotes, equilibrando latencia e custo.

  validacao na entrada
    eventos fora do formato ou fora de faixa sao desviados para uma
    area de quarentena em vez de contaminarem a Bronze. Nenhum evento
    e descartado em silencio.

Execucao:
    python src/streaming/consumidor_eventos.py
    python src/streaming/consumidor_eventos.py --lote 20 --timeout 15
"""

from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import sys
import time

import pandas as pd

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_STREAMING = RAIZ_PROJETO / "data" / "streaming"
ARQUIVO_TOPICO = PASTA_STREAMING / "eventos.jsonl"
ARQUIVO_CHECKPOINT = PASTA_STREAMING / "_checkpoint.json"
PASTA_QUARENTENA = PASTA_STREAMING / "quarentena"
PASTA_DESTINO = RAIZ_PROJETO / "data" / "bronze" / "eventos_streaming"

CAMPOS_OBRIGATORIOS = (
    "evento_id",
    "publicado_em",
    "tipo",
    "ano",
    "id_municipio",
    "taxa_alfabetizacao",
)


def ler_checkpoint() -> int:
    """Devolve a ultima linha processada. Zero se nunca houve execucao."""
    if not ARQUIVO_CHECKPOINT.exists():
        return 0
    dados = json.loads(ARQUIVO_CHECKPOINT.read_text(encoding="utf-8"))
    return int(dados.get("offset", 0))


def gravar_checkpoint(offset: int, processados: int) -> None:
    """Persiste a posicao de leitura apos gravar o lote.

    A ordem importa: primeiro grava o lote, depois o checkpoint. Se o
    processo cair entre as duas operacoes, o lote sera reprocessado na
    proxima execucao. Isso e preferivel a perder eventos.
    """
    PASTA_STREAMING.mkdir(parents=True, exist_ok=True)
    ARQUIVO_CHECKPOINT.write_text(
        json.dumps(
            {
                "offset": offset,
                "eventos_processados": processados,
                "atualizado_em": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def validar(evento: dict) -> tuple[bool, str]:
    """Verifica se o evento pode entrar na Bronze."""
    for campo in CAMPOS_OBRIGATORIOS:
        if campo not in evento or evento[campo] is None:
            return False, f"campo obrigatorio ausente: {campo}"

    identificador = str(evento["id_municipio"])
    if len(identificador) != 7 or not identificador.isdigit():
        return False, f"id_municipio invalido: {identificador}"

    taxa = evento["taxa_alfabetizacao"]
    if not isinstance(taxa, (int, float)) or not 0 <= taxa <= 100:
        return False, f"taxa fora da faixa 0-100: {taxa}"

    return True, ""


def quarentenar(linha: str, motivo: str) -> None:
    """Guarda o evento rejeitado junto com o motivo da rejeicao."""
    PASTA_QUARENTENA.mkdir(parents=True, exist_ok=True)
    registro = {
        "recebido_em": datetime.now(timezone.utc).isoformat(),
        "motivo": motivo,
        "conteudo": linha.strip(),
    }
    arquivo = PASTA_QUARENTENA / "rejeitados.jsonl"
    with arquivo.open("a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


def gravar_lote(eventos: list, numero_lote: int) -> Path:
    """Grava um micro-lote de eventos validos na Bronze."""
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(eventos)
    df["_ingerido_em"] = datetime.now(timezone.utc).isoformat()
    df["_origem"] = "streaming"
    df["_lote"] = numero_lote

    carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destino = PASTA_DESTINO / f"lote_{carimbo}.parquet"
    df.to_parquet(destino, engine="pyarrow", compression="snappy", index=False)
    return destino


def main() -> int:
    parser = argparse.ArgumentParser(description="Consumidor de eventos")
    parser.add_argument(
        "--lote", type=int, default=10,
        help="quantos eventos acumular antes de gravar",
    )
    parser.add_argument(
        "--timeout", type=float, default=20.0,
        help="segundos sem eventos novos ate encerrar (0 = nunca)",
    )
    parser.add_argument(
        "--reiniciar", action="store_true",
        help="ignora o checkpoint e le o topico desde o inicio",
    )
    args = parser.parse_args()

    if args.reiniciar and ARQUIVO_CHECKPOINT.exists():
        ARQUIVO_CHECKPOINT.unlink()
        print("Checkpoint apagado. Leitura recomeca do inicio do topico.")

    offset = ler_checkpoint()

    print("\nConsumidor de eventos")
    print(f"Topico:     {ARQUIVO_TOPICO.relative_to(RAIZ_PROJETO)}")
    print(f"Destino:    {PASTA_DESTINO.relative_to(RAIZ_PROJETO)}")
    print(f"Offset:     linha {offset}")
    print(f"Micro-lote: {args.lote} eventos")
    print("-" * 72)

    buffer = []
    total_validos = 0
    total_rejeitados = 0
    lotes_gravados = 0
    ultima_novidade = time.time()

    try:
        while True:
            if not ARQUIVO_TOPICO.exists():
                time.sleep(1.0)
                if args.timeout and time.time() - ultima_novidade > args.timeout:
                    print("  Topico inexistente. Encerrando por inatividade.")
                    break
                continue

            with ARQUIVO_TOPICO.open("r", encoding="utf-8") as arquivo:
                linhas = arquivo.readlines()

            novas = linhas[offset:]

            if novas:
                ultima_novidade = time.time()

                for linha in novas:
                    offset += 1
                    if not linha.strip():
                        continue
                    try:
                        evento = json.loads(linha)
                    except json.JSONDecodeError as erro:
                        quarentenar(linha, f"JSON invalido: {erro}")
                        total_rejeitados += 1
                        continue

                    valido, motivo = validar(evento)
                    if not valido:
                        quarentenar(linha, motivo)
                        total_rejeitados += 1
                        print(f"  [REJEITADO] {motivo}")
                        continue

                    buffer.append(evento)
                    total_validos += 1

                    if len(buffer) >= args.lote:
                        lotes_gravados += 1
                        destino = gravar_lote(buffer, lotes_gravados)
                        gravar_checkpoint(offset, total_validos)
                        print(
                            f"  [LOTE {lotes_gravados:>3}] {len(buffer):>3} eventos "
                            f"-> {destino.name}"
                        )
                        buffer = []

            if args.timeout and time.time() - ultima_novidade > args.timeout:
                print(f"  Sem eventos novos por {args.timeout}s. Encerrando.")
                break

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n  Interrompido pelo usuario.")

    # Grava o que sobrou no buffer antes de sair.
    if buffer:
        lotes_gravados += 1
        destino = gravar_lote(buffer, lotes_gravados)
        gravar_checkpoint(offset, total_validos)
        print(f"  [LOTE {lotes_gravados:>3}] {len(buffer):>3} eventos (final)")
    else:
        gravar_checkpoint(offset, total_validos)

    print("-" * 72)
    print(
        f"{total_validos} eventos ingeridos em {lotes_gravados} lotes  |  "
        f"{total_rejeitados} rejeitados  |  offset {offset}\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())