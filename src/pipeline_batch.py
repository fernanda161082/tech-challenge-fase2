"""
Orquestrador do pipeline batch.

Executa todas as etapas do pipeline na ordem correta, respeitando as
dependencias entre camadas, e interrompe a execucao na primeira falha.

Por que existe:
  as etapas tem dependencia estrita (Bronze produz o que a Silver le).
  Rodar na mao exige lembrar a ordem e conferir cada saida. O orquestrador
  garante a ordem, mede o tempo de cada etapa e registra o resultado.

Cada etapa roda como um processo separado. Isso isola falhas: um erro
em uma etapa nao contamina o estado das demais, e o codigo de saida de
cada script e propagado naturalmente.

Execucao:
    python src/pipeline_batch.py
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import subprocess
import sys
import time

RAIZ_PROJETO = Path(__file__).resolve().parents[1]
PASTA_LOGS = RAIZ_PROJETO / "data" / "_logs"

# Ordem das etapas. A dependencia e estrita: cada uma consome o que a
# anterior produziu.
ETAPAS = [
    (
        "bronze",
        "src/bronze/ingestao_bronze.py",
        "Ingestao dos CSVs brutos em Parquet",
    ),
    (
        "silver_padronizacao",
        "src/silver/padronizacao_silver.py",
        "Padronizacao de tipos, nomes e codificacao de rede",
    ),
    (
        "silver_integracao",
        "src/silver/integracao_silver.py",
        "Integracao entre resultados e metas",
    ),
    (
        "qualidade",
        "src/quality/validacao_qualidade.py",
        "Validacao de qualidade dos dados",
    ),
]


def executar_etapa(nome: str, caminho: str, descricao: str) -> dict:
    """Roda uma etapa como processo separado e devolve suas metricas."""
    print(f"\n{'=' * 78}")
    print(f"ETAPA: {nome}")
    print(f"{descricao}")
    print("=" * 78)

    inicio = time.perf_counter()
    processo = subprocess.run(
        [sys.executable, caminho],
        cwd=RAIZ_PROJETO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    duracao = time.perf_counter() - inicio

    if processo.stdout:
        print(processo.stdout, end="")
    if processo.stderr:
        print(processo.stderr, end="", file=sys.stderr)

    sucesso = processo.returncode == 0
    print(
        f"\n--> {nome}: {'OK' if sucesso else 'FALHOU'} "
        f"em {duracao:.2f}s (codigo {processo.returncode})"
    )

    return {
        "etapa": nome,
        "descricao": descricao,
        "sucesso": sucesso,
        "codigo_saida": processo.returncode,
        "duracao_segundos": round(duracao, 3),
    }


def medir_volume() -> dict:
    """Conta arquivos e bytes produzidos em cada camada."""
    volume = {}
    for camada in ("bronze", "silver", "gold"):
        pasta = RAIZ_PROJETO / "data" / camada
        if not pasta.exists():
            continue
        arquivos = list(pasta.rglob("*.parquet"))
        volume[camada] = {
            "arquivos": len(arquivos),
            "kilobytes": round(sum(a.stat().st_size for a in arquivos) / 1024, 1),
        }
    return volume


def gravar_log(execucao: dict) -> Path:
    """Grava o registro da execucao para acompanhamento posterior."""
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    caminho = PASTA_LOGS / f"execucao_{carimbo}.json"
    caminho.write_text(
        json.dumps(execucao, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return caminho


def main() -> int:
    inicio_geral = time.perf_counter()
    momento = datetime.now(timezone.utc).isoformat()

    print("\n" + "#" * 78)
    print("# PIPELINE BATCH - Indicador Crianca Alfabetizada")
    print(f"# Inicio: {momento}")
    print("#" * 78)

    metricas = []
    interrompida = False

    for nome, caminho, descricao in ETAPAS:
        resultado = executar_etapa(nome, caminho, descricao)
        metricas.append(resultado)
        if not resultado["sucesso"]:
            interrompida = True
            break

    duracao_total = time.perf_counter() - inicio_geral

    # ---------------- resumo ----------------
    print("\n" + "#" * 78)
    print("# RESUMO DA EXECUCAO")
    print("#" * 78)
    for m in metricas:
        marca = "OK    " if m["sucesso"] else "FALHOU"
        print(f"  [{marca}] {m['etapa']:24} {m['duracao_segundos']:>8.2f}s")

    nao_executadas = len(ETAPAS) - len(metricas)
    if nao_executadas:
        print(f"  [PULADO] {nao_executadas} etapa(s) nao executada(s) apos a falha")

    volume = medir_volume()
    if volume:
        print("\n  Volume produzido:")
        for camada, dados in volume.items():
            print(
                f"    {camada:8} {dados['arquivos']:>3} arquivos  "
                f"{dados['kilobytes']:>9.1f} KB"
            )

    print(f"\n  Tempo total: {duracao_total:.2f}s")

    execucao = {
        "momento": momento,
        "sucesso": not interrompida,
        "duracao_total_segundos": round(duracao_total, 3),
        "etapas": metricas,
        "volume": volume,
    }
    caminho_log = gravar_log(execucao)
    print(f"  Log: {caminho_log.relative_to(RAIZ_PROJETO)}")

    if interrompida:
        print("\nPIPELINE INTERROMPIDA.\n")
        return 1

    print("\nPIPELINE CONCLUIDA COM SUCESSO.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())