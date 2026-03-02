# =============================================================================
# pipeline.py — Orquestração do pipeline RFB.
#
# Baixa todos os arquivos ESTABELE e EMPRE do mês, filtra ESTABELE por
# SITUACAO_CADASTRAL == "02" (ativos) e salva um Parquet brotli por CSV.
#
# Uso:
#   python pipeline.py \
#     --month 2025-12 \
#     --base-dir /tmp/rfb_data \
#     --max-parallel 2
# =============================================================================

import argparse
from pathlib import Path

from downloader import download_all, get_latest_month


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de dados CNPJ — Receita Federal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--month", default="latest",
        help="Mês de referência (ex: 2025-12) ou 'latest' para o mais recente.",
    )
    parser.add_argument(
        "--base-dir", required=True,
        help="Diretório base para armazenar todos os dados (ex: /tmp/rfb_data).",
    )
    parser.add_argument(
        "--max-parallel", type=int, default=2,
        help="Máximo de downloads simultâneos (padrão: 2).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    base_dir = Path(args.base_dir)

    # Resolve mês
    month = get_latest_month() if args.month == "latest" else args.month

    # Diretório flat de saída: /{base_dir}/{month}/
    output_dir = base_dir / month
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print(f"  Mês de referência : {month}")
    print(f"  Diretório de saída: {output_dir}")
    print(f"  Parallelismo      : {args.max_parallel}")
    print("=" * 60 + "\n")

    # -----------------------------------------------------------------------
    # ETAPA 1 — ESTABELE: download + filtro de ativos + Parquet brotli
    # -----------------------------------------------------------------------
    print("── ETAPA 1: ESTABELE ──────────────────────────────────────\n")
    download_all(month, "ESTABELE", output_dir, args.max_parallel)

    # -----------------------------------------------------------------------
    # ETAPA 2 — EMPRE: download + Parquet brotli
    # -----------------------------------------------------------------------
    print("\n── ETAPA 2: EMPRE ─────────────────────────────────────────\n")
    download_all(month, "EMPRE", output_dir, args.max_parallel)

    print("\n" + "=" * 60)
    print("  ✅ Pipeline concluído!")
    print("=" * 60)


if __name__ == "__main__":
    main()
