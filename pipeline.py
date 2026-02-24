# =============================================================================
# pipeline.py — Orquestração do pipeline RFB.
#
# Uso:
#   python pipeline.py \
#     --month 2025-11 \
#     --outputs municipio cnpj_estab \
#     --municipios 5002704 5002472 \
#     --uf MS \
#     --base-dir /tmp/rfb_data \
#     --cnpjs-file /tmp/cnpjs.xlsx
# =============================================================================

import argparse
import sys
from pathlib import Path

import polars as pl

from config import SIAFI_MAP_PATH
from downloader import download_all, get_latest_month
from enricher import enrich
from filterer import (
    filter_by_municipio,
    filter_cnpj_empresa,
    filter_cnpj_estab,
    ibge_to_info,
    load_cnpjs_from_xlsx,
    load_siafi_map,
    siafi_to_ibge,
)


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
        help="Mês de referência (ex: 2025-11) ou 'latest' para o mais recente.",
    )
    parser.add_argument(
        "--outputs", nargs="+", required=True,
        choices=["municipio", "cnpj_estab", "cnpj_empresa"],
        help="Outputs desejados (um ou mais).",
    )
    parser.add_argument(
        "--municipios", nargs="+", type=int,
        help="Códigos IBGE dos municípios (ex: 5002704 5002472). "
             "Obrigatório para output 'municipio'.",
    )
    parser.add_argument(
        "--uf", type=str,
        help="UF para arquivo de coordenadas (ex: MS). "
             "Obrigatório para outputs de estabelecimentos.",
    )
    parser.add_argument(
        "--cnpjs-file", type=str,
        help="Caminho do XLSX com coluna de CNPJs. "
             "Obrigatório para outputs 'cnpj_estab' e 'cnpj_empresa'.",
    )
    parser.add_argument(
        "--base-dir", required=True,
        help="Diretório base para armazenar todos os dados (ex: /tmp/rfb_data).",
    )
    parser.add_argument(
        "--coords-dir", type=str, default=None,
        help="Diretório com arquivos coord_{UF}.parquet. "
             "Padrão: mesmo que --base-dir.",
    )
    parser.add_argument(
        "--max-parallel", type=int, default=2,
        help="Máximo de downloads simultâneos (padrão: 2).",
    )
    parser.add_argument(
        "--no-nominatim", action="store_true",
        help="Desabilita o fallback via Nominatim.",
    )

    args = parser.parse_args()

    # Validações cruzadas
    needs_estab  = "municipio"    in args.outputs or "cnpj_estab"   in args.outputs
    needs_empresa = "cnpj_empresa" in args.outputs
    needs_cnpj   = "cnpj_estab"  in args.outputs or "cnpj_empresa"  in args.outputs

    if "municipio" in args.outputs and not args.municipios:
        parser.error("--municipios é obrigatório para o output 'municipio'.")

    if needs_cnpj and not args.cnpjs_file:
        parser.error("--cnpjs-file é obrigatório para outputs 'cnpj_estab' e 'cnpj_empresa'.")

    if needs_estab and not args.uf:
        parser.error("--uf é obrigatório para outputs que enriquecem estabelecimentos.")

    return args


# ---------------------------------------------------------------------------
# Helpers de enriquecimento
# ---------------------------------------------------------------------------

def _enrich_and_save(
    raw_path: Path,
    out_path: Path,
    uf: str,
    coords_dir: Path,
    ibge_codes: list[int] | None,
    use_nominatim: bool,
) -> None:
    """Lê um Parquet de estabelecimentos, enriquece e salva."""
    df = pl.read_parquet(raw_path)
    df = enrich(df, uf, coords_dir, ibge_codes=ibge_codes, use_nominatim=use_nominatim)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path, compression="snappy")
    print(f"[SAVE] {out_path.name}\n")


def _enrich_cnpj_estab(
    raw_path: Path,
    out_path: Path,
    uf_arg: str,
    coords_dir: Path,
    df_map: pl.DataFrame,
    use_nominatim: bool,
) -> None:
    """
    Enriquece o resultado do filtro por CNPJ agrupando por UF presente nos dados.
    Para cada UF encontrada, carrega o arquivo de coordenadas correspondente
    e enriquece apenas as linhas daquela UF.
    """
    df = pl.read_parquet(raw_path)
    ufs_no_df = df["UF"].drop_nulls().unique().to_list()
    partes: list[pl.DataFrame] = []

    for uf_val in ufs_no_df:
        df_uf = df.filter(pl.col("UF") == uf_val)

        # Converte os códigos SIAFI presentes na coluna MUNICIPIO → IBGE
        siafi_no_df = df_uf["MUNICIPIO"].drop_nulls().unique().to_list()
        ibge_codes  = siafi_to_ibge(siafi_no_df, df_map)

        try:
            df_uf = enrich(
                df_uf, uf_val, coords_dir,
                ibge_codes=ibge_codes or None,
                use_nominatim=use_nominatim,
            )
        except FileNotFoundError:
            print(f"[WARN] Sem arquivo de coords para UF={uf_val}. Pulando enriquecimento.")

        partes.append(df_uf)

    df_final = pl.concat(partes, how="diagonal") if partes else df
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.write_parquet(out_path, compression="snappy")
    print(f"[SAVE] {out_path.name}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    base_dir   = Path(args.base_dir)
    coords_dir = Path(args.coords_dir) if args.coords_dir else base_dir
    use_nominatim = not args.no_nominatim

    # Resolve mês
    month = get_latest_month() if args.month == "latest" else args.month

    print("\n" + "=" * 60)
    print(f"  Mês de referência : {month}")
    print(f"  Outputs           : {args.outputs}")
    if args.municipios:
        print(f"  Municípios (IBGE) : {args.municipios}")
    if args.uf:
        print(f"  UF                : {args.uf}")
    print("=" * 60 + "\n")

    parquet_dir = base_dir / month / "parquet"
    output_dir  = base_dir / month / "outputs"

    needs_estab  = "municipio"    in args.outputs or "cnpj_estab"   in args.outputs
    needs_empresa = "cnpj_empresa" in args.outputs

    # -----------------------------------------------------------------------
    # ETAPA 1 — Download e conversão
    # -----------------------------------------------------------------------
    print("── ETAPA 1: Download ──────────────────────────────────────\n")

    if needs_estab:
        download_all(month, "ESTABELE", parquet_dir / "ESTABELE", args.max_parallel)

    if needs_empresa:
        download_all(month, "EMPRE", parquet_dir / "EMPRE", args.max_parallel)

    # -----------------------------------------------------------------------
    # ETAPA 2 — Filtragem
    # -----------------------------------------------------------------------
    print("\n── ETAPA 2: Filtragem ─────────────────────────────────────\n")

    cnpjs: set[str] | None = None
    if args.cnpjs_file:
        cnpjs = load_cnpjs_from_xlsx(args.cnpjs_file)

    df_map = load_siafi_map()

    # Output: por município
    municipio_paths: dict[int, Path] = {}
    if "municipio" in args.outputs:
        ibge_info = ibge_to_info(args.municipios, df_map)
        municipio_paths = filter_by_municipio(
            parquet_dir / "ESTABELE",
            ibge_info,
            output_dir / "municipio" / "_raw",
        )

    # Output: estabelecimentos por CNPJ
    cnpj_estab_raw: Path | None = None
    if "cnpj_estab" in args.outputs:
        cnpj_estab_raw = output_dir / "cnpj_estab" / "_raw" / "ESTAB_CNPJ.parquet"
        filter_cnpj_estab(parquet_dir / "ESTABELE", cnpjs, cnpj_estab_raw)

    # Output: empresas por CNPJ (sem enriquecimento)
    if "cnpj_empresa" in args.outputs:
        empresa_out = output_dir / "cnpj_empresa" / "EMPRESA_CNPJ.parquet"
        filter_cnpj_empresa(parquet_dir / "EMPRE", cnpjs, empresa_out)

    # -----------------------------------------------------------------------
    # ETAPA 3 — Enriquecimento com coordenadas
    # -----------------------------------------------------------------------
    print("\n── ETAPA 3: Enriquecimento ────────────────────────────────\n")

    if "municipio" in args.outputs:
        ibge_info = ibge_to_info(args.municipios, df_map)
        for ibge, raw_path in municipio_paths.items():
            info = ibge_info[ibge]
            nome_safe = info["nome"].replace(" ", "_").replace("/", "-")
            out_path  = output_dir / "municipio" / f"ESTAB_{nome_safe}_{ibge}.parquet"
            print(f"[ENRI] {info['nome']} ({ibge})...")
            _enrich_and_save(
                raw_path, out_path,
                uf=args.uf,
                coords_dir=coords_dir,
                ibge_codes=[ibge],
                use_nominatim=use_nominatim,
            )

    if "cnpj_estab" in args.outputs and cnpj_estab_raw:
        out_path = output_dir / "cnpj_estab" / "ESTAB_CNPJ.parquet"
        print("[ENRI] Estabelecimentos por CNPJ...")
        _enrich_cnpj_estab(
            cnpj_estab_raw, out_path,
            uf_arg=args.uf,
            coords_dir=coords_dir,
            df_map=df_map,
            use_nominatim=use_nominatim,
        )

    print("=" * 60)
    print("  ✅ Pipeline concluído!")
    print("=" * 60)


if __name__ == "__main__":
    main()
