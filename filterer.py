# =============================================================================
# filterer.py — Filtragem de Estabelecimentos e Empresas via Polars LazyFrame.
#
# Outputs disponíveis:
#   • filter_by_municipio  → um Parquet por município (código SIAFI)
#   • filter_cnpj_estab    → Parquet de estabelecimentos filtrados por CNPJ
#   • filter_cnpj_empresa  → Parquet de empresas filtradas por CNPJ_BASICO
#
# IMPORTANTE: todos os filtros processam um arquivo Parquet por vez para
# evitar OOM no runner do GitHub Actions (7 GB de RAM). O LazyFrame global
# (scan_parquet com wildcard) foi substituído por um loop explícito que lê,
# filtra e libera cada arquivo individualmente.
# =============================================================================

from pathlib import Path

import polars as pl

from config import SIAFI_MAP_PATH


# ---------------------------------------------------------------------------
# Tabela SIAFI ↔ IBGE
# ---------------------------------------------------------------------------

def load_siafi_map() -> pl.DataFrame:
    """Carrega a tabela de mapeamento SIAFI ↔ IBGE."""
    return pl.read_csv(
        SIAFI_MAP_PATH,
        separator=";",
        encoding="latin1",
        has_header=True,
        new_columns=["SIAFI", "IBGE", "MUNICIPIO_NOME", "UF"],
        infer_schema_length=0,   # tudo como string para evitar perda de zeros
    ).with_columns([
        pl.col("SIAFI").cast(pl.Int64),
        pl.col("IBGE").cast(pl.Int64),
    ])


def ibge_to_info(ibge_codes: list[int], df_map: pl.DataFrame) -> dict[int, dict]:
    """
    Converte lista de códigos IBGE em dicionário com informações de cada município.

    Retorna: {ibge: {"siafi": int, "nome": str, "uf": str}}
    """
    result = {}
    for ibge in ibge_codes:
        row = df_map.filter(pl.col("IBGE") == ibge)
        if row.is_empty():
            raise ValueError(
                f"Código IBGE {ibge} não encontrado na tabela de mapeamento."
            )
        r = row.row(0, named=True)
        result[ibge] = {
            "siafi": r["SIAFI"],
            "nome": r["MUNICIPIO_NOME"],
            "uf": r["UF"],
        }
    return result


def siafi_to_ibge(siafi_codes: list[str], df_map: pl.DataFrame) -> list[int]:
    """
    Converte lista de códigos SIAFI (string) em lista de códigos IBGE (int).
    Usado internamente para determinar quais municípios buscar no arquivo de coords.
    """
    df_map_str = df_map.with_columns(pl.col("SIAFI").cast(pl.Utf8))
    filtered = df_map_str.filter(pl.col("SIAFI").is_in(siafi_codes))
    return filtered["IBGE"].to_list()


# ---------------------------------------------------------------------------
# Utilidade interna
# ---------------------------------------------------------------------------

def _list_parquet_files(parquet_dir: Path) -> list[Path]:
    """Retorna lista de arquivos .parquet em um diretório. Lança erro se vazio."""
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo .parquet encontrado em: {parquet_dir}"
        )
    return files


# ---------------------------------------------------------------------------
# Filtro por município
# ---------------------------------------------------------------------------

def filter_by_municipio(
    parquet_dir: Path,
    ibge_info: dict[int, dict],
    output_dir: Path,
) -> dict[int, Path]:
    """
    Filtra os Parquets de ESTABELE por código SIAFI do município.
    Processa um arquivo por vez para controlar o uso de memória.
    Gera um arquivo de saída independente por município.

    Args:
        parquet_dir: Diretório com os Parquets de ESTABELE
        ibge_info:   Dicionário retornado por ibge_to_info()
        output_dir:  Diretório de saída

    Retorna: {ibge_code: path_do_parquet_filtrado}
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = _list_parquet_files(parquet_dir)

    # Prepara acumuladores por município
    siafi_to_ibge_local = {
        str(info["siafi"]): ibge for ibge, info in ibge_info.items()
    }
    acumuladores: dict[int, list[pl.DataFrame]] = {ibge: [] for ibge in ibge_info}

    for f in files:
        print(f"[FILT] Escaneando {f.name}...")
        df = (
            pl.scan_parquet(str(f))
            .filter(pl.col("MUNICIPIO").is_in(list(siafi_to_ibge_local.keys())))
            .collect()
        )
        if df.is_empty():
            continue

        # Distribui as linhas para cada município encontrado
        for siafi_str, ibge in siafi_to_ibge_local.items():
            parte = df.filter(pl.col("MUNICIPIO") == siafi_str)
            if len(parte) > 0:
                acumuladores[ibge].append(parte)

    outputs: dict[int, Path] = {}
    for ibge, info in ibge_info.items():
        partes = acumuladores[ibge]
        nome_safe = info["nome"].replace(" ", "_").replace("/", "-")
        out_path = output_dir / f"ESTAB_{nome_safe}_{ibge}.parquet"

        if not partes:
            print(f"[WARN] Nenhum registro encontrado para {info['nome']}.")
            continue

        df_final = pl.concat(partes, how="diagonal")
        df_final.write_parquet(out_path, compression="snappy")
        print(f"[SAVE] {out_path.name} — {len(df_final):,} registros")
        outputs[ibge] = out_path

    return outputs


# ---------------------------------------------------------------------------
# Filtro de Estabelecimentos por CNPJ
# ---------------------------------------------------------------------------

def load_cnpjs_from_xlsx(path: str) -> set[str]:
    """
    Lê a primeira coluna de um XLSX e retorna um conjunto de CNPJs
    com 14 dígitos (somente algarismos).
    """
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    cnpjs: set[str] = set()

    for row in ws.iter_rows(values_only=True):
        if row[0] is None:
            continue
        digits = "".join(c for c in str(row[0]) if c.isdigit())
        if len(digits) == 14:
            cnpjs.add(digits)

    wb.close()
    print(f"[CNPJ] {len(cnpjs):,} CNPJs válidos carregados de {path}")
    return cnpjs


def filter_cnpj_estab(
    parquet_dir: Path,
    cnpjs: set[str],
    output_path: Path,
) -> Path:
    """
    Filtra Parquets de ESTABELE pelo CNPJ completo de 14 dígitos
    (CNPJ_BASICO + CNPJ_ORDEM + CNPJ_DV).
    Processa um arquivo por vez para controlar o uso de memória.

    Args:
        parquet_dir: Diretório com os Parquets de ESTABELE
        cnpjs:       Conjunto de CNPJs de 14 dígitos (somente algarismos)
        output_path: Caminho do arquivo de saída

    Retorna: output_path
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    files = _list_parquet_files(parquet_dir)
    partes: list[pl.DataFrame] = []

    for f in files:
        print(f"[FILT] Escaneando {f.name}...")
        df = (
            pl.scan_parquet(str(f))
            .with_columns(
                (
                    pl.col("CNPJ_BASICO").str.zfill(8)
                    + pl.col("CNPJ_ORDEM").str.zfill(4)
                    + pl.col("CNPJ_DV").str.zfill(2)
                ).alias("_CNPJ_FULL")
            )
            .filter(pl.col("_CNPJ_FULL").is_in(cnpjs))
            .drop("_CNPJ_FULL")
            .collect()
        )
        if len(df) > 0:
            partes.append(df)
            print(f"[FILT] → {len(df):,} matches em {f.name}")

    df_final = pl.concat(partes, how="diagonal") if partes else pl.DataFrame()
    df_final.write_parquet(output_path, compression="snappy")
    print(f"[SAVE] {output_path.name} — {len(df_final):,} registros")
    return output_path


# ---------------------------------------------------------------------------
# Filtro de Empresas por CNPJ
# ---------------------------------------------------------------------------

def filter_cnpj_empresa(
    parquet_dir: Path,
    cnpjs: set[str],
    output_path: Path,
) -> Path:
    """
    Filtra Parquets de EMPRE pelo CNPJ_BASICO (8 primeiros dígitos do CNPJ).
    Processa um arquivo por vez para controlar o uso de memória.

    Args:
        parquet_dir: Diretório com os Parquets de EMPRE
        cnpjs:       Conjunto de CNPJs de 14 dígitos (somente algarismos)
        output_path: Caminho do arquivo de saída

    Retorna: output_path
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Garante zeros à esquerda em ambos os lados da comparação
    basicos = {c[:8].zfill(8) for c in cnpjs}
    files = _list_parquet_files(parquet_dir)
    partes: list[pl.DataFrame] = []

    for f in files:
        print(f"[FILT] Escaneando {f.name}...")
        df = (
            pl.scan_parquet(str(f))
            .with_columns(pl.col("CNPJ_BASICO").str.zfill(8).alias("_BASICO_NORM"))
            .filter(pl.col("_BASICO_NORM").is_in(basicos))
            .drop("_BASICO_NORM")
            .collect()
        )
        if len(df) > 0:
            partes.append(df)
            print(f"[FILT] → {len(df):,} matches em {f.name}")

    df_final = pl.concat(partes, how="diagonal") if partes else pl.DataFrame()
    df_final.write_parquet(output_path, compression="snappy")
    print(f"[SAVE] {output_path.name} — {len(df_final):,} registros")
    return output_path
