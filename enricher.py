# =============================================================================
# enricher.py — Enriquecimento de Estabelecimentos com coordenadas geográficas.
#
# Estratégia em três etapas:
#   1. Merge exato:      CEP + NUMERO (string)
#   2. Merge aproximado: join_asof por número mais próximo dentro do mesmo CEP
#   3. Nominatim:        fallback via API OpenStreetMap para os restantes
# =============================================================================

import time
from pathlib import Path

import polars as pl
import requests


# ---------------------------------------------------------------------------
# Carregamento e preparação das coordenadas IBGE
# ---------------------------------------------------------------------------

def load_coords(
    uf: str,
    coords_dir: Path,
    ibge_codes: list[int] | None = None,
) -> pl.DataFrame:
    """
    Carrega o arquivo coord_{UF}.parquet e filtra pelos municípios de interesse.
    Padroniza CEP e NUM_ENDERECO para os merges.

    Colunas esperadas no arquivo: COD_MUNICIPIO, CEP, NUM_ENDERECO, LATITUDE, LONGITUDE
    """
    path = coords_dir / f"coord_{uf.upper()}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de coordenadas não encontrado: {path}")

    df = pl.read_parquet(path)

    # Valida colunas obrigatórias
    required = {"COD_MUNICIPIO", "CEP", "NUM_ENDERECO", "LATITUDE", "LONGITUDE"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes em {path.name}: {missing}")

    # Filtra municípios de interesse
    if ibge_codes:
        df = df.filter(pl.col("COD_MUNICIPIO").cast(pl.Int64).is_in(ibge_codes))

    # Padroniza e limpa
    df = (
        df
        .with_columns([
            pl.col("CEP").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(8),
            pl.col("NUM_ENDERECO").cast(pl.Utf8).str.replace_all(r"\D", ""),
            pl.col("LATITUDE").cast(pl.Float64, strict=False),
            pl.col("LONGITUDE").cast(pl.Float64, strict=False),
        ])
        .filter(
            pl.col("NUM_ENDERECO").str.len_bytes() > 0,
            pl.col("LATITUDE").is_not_null(),
            pl.col("LONGITUDE").is_not_null(),
        )
        .unique(subset=["CEP", "NUM_ENDERECO"])
        # Adiciona versão numérica do número para o merge aproximado
        .with_columns(
            pl.col("NUM_ENDERECO").cast(pl.Float64, strict=False).alias("_NUM_IBGE")
        )
    )

    print(
        f"[IBGE] {len(df):,} registros de coordenadas carregados "
        f"(UF={uf.upper()}, municípios={ibge_codes or 'todos'})"
    )
    return df


# ---------------------------------------------------------------------------
# Preparação do DataFrame de Estabelecimentos
# ---------------------------------------------------------------------------

def _prepare_estab(df: pl.DataFrame) -> pl.DataFrame:
    """
    Padroniza CEP, NUMERO e monta CNPJ completo.
    Inicializa colunas LATITUDE e LONGITUDE como nulas.
    """
    return df.with_columns([
        pl.col("CEP").str.replace_all(r"\D", "").str.zfill(8),
        pl.col("NUMERO").str.replace_all(r"\D", "").alias("NUMERO"),
        (
            pl.col("CNPJ_BASICO").str.zfill(8)
            + pl.col("CNPJ_ORDEM").str.zfill(4)
            + pl.col("CNPJ_DV").str.zfill(2)
        ).alias("CNPJ"),
        pl.lit(None).cast(pl.Float64).alias("LATITUDE"),
        pl.lit(None).cast(pl.Float64).alias("LONGITUDE"),
    ])


# ---------------------------------------------------------------------------
# Etapa 1 — Merge exato (CEP + NUMERO)
# ---------------------------------------------------------------------------

def _enrich_exact(df: pl.DataFrame, df_coords: pl.DataFrame) -> pl.DataFrame:
    """Preenche coordenadas por correspondência exata de CEP e número."""
    coords = df_coords.select([
        pl.col("CEP"),
        pl.col("NUM_ENDERECO"),
        pl.col("LATITUDE").alias("_LAT"),
        pl.col("LONGITUDE").alias("_LON"),
    ])

    merged = df.join(
        coords,
        left_on=["CEP", "NUMERO"],
        right_on=["CEP", "NUM_ENDERECO"],
        how="left",
    ).with_columns([
        pl.when(pl.col("LATITUDE").is_null())
          .then(pl.col("_LAT"))
          .otherwise(pl.col("LATITUDE"))
          .alias("LATITUDE"),
        pl.when(pl.col("LONGITUDE").is_null())
          .then(pl.col("_LON"))
          .otherwise(pl.col("LONGITUDE"))
          .alias("LONGITUDE"),
    ]).drop(["_LAT", "_LON"])

    matched = merged["LATITUDE"].is_not_null().sum()
    print(f"[IBGE] Merge exato:      {matched:,} coordenadas obtidas")
    return merged


# ---------------------------------------------------------------------------
# Etapa 2 — Merge aproximado via join_asof (número mais próximo no CEP)
# ---------------------------------------------------------------------------

def _enrich_approximate(df: pl.DataFrame, df_coords: pl.DataFrame) -> pl.DataFrame:
    """
    Para registros ainda sem coordenadas, busca o número mais próximo
    dentro do mesmo CEP usando join_asof do Polars (O(n log n)).
    """
    original_cols = df.columns  # preserva schema original

    coords_asof = (
        df_coords
        .filter(pl.col("_NUM_IBGE").is_not_null())
        .select([
            pl.col("CEP").alias("_RCEP"),
            pl.col("_NUM_IBGE"),
            pl.col("LATITUDE").alias("_LAT"),
            pl.col("LONGITUDE").alias("_LON"),
        ])
        .sort(["_RCEP", "_NUM_IBGE"])
    )

    # Adiciona versão numérica do NUMERO para comparação
    df = df.with_columns(
        pl.col("NUMERO").cast(pl.Float64, strict=False).alias("_NUM_EST")
    )

    # Separa registros com número válido dos sem número
    df_with_num = df.filter(pl.col("_NUM_EST").is_not_null()).sort(["CEP", "_NUM_EST"])
    df_no_num   = df.filter(pl.col("_NUM_EST").is_null()).drop("_NUM_EST")

    if df_with_num.is_empty():
        return df.drop("_NUM_EST")

    df_joined = (
        df_with_num
        .join_asof(
            coords_asof,
            left_on="_NUM_EST",
            right_on="_NUM_IBGE",
            by_left="CEP",
            by_right="_RCEP",
            strategy="nearest",
        )
        .with_columns([
            pl.when(pl.col("LATITUDE").is_null())
              .then(pl.col("_LAT"))
              .otherwise(pl.col("LATITUDE"))
              .alias("LATITUDE"),
            pl.when(pl.col("LONGITUDE").is_null())
              .then(pl.col("_LON"))
              .otherwise(pl.col("LONGITUDE"))
              .alias("LONGITUDE"),
        ])
        # Retorna ao schema original (descarta colunas temporárias do join)
        .select(original_cols)
    )

    result = pl.concat(
        [df_joined, df_no_num.select(original_cols)],
        how="vertical",
    )

    matched = result["LATITUDE"].is_not_null().sum()
    print(f"[IBGE] Merge aproximado: {matched:,} coordenadas acumuladas")
    return result


# ---------------------------------------------------------------------------
# Etapa 3 — Nominatim (fallback via API)
# ---------------------------------------------------------------------------

def _build_address(row: dict) -> str:
    """Monta string de endereço para consulta ao Nominatim."""
    parts = [
        row.get("TIPO_LOGRADOURO") or "",
        row.get("LOGRADOURO") or "",
        row.get("NUMERO") or "",
        row.get("BAIRRO") or "",
        row.get("UF") or "",
        "Brasil",
    ]
    return " ".join(p.strip() for p in parts if p.strip())


def _enrich_nominatim(df: pl.DataFrame, rate_limit: float = 1.1) -> pl.DataFrame:
    """
    Geocodifica via Nominatim os registros ainda sem coordenadas.
    Tenta primeiro por endereço completo; se falhar, tenta por NOME_FANTASIA + UF.
    """
    mask_null = pl.col("LATITUDE").is_null()
    df_with  = df.filter(~mask_null)
    df_without = df.filter(mask_null)

    if df_without.is_empty():
        return df

    total = len(df_without)
    print(f"[NOMI] Geocodificando {total:,} registros via Nominatim...")

    headers = {"User-Agent": "RFB-Pipeline/1.0 (github-actions)"}
    lats: list[float | None] = []
    lons: list[float | None] = []

    for i, row in enumerate(df_without.iter_rows(named=True), 1):
        if i % 500 == 0:
            print(f"[NOMI] {i}/{total}...")

        lat, lon = None, None

        # Tentativa 1: endereço completo
        for query in [_build_address(row), row.get("NOME_FANTASIA") or ""]:
            query = query.strip()
            if not query:
                continue
            try:
                resp = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": query, "format": "json", "limit": 1, "countrycodes": "br"},
                    headers=headers,
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                if data:
                    lat_v = float(data[0]["lat"])
                    lon_v = float(data[0]["lon"])
                    # Coordenadas brasileiras são negativas
                    if lat_v < 0 and lon_v < 0:
                        lat, lon = lat_v, lon_v
                        break
            except Exception as exc:
                print(f"[NOMI] Erro: {exc}")

            time.sleep(rate_limit)

        lats.append(lat)
        lons.append(lon)

    df_without = df_without.with_columns([
        pl.Series("LATITUDE",  lats,  dtype=pl.Float64),
        pl.Series("LONGITUDE", lons, dtype=pl.Float64),
    ])

    result = pl.concat([df_with, df_without], how="vertical")
    matched = result["LATITUDE"].is_not_null().sum()
    print(f"[NOMI] Após Nominatim: {matched:,} coordenadas acumuladas")
    return result


# ---------------------------------------------------------------------------
# Função principal de enriquecimento
# ---------------------------------------------------------------------------

def enrich(
    df: pl.DataFrame,
    uf: str,
    coords_dir: Path,
    ibge_codes: list[int] | None = None,
    use_nominatim: bool = False,
) -> pl.DataFrame:
    """
    Pipeline completo de enriquecimento de estabelecimentos com coordenadas.

    Etapas:
      1. Padroniza o DataFrame (CEP, NUMERO, CNPJ)
      2. Merge exato por CEP + NUMERO
      3. Merge aproximado por CEP + número mais próximo
      4. Nominatim (opcional) para os restantes sem coordenadas

    Args:
        df:           DataFrame de estabelecimentos (colunas padrão ESTABELE)
        uf:           Sigla da UF para localizar o arquivo coord_{UF}.parquet
        coords_dir:   Diretório onde estão os arquivos de coordenadas
        ibge_codes:   Filtra o arquivo de coords por municípios específicos
        use_nominatim: Habilita o fallback via API Nominatim

    Retorna DataFrame com colunas CNPJ, LATITUDE, LONGITUDE adicionadas.
    """
    df_coords = load_coords(uf, coords_dir, ibge_codes)
    df = _prepare_estab(df)

    total = len(df)
    print(f"[ENRI] {total:,} estabelecimentos para enriquecer")

    df = _enrich_exact(df, df_coords)
    df = _enrich_approximate(df, df_coords)

    if use_nominatim:
        df = _enrich_nominatim(df)

    with_coords = df["LATITUDE"].is_not_null().sum()
    pct = with_coords / total * 100 if total else 0
    print(f"[ENRI] Resultado: {with_coords:,}/{total:,} ({pct:.1f}%) com coordenadas")

    return df
