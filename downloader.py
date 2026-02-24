# =============================================================================
# downloader.py — Download ZIP → extrai CSV → deleta ZIP → converte Parquet
#                 → deleta CSV. Controla paralelismo via ThreadPoolExecutor.
# =============================================================================

import re
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

from config import BASE_URL, FILE_TYPES


# ---------------------------------------------------------------------------
# Descoberta de meses disponíveis
# ---------------------------------------------------------------------------

def get_available_months() -> list[str]:
    """Retorna lista ordenada dos meses disponíveis no site da RF."""
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    months = [
        a["href"].strip("/")
        for a in soup.find_all("a", href=True)
        if re.match(r"\d{4}-\d{2}/", a["href"])
    ]
    return sorted(months)


def get_latest_month() -> str:
    return get_available_months()[-1]


# ---------------------------------------------------------------------------
# Listagem de arquivos ZIP do mês
# ---------------------------------------------------------------------------

def list_zip_files(month: str, file_type: str) -> list[tuple[str, str]]:
    """
    Retorna lista de (nome_arquivo, url_completa) dos ZIPs do tipo informado.

    Args:
        month:     Mês no formato YYYY-MM (ex: "2025-11")
        file_type: "ESTABELE" ou "EMPRE"
    """
    folder_url = f"{BASE_URL}{month}/"
    resp = requests.get(folder_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    return [
        (a["href"], folder_url + a["href"])
        for a in soup.find_all("a", href=True)
        if a["href"].endswith(".zip") and file_type in a["href"]
    ]


# ---------------------------------------------------------------------------
# Processamento de um único ZIP
# ---------------------------------------------------------------------------

def _process_one_zip(
    filename: str,
    url: str,
    dest_dir: Path,
    columns: list[str],
) -> Path:
    """
    Pipeline completo para um arquivo ZIP:
      1. Download em streaming
      2. Extração do CSV
      3. Deleção do ZIP
      4. Conversão CSV → Parquet (Polars, tudo como string)
      5. Deleção do CSV

    Retorna o caminho do Parquet gerado.
    Pula silenciosamente se o Parquet já existir.
    """
    parquet_path = dest_dir / f"{filename}.parquet"

    if parquet_path.exists():
        print(f"[SKIP] {filename} — parquet já existe.")
        return parquet_path

    # 1. Download
    zip_path = dest_dir / filename
    print(f"[DOWN] Baixando {filename}...")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

    # 2. Extração
    print(f"[EXTR] Extraindo {filename}...")
    with zipfile.ZipFile(zip_path) as z:
        csv_names = z.namelist()
        z.extractall(dest_dir)

    # 3. Deleta ZIP imediatamente para liberar disco
    zip_path.unlink()

    # 4. Conversão CSV → Parquet
    for csv_name in csv_names:
        csv_path = dest_dir / csv_name
        if not csv_path.exists():
            continue

        print(f"[CONV] Convertendo {csv_name} → {parquet_path.name}...")
        df = pl.read_csv(
            csv_path,
            separator=";",
            encoding="latin1",
            has_header=False,
            new_columns=columns,
            infer_schema_length=0,      # tudo como Utf8
            truncate_ragged_lines=True,
            null_values=[""],
        )
        df.write_parquet(parquet_path, compression="snappy")

        # 5. Deleta CSV
        csv_path.unlink()

    print(f"[DONE] {parquet_path.name}")
    return parquet_path


# ---------------------------------------------------------------------------
# Orquestração dos downloads com paralelismo controlado
# ---------------------------------------------------------------------------

def download_all(
    month: str,
    file_type: str,
    dest_dir: Path,
    max_parallel: int = 2,
) -> list[Path]:
    """
    Baixa e converte todos os ZIPs de um tipo para um mês.

    max_parallel controla quantos arquivos são processados simultaneamente,
    evitando esgotar o disco (cada ZIP pode ocupar ~1 GB).

    Retorna lista de caminhos dos Parquets gerados.
    """
    if file_type not in FILE_TYPES:
        raise ValueError(f"file_type deve ser um de: {list(FILE_TYPES)}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    columns = FILE_TYPES[file_type]
    files = list_zip_files(month, file_type)

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo {file_type} encontrado para o mês {month}."
        )

    print(f"[INFO] {len(files)} arquivo(s) {file_type} encontrado(s) para {month}.")

    parquet_paths: list[Path] = []
    errors: list[str] = []

    # max_workers == max_parallel garante que no máximo N arquivos
    # estejam em processamento (e portanto em disco) ao mesmo tempo.
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(_process_one_zip, fn, url, dest_dir, columns): fn
            for fn, url in files
        }
        for future in as_completed(futures):
            fn = futures[future]
            try:
                parquet_paths.append(future.result())
            except Exception as exc:
                errors.append(fn)
                print(f"[ERRO] {fn}: {exc}")

    if errors:
        print(f"[WARN] {len(errors)} arquivo(s) falharam: {errors}")

    return parquet_paths
