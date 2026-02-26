# =============================================================================
# downloader.py — Download ZIP → extrai CSV → deleta ZIP → converte Parquet
#                 → deleta CSV. Controla paralelismo via ThreadPoolExecutor.
#
# A Receita Federal migrou para Nextcloud. A API utilizada é WebDAV sobre
# share público, sem necessidade de login:
#   PROPFIND  https://host/public.php/webdav/{path}   → lista arquivos
#   GET       https://host/public.php/webdav/{path}   → baixa arquivo
#   Auth: HTTPBasicAuth(SHARE_TOKEN, "")
# =============================================================================

import re
import subprocess
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests
from requests.auth import HTTPBasicAuth

from config import COLS_ESTABELECIMENTO, COLS_ESTABELECIMENTO_RAW, FILE_TYPES

# ---------------------------------------------------------------------------
# Configuração da nova plataforma Nextcloud
# ---------------------------------------------------------------------------

NEXTCLOUD_HOST  = "https://arquivos.receitafederal.gov.br"
SHARE_TOKEN     = "gn672Ad4CF8N6TK"
SHARE_BASE_PATH = "/Dados/Cadastros/CNPJ"
WEBDAV_BASE     = f"{NEXTCLOUD_HOST}/public.php/webdav"
AUTH            = HTTPBasicAuth(SHARE_TOKEN, "")

# Namespace WebDAV usado nas respostas XML
_DAV_NS = {"d": "DAV:"}

# Número de tentativas para download e espera entre elas
_DOWNLOAD_RETRIES = 3
_RETRY_WAIT_SECS  = 30


# ---------------------------------------------------------------------------
# WebDAV helpers
# ---------------------------------------------------------------------------

def _propfind(path: str) -> list[dict]:
    """
    Faz PROPFIND no path relativo ao share e retorna lista de entradas.
    Cada entrada é um dict com: href, name, is_dir, size.
    """
    url = f"{WEBDAV_BASE}{path}"
    resp = requests.request(
        "PROPFIND",
        url,
        auth=AUTH,
        headers={"Depth": "1"},
        timeout=30,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    entries = []

    for response in root.findall("d:response", _DAV_NS):
        href = response.findtext("d:href", default="", namespaces=_DAV_NS)
        clean_href = href.replace("/public.php/webdav", "")
        name = clean_href.rstrip("/").split("/")[-1]
        is_dir = response.find(".//d:collection", _DAV_NS) is not None
        size_text = response.findtext(".//d:getcontentlength", default="0", namespaces=_DAV_NS)

        # Ignora a própria pasta (entrada raiz do PROPFIND)
        if clean_href.rstrip("/") == path.rstrip("/"):
            continue

        entries.append({
            "href": clean_href,
            "name": name,
            "is_dir": is_dir,
            "size": int(size_text or 0),
        })

    return entries


# ---------------------------------------------------------------------------
# Descoberta de meses disponíveis
# ---------------------------------------------------------------------------

def get_available_months() -> list[str]:
    """Retorna lista ordenada dos meses disponíveis (ex: ['2025-10','2025-11'])."""
    entries = _propfind(SHARE_BASE_PATH)
    months = sorted(
        e["name"] for e in entries
        if e["is_dir"] and re.match(r"\d{4}-\d{2}$", e["name"])
    )
    return months


def get_latest_month() -> str:
    return get_available_months()[-1]


# ---------------------------------------------------------------------------
# Listagem de arquivos ZIP do mês
# ---------------------------------------------------------------------------

def list_zip_files(month: str, file_type: str) -> list[tuple[str, str]]:
    """
    Retorna lista de (nome_arquivo, url_download) dos ZIPs do tipo informado.

    Args:
        month:     "2025-11"
        file_type: "ESTABELE" ou "EMPRE"
    """
    path = f"{SHARE_BASE_PATH}/{month}"
    entries = _propfind(path)
    result = []
    for e in entries:
        name = e["name"]
        if name.lower().endswith(".zip") and file_type.upper() in name.upper():
            url = f"{WEBDAV_BASE}{e['href']}"
            result.append((name, url))
    return result


# ---------------------------------------------------------------------------
# Download com retry
# ---------------------------------------------------------------------------

def _download_with_retry(filename: str, url: str, zip_path: Path) -> None:
    """
    Baixa um arquivo ZIP com até _DOWNLOAD_RETRIES tentativas.
    Remove arquivo parcial antes de cada nova tentativa.
    """
    for attempt in range(1, _DOWNLOAD_RETRIES + 1):
        try:
            print(f"[DOWN] Baixando {filename} (tentativa {attempt}/{_DOWNLOAD_RETRIES})...")
            with requests.get(url, auth=AUTH, stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
            return  # sucesso
        except Exception as exc:
            zip_path.unlink(missing_ok=True)
            if attempt < _DOWNLOAD_RETRIES:
                print(f"[RETRY] {filename} falhou ({exc}). Aguardando {_RETRY_WAIT_SECS}s...")
                time.sleep(_RETRY_WAIT_SECS)
            else:
                raise


# ---------------------------------------------------------------------------
# Conversão de encoding latin1 → utf8 via iconv (streaming, sem RAM extra)
# ---------------------------------------------------------------------------

def _latin1_to_utf8(src: Path, dst: Path) -> None:
    """
    Converte o arquivo src de latin1 para utf8 usando iconv.
    Processa em streaming — não carrega o arquivo na RAM.
    O arquivo src é deletado após a conversão.
    """
    print(f"[ENCO] Convertendo encoding {src.name} → utf8...")
    subprocess.run(
        ["iconv", "-f", "latin1", "-t", "utf-8", str(src), "-o", str(dst)],
        check=True,
    )
    src.unlink()


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
      1. Download em streaming com retry
      2. Extração do CSV (latin1)
      3. Deleção do ZIP
      4. iconv: latin1 → utf8  (streaming, sem custo de RAM)
      5. Deleção do CSV latin1
      6. scan_csv utf8 → filtra → Parquet  (streaming via LazyFrame)
      7. Deleção do CSV utf8

    Retorna o caminho do Parquet gerado.
    Pula silenciosamente se o Parquet já existir.
    """
    parquet_path = dest_dir / f"{filename}.parquet"

    if parquet_path.exists():
        print(f"[SKIP] {filename} — parquet já existe.")
        return parquet_path

    # 1. Download com retry
    zip_path = dest_dir / filename
    _download_with_retry(filename, url, zip_path)

    # 2. Extração
    print(f"[EXTR] Extraindo {filename}...")
    with zipfile.ZipFile(zip_path) as z:
        csv_names = z.namelist()
        z.extractall(dest_dir)

    # 3. Deleta ZIP imediatamente para liberar disco
    zip_path.unlink()

    is_estabele = "ESTABELE" in filename.upper()

    for csv_name in csv_names:
        csv_path = dest_dir / csv_name
        if not csv_path.exists():
            continue

        # 4. Converte latin1 → utf8 via iconv (streaming)
        utf8_path = csv_path.with_suffix(".utf8.csv")
        _latin1_to_utf8(csv_path, utf8_path)
        # csv_path (latin1) já foi deletado dentro de _latin1_to_utf8

        print(f"[CONV] Convertendo {csv_name} → {parquet_path.name}...")

        # 5. scan_csv em streaming (utf8, sem carregar tudo na RAM)
        if is_estabele:
            df = (
                pl.scan_csv(
                    utf8_path,
                    separator=";",
                    has_header=False,
                    new_columns=COLS_ESTABELECIMENTO_RAW,
                    infer_schema_length=0,
                    truncate_ragged_lines=True,
                    null_values=[""],
                )
                .filter(pl.col("SITUACAO_CADASTRAL") == "02")
                .select(COLS_ESTABELECIMENTO)
                .collect()
            )
            print(f"[FILT] Ativos retidos: {len(df):,}")
        else:
            df = (
                pl.scan_csv(
                    utf8_path,
                    separator=";",
                    has_header=False,
                    new_columns=columns,
                    infer_schema_length=0,
                    truncate_ragged_lines=True,
                    null_values=[""],
                )
                .collect()
            )

        df.write_parquet(parquet_path, compression="snappy")

        # 6. Deleta CSV utf8
        utf8_path.unlink()

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
