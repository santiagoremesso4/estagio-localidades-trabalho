#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from unicodedata import normalize


INE_INDEX_URL = "https://mapas.ine.pt/download/index2021LugaresFregs.phtml"
INE_DOWNLOAD_PREFIX = "https://mapas.ine.pt/download/"
INE_INDEX_CACHE = "ine_lugares_restore.html"


def norm(text: str) -> str:
    text = normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    return re.sub(r"\s+", " ", text).strip()


def strip_parentheses(text: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*", "", text).strip()


def inner_parentheses(text: str) -> str:
    match = re.search(r"\(([^)]*)\)", text)
    return match.group(1).strip() if match else ""


def aliases(text: str, municipality: str) -> set[str]:
    municipality_n = norm(municipality)
    full = norm(text)
    base = norm(strip_parentheses(text))
    inner = norm(inner_parentheses(text))
    alias_set = {full, base, re.sub(r"^uniao das freguesias de\s+", "", full)}
    if inner and inner != municipality_n:
        alias_set.add(inner)
    return {alias for alias in alias_set if alias}


def download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, destination)


def load_json(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, data: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def ensure_index_cache(workdir: Path) -> Path:
    cache_path = workdir / INE_INDEX_CACHE
    if not cache_path.exists():
        download(INE_INDEX_URL, cache_path)
    return cache_path


def parse_municipality_zip_map(index_path: Path) -> dict[str, str]:
    text = html.unescape(index_path.read_text(encoding="utf-8", errors="replace"))
    mapping: dict[str, str] = {}
    pattern = re.compile(
        r"d\.add\([^,]+,[^,]+,'([^']+)'\s*,'(filesGPG/2021localitiesFregs/municipios/[^']+\.zip)'"
    )
    for line in text.splitlines():
        match = pattern.search(line)
        if match:
            mapping[norm(match.group(1).strip())] = match.group(2)
    return mapping


def ensure_geopackage(workdir: Path, zip_relative_path: str) -> Path:
    zip_name = Path(zip_relative_path).name
    zip_path = workdir / zip_name
    extract_dir = workdir / zip_name.replace(".zip", "")

    if not extract_dir.exists():
        if not zip_path.exists():
            download(INE_DOWNLOAD_PREFIX + zip_relative_path, zip_path)
        with zipfile.ZipFile(zip_path) as zipped:
            zipped.extractall(extract_dir)

    gpkg_files = list(extract_dir.rglob("*.gpkg"))
    if not gpkg_files:
        raise FileNotFoundError(f"Nenhum .gpkg encontrado em {extract_dir}")
    return gpkg_files[0]


def load_localities_from_gpkg(gpkg_path: Path) -> tuple[dict[str, list[str]], list[str]]:
    table_name = gpkg_path.stem
    con = sqlite3.connect(gpkg_path)
    try:
        cur = con.cursor()
        rows = cur.execute(
            f'SELECT FR_DSG, LG_DSG, LG_COD FROM "{table_name}" ORDER BY CAST(LG_COD AS INTEGER)'
        ).fetchall()
    finally:
        con.close()

    grouped: dict[str, list[str]] = {}
    seen_localities: dict[str, set[str]] = {}
    parish_names: list[str] = []

    for parish_name, locality_name, _ in rows:
        parish_key = norm(parish_name)
        grouped.setdefault(parish_key, [])
        seen_localities.setdefault(parish_key, set())
        parish_names.append(parish_name)

        locality_key = norm(locality_name)
        if locality_key in seen_localities[parish_key]:
            continue

        seen_localities[parish_key].add(locality_key)
        grouped[parish_key].append(locality_name)

    return grouped, sorted(set(parish_names))


@dataclass
class Stats:
    updated: int = 0
    skipped_existing: int = 0
    unmatched: int = 0


MANUAL_MUNICIPALITY_LOOKUP = {
    "calheta": "calheta (r.a.m.)",
    "lagoa (ilha de sao miguel)": "lagoa (r.a.a.)",
    "calheta (ilha de sao jorge)": "calheta (r.a.a.)",
}


MANUAL_PARISH_LOOKUP = {
    ("viseu", "coutos de viseu"): "uniao das freguesias de couto de baixo e couto de cima",
    ("viseu", "viseu"): "uniao das freguesias de viseu",
    ("funchal", "imaculado coracao maria"): "imaculado coracao de maria",
    ("calheta (ilha de sao jorge)", "ribeira seca (calheta)"): "ribeira seca",
}


def iter_parishes(data: list[dict]) -> Iterable[tuple[dict, dict, dict]]:
    for district in data:
        for municipality in district.get("concelhos", []):
            for parish in municipality.get("freguesias", []):
                yield district, municipality, parish


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preenche automaticamente as localidades das freguesias usando os dados oficiais do INE."
    )
    parser.add_argument(
        "--input",
        default="distritos.json",
        help="Caminho para o ficheiro JSON a atualizar. Default: distritos.json",
    )
    parser.add_argument(
        "--output",
        help="Caminho de saída. Se omitido, escreve por cima do ficheiro de entrada.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Substitui localidades já existentes. Por omissão, só preenche freguesias sem localidades.",
    )
    parser.add_argument(
        "--keep-zips",
        action="store_true",
        help="Mantém os ZIPs e pastas descarregados na pasta do script.",
    )
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"Ficheiro não encontrado: {input_path}", file=sys.stderr)
        return 1

    output_path = Path(args.output).resolve() if args.output else input_path
    workdir = input_path.parent

    data = load_json(input_path)
    index_path = ensure_index_cache(workdir)
    municipality_zip_map = parse_municipality_zip_map(index_path)

    municipality_cache: dict[str, tuple[dict[str, list[str]], list[str]]] = {}
    stats = Stats()
    unmatched_rows: list[tuple[str, str, str]] = []

    for district, municipality, parish in iter_parishes(data):
        if parish.get("localidades") and not args.replace_existing:
            stats.skipped_existing += 1
            continue

        municipality_name = municipality["name"]
        municipality_key = norm(municipality_name)
        lookup_key = MANUAL_MUNICIPALITY_LOOKUP.get(municipality_key, municipality_key)
        zip_relative_path = municipality_zip_map.get(lookup_key)
        if not zip_relative_path:
            stats.unmatched += 1
            unmatched_rows.append((district["name"], municipality_name, parish["name"]))
            continue

        if municipality_name not in municipality_cache:
            gpkg_path = ensure_geopackage(workdir, zip_relative_path)
            municipality_cache[municipality_name] = load_localities_from_gpkg(gpkg_path)

        grouped, source_parish_names = municipality_cache[municipality_name]
        parish_key = norm(parish["name"])
        chosen_key: str | None = None

        if parish_key in grouped:
            chosen_key = parish_key
        else:
            manual_key = MANUAL_PARISH_LOOKUP.get((municipality_key, parish_key))
            if manual_key in grouped:
                chosen_key = manual_key
            else:
                candidates: list[str] = []
                for source_name in source_parish_names:
                    if aliases(parish["name"], municipality_name) & aliases(source_name, municipality_name):
                        candidates.append(norm(source_name))
                candidates = list(dict.fromkeys(candidates))
                if len(candidates) == 1:
                    chosen_key = candidates[0]

        if chosen_key is None:
            stats.unmatched += 1
            unmatched_rows.append((district["name"], municipality_name, parish["name"]))
            continue

        parish["localidades"] = [{"name": locality} for locality in grouped[chosen_key]]
        stats.updated += 1

    save_json(output_path, data)

    if not args.keep_zips:
        for path in workdir.glob("C21_LUGF*.zip"):
            path.unlink(missing_ok=True)
        for path in workdir.glob("C21_LUGF*"):
            if path.is_dir():
                for child in sorted(path.rglob("*"), reverse=True):
                    if child.is_file():
                        child.unlink(missing_ok=True)
                    elif child.is_dir():
                        child.rmdir()
                path.rmdir()

    print(f"Atualizadas: {stats.updated}")
    print(f"Ignoradas por já terem localidades: {stats.skipped_existing}")
    print(f"Sem correspondência: {stats.unmatched}")
    if unmatched_rows:
        print("\nFreguesias sem correspondência:")
        for district_name, municipality_name, parish_name in unmatched_rows:
            print(f"- {district_name} / {municipality_name} / {parish_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
