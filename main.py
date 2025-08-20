#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import math
import re
import time
import random
import requests
import streamlit as st
from typing import List, Optional, Tuple, Dict, Any
from pypdf import PdfReader, PdfWriter  # pypdf >=3

st.set_page_config(page_title="ZPL ➜ PDF unificado (Labelary)", page_icon="📦", layout="centered")

LABELARY_URL = "https://api.labelary.com/v1/printers/{dpmm}dpmm/labels/{w}x{h}/"

# ---------- Regex ----------
RE_BLOCKS = re.compile(r"(\^XA.*?\^XZ)", flags=re.DOTALL | re.IGNORECASE)
RE_PQ     = re.compile(r"\^PQ\s*([0-9]+)", flags=re.IGNORECASE)
RE_JSON_ID = re.compile(r'"id"\s*:\s*"([^"]+)"')
RE_FD     = re.compile(r"\^FD(.*?)\^FS", re.DOTALL | re.IGNORECASE)

def zpl_split_blocks(zpl_text: str) -> List[str]:
    t = zpl_text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = RE_BLOCKS.findall(t)
    return [b.strip() for b in blocks if b.strip()]

def dpmm_from_dpi(dpi: int) -> int:
    return 24 if dpi >= 600 else 12 if dpi >= 300 else 8  # 203->8

def parse_pq(block: str) -> int:
    """Devuelve el número de copias (PQ). Si no hay ^PQ, es 1."""
    m = RE_PQ.search(block)
    try:
        return int(m.group(1)) if m else 1
    except Exception:
        return 1

def set_pq(block: str, new_pq: int) -> str:
    """Fuerza ^PQ a new_pq. Si existe, lo reemplaza; si no, lo inserta antes de ^XZ."""
    if RE_PQ.search(block):
        return RE_PQ.sub(f"^PQ{new_pq}", block, count=1)
    # Insertar antes del último ^XZ
    return re.sub(r"\^XZ\s*$", f"^PQ{new_pq}\n^XZ", block, flags=re.IGNORECASE)

def describe_block(block: str, idx: int, pq: int) -> str:
    """Extrae algo legible para log: id del QR o primer FD."""
    ident = None
    m = RE_JSON_ID.search(block)
    if m:
        ident = m.group(1)
    else:
        fd = RE_FD.search(block)
        if fd:
            txt = re.sub(r"\s+", " ", fd.group(1)).strip()
            ident = (txt[:60] + "…") if len(txt) > 60 else txt
    base = f"#{idx+1} (PQ={pq})"
    return f"{base} — {ident}" if ident else base

# ---------- Llamada a Labelary ----------
def call_labelary_pdf(
    blocks: List[str],
    width_in: float,
    height_in: float,
    dpi: int,
    timeout: int = 30,
    max_retries: int = 5,
    rate_delay_s: float = 0.5,
) -> Tuple[Optional[bytes], Optional[str], Optional[int]]:
    """
    Devuelve (pdf_bytes, error_text, http_code).
    """
    dpmm = dpmm_from_dpi(dpi)
    url = LABELARY_URL.format(dpmm=dpmm, w=width_in, h=height_in)
    headers = {"Accept": "application/pdf"}
    body = "\n".join(blocks).encode("utf-8")

    time.sleep(rate_delay_s)

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=body, timeout=timeout)
            if resp.status_code == 200:
                return resp.content, None, None

            code = resp.status_code
            text = (resp.text or "").strip()
            if code == 429 or 500 <= code < 600:
                backoff = min(60, (2 ** (attempt - 1))) + random.uniform(0, 0.5 * attempt)
                st.write(f"HTTP {code}. Reintentando en **{backoff:.1f}s** (intento {attempt}/{max_retries})…")
                time.sleep(backoff)
                continue
            return None, text, code  # 4xx/otros: devolvemos error “duro”

        except requests.RequestException as e:
            backoff = min(60, (2 ** (attempt - 1))) + random.uniform(0, 0.5 * attempt)
            st.write(f"Error de red: {e}. Reintentando en **{backoff:.1f}s** (intento {attempt}/{max_retries})…")
            time.sleep(backoff)

    return None, "Max retries exceeded", -1

# ---------- Unir PDFs ----------
def merge_pdf_bytes(chunks: List[bytes]) -> io.BytesIO:
    writer = PdfWriter()
    for blob in chunks:
        reader = PdfReader(io.BytesIO(blob))
        for page in reader.pages:
            writer.add_page(page)
    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return out

# ---------- Lógica de empaquetado respetando el límite 50 ----------
def build_requests_from_blocks(blocks: List[str]) -> List[List[str]]:
    """
    Crea una lista de “requests”, cada request es una lista de bloques ZPL.
    - Respeta el límite de **50 etiquetas reales** (sumando ^PQ de cada bloque).
    - Si un bloque tiene ^PQ > 50, lo parte en varios sub-bloques con ^PQ ajustado.
    """
    reqs: List[List[str]] = []
    current: List[str] = []
    current_count = 0  # etiquetas reales del request actual

    for b in blocks:
        pq = parse_pq(b)
        if pq <= 50:
            # ¿cabe entero?
            if current_count + pq <= 50:
                current.append(b)
                current_count += pq
            else:
                # cerrar actual y abrir nuevo
                if current:
                    reqs.append(current)
                current = [b]
                current_count = pq
        else:
            # partir en trozos de 50
            remaining = pq
            while remaining > 0:
                take = min(50, remaining)
                b_piece = set_pq(b, take)
                if current_count + take <= 50 and current:
                    current.append(b_piece)
                    current_count += take
                else:
                    if current:
                        reqs.append(current)
                    current = [b_piece]
                    current_count = take
                remaining -= take

    if current:
        reqs.append(current)
    return reqs

# ---------- UI ----------
st.title("📦 ZPL ➜ PDF unificado (Labelary)")
st.caption("Evita el 413 agrupando por ≤50 etiquetas por request (cuenta ^PQ).")

with st.sidebar:
    st.header("⚙️ Parámetros")
    width_in  = st.number_input("Ancho (pulgadas)", min_value=0.5, max_value=15.0, value=4.0, step=0.1, format="%.2f")
    height_in = st.number_input("Alto (pulgadas)",  min_value=0.5, max_value=15.0, value=6.0, step=0.1, format="%.2f")
    dpi       = st.selectbox("Resolución (DPI)", [203, 300, 600], index=0)
    st.caption("La API limita a **50 etiquetas** por request (incluye duplicados por ^PQ).")

# Valores fijos (los escondemos de la UI)
CHUNK_RPS   = 2.0     # req/seg
TIMEOUT_S   = 30
RATE_DELAY  = 1.0 / CHUNK_RPS

uploaded = st.file_uploader("Sube tu archivo .txt con ZPL", type=["txt"])
go = st.button("🚀 Convertir y unir en un solo PDF", disabled=(uploaded is None))

if go and uploaded is not None:
    try:
        text = uploaded.read().decode("utf-8", errors="ignore")
        blocks = zpl_split_blocks(text)
        if not blocks:
            st.error("No se detectaron bloques ^XA…^XZ.")
        else:
            # Datos de bloques para log
            block_infos = [(i, parse_pq(b), describe_block(b, i, parse_pq(b))) for i, b in enumerate(blocks)]
            total_etiquetas = sum(pq for _, pq, _ in block_infos)
            st.info(f"Detectados **{len(blocks)}** bloques, **{total_etiquetas}** etiqueta(s) reales considerando ^PQ.")

            # Construir requests seguros
            requests_list = build_requests_from_blocks(blocks)
            st.write(f"Se generarán **{len(requests_list)}** request(s) (máx 50 etiquetas cada uno).")

            prog = st.progress(0)
            merged: List[bytes] = []
            failed: List[Dict[str, Any]] = []

            for gi, req_blocks in enumerate(requests_list, start=1):
                # calcular conteo real del grupo
                pq_sum = sum(parse_pq(b) for b in req_blocks)
                st.write(f"➡️ **Grupo #{gi}** — {len(req_blocks)} bloque(s), **{pq_sum}** etiqueta(s)")

                pdf_bytes, err_txt, err_code = call_labelary_pdf(
                    req_blocks, width_in=width_in, height_in=height_in, dpi=dpi,
                    timeout=TIMEOUT_S, rate_delay_s=RATE_DELAY
                )

                if pdf_bytes:
                    merged.append(pdf_bytes)
                    st.success(f"✔ Grupo #{gi} listo")
                else:
                    st.error(f"✗ Grupo #{gi} falló (HTTP {err_code}). {err_txt[:200] if err_txt else ''}")
                    # Loggear los bloques del grupo
                    for b in req_blocks:
                        idx = blocks.index(b) if b in blocks else -1
                        pq = parse_pq(b)
                        failed.append({
                            "index": idx+1 if idx >= 0 else None,
                            "pq": pq,
                            "desc": describe_block(b, idx if idx>=0 else 0, pq),
                            "group": gi,
                            "http": err_code,
                            "err": (err_txt or "")[:500]
                        })

                prog.progress(gi / max(1, len(requests_list)))

            if not merged:
                st.error("No se pudo generar ningún PDF.")
                if failed:
                    with st.expander("Ver fallos"):
                        for f in failed:
                            st.write(f"- Bloque {f['desc']} | Grupo {f['group']} | HTTP {f['http']} | {f['err']}")
                st.stop()

            final_buf = merge_pdf_bytes(merged)
            st.success("🏁 Proceso terminado. PDF unificado listo.")
            st.download_button(
                label="⬇️ Descargar PDF unificado",
                data=final_buf,
                file_name="labels_unificado.pdf",
                mime="application/pdf"
            )

            # Reporte de fallos detallado (si hubo)
            if failed:
                st.warning(f"Algunos bloques fallaron: {len(failed)}")
                with st.expander("🔎 Detalle de bloques fallidos"):
                    for f in failed:
                        st.write(f"- Bloque {f['desc']} | Grupo {f['group']} | HTTP {f['http']} | {f['err']}")

            # Resumen por bloque (opcional)
            with st.expander("📋 Resumen por bloque (PQ e identificador)"):
                for i, pq, desc in block_infos:
                    st.write(f"- {desc}")

    except Exception as e:
        st.exception(e)
