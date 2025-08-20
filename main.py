#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import math
import re
import time
import random
import requests
import streamlit as st
from typing import List, Optional
from pypdf import PdfReader, PdfWriter  # <- funciona en pypdf >=3 y >=6

# ============== Config de página ==============
st.set_page_config(page_title="ZPL ➜ PDF", page_icon="📦", layout="centered")

# ============== Constantes ==============
LABELARY_URL = "https://api.labelary.com/v1/printers/{dpmm}dpmm/labels/{w}x{h}/"

# ============== Utilidades ZPL ==============
RE_BLOCKS = re.compile(r"(\^XA.*?\^XZ)", flags=re.DOTALL | re.IGNORECASE)

def zpl_split_blocks(zpl_text: str) -> List[str]:
    t = zpl_text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = RE_BLOCKS.findall(t)
    return [b.strip() for b in blocks if b.strip()]

def dpmm_from_dpi(dpi: int) -> int:
    return 24 if dpi >= 600 else 12 if dpi >= 300 else 8  # 203->8

# ============== Cliente Labelary ==============
def call_labelary_pdf(
    blocks: List[str],
    width_in: float,
    height_in: float,
    dpi: int,
    timeout: int = 30,
    max_retries: int = 5,
    rate_delay_s: float = 0.5,
) -> Optional[bytes]:
    """Devuelve un PDF (bytes) para uno o varios bloques ZPL."""
    dpmm = dpmm_from_dpi(dpi)
    url = LABELARY_URL.format(dpmm=dpmm, w=width_in, h=height_in)
    headers = {"Accept": "application/pdf"}
    body = "\n".join(blocks).encode("utf-8")

    time.sleep(rate_delay_s)  # respeta cadencia

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=body, timeout=timeout)
            if resp.status_code == 200:
                return resp.content

            code = resp.status_code
            text = (resp.text or "").strip()
            st.write(f"**HTTP {code}**: {text[:300]}{'…' if len(text) > 300 else ''}")

            if code == 429 or 500 <= code < 600:
                backoff = min(60, (2 ** (attempt - 1))) + random.uniform(0, 0.5 * attempt)
                st.write(f"Reintentando en **{backoff:.1f} s** … (intento {attempt}/{max_retries})")
                time.sleep(backoff)
                continue
            return None  # 4xx duros (400/404/413)

        except requests.RequestException as e:
            st.write(f"**Error de red**: {e}")
            backoff = min(60, (2 ** (attempt - 1))) + random.uniform(0, 0.5 * attempt)
            st.write(f"Reintentando en **{backoff:.1f} s** … (intento {attempt}/{max_retries})")
            time.sleep(backoff)

    return None

# ============== Unir PDFs (pypdf >=3/6) ==============
def merge_pdf_bytes(chunks: List[bytes]) -> io.BytesIO:
    """Une una lista de PDFs (bytes) en un único PDF y devuelve un buffer listo para descargar."""
    writer = PdfWriter()
    for blob in chunks:
        reader = PdfReader(io.BytesIO(blob))
        for page in reader.pages:
            writer.add_page(page)
    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return out

# ============== UI ==============
st.title("📦 ZPL ➜ PDF")
st.caption("Sube un .txt con bloques ^XA…^XZ, elige tamaño y DPI, y descarga un solo PDF.")

with st.sidebar:
    st.header("⚙️ Parámetros")
    width_in  = st.number_input(
        "Ancho (pulgadas)",
        min_value=0.5,
        max_value=15.0,
        value=4.0,
        step=0.1,
        format="%.2f"
    )
    height_in = st.number_input(
        "Alto (pulgadas)",
        min_value=0.5,
        max_value=15.0,
        value=6.0,
        step=0.1,
        format="%.2f"
    )
    dpi = st.selectbox("Resolución (DPI)", [203, 300, 600], index=0)
    st.caption("Tip: muchas guías 4×6 son 203 dpi (8 dpmm). Máx. 50 etiquetas por request.")

# ===== valores fijos =====
CHUNK_START = 10          # tamaño inicial del grupo
TIMEOUT_S   = 30          # timeout HTTP en segundos
RPS         = 2.0         # requests por segundo


uploaded = st.file_uploader("Sube tu archivo .txt con ZPL", type=["txt"])
go = st.button("🚀 Convertir y unir en un solo PDF", disabled=(uploaded is None))

# ============== Lógica principal ==============
if go and uploaded is not None:
    try:
        text = uploaded.read().decode("utf-8", errors="ignore")
        blocks = zpl_split_blocks(text)
        if not blocks:
            st.error("No se detectaron bloques ^XA…^XZ.")
        else:
            total = len(blocks)
            st.info(f"**Total de etiquetas detectadas:** {total}")

            prog = st.progress(0)
            log_box = st.empty()

            merged_chunks: List[bytes] = []
            idx = 0
            group_id = 1
            chunk_size = int(CHUNK_START)
            rate_delay = max(0.0, 1.0 / float(RPS)) if RPS > 0 else 0.0

            # número de grupos estimado (cambia si reducimos chunk_size)
            estimated_groups = math.ceil(total / max(1, chunk_size))
            finished_groups = 0

            while idx < total:
                group = blocks[idx: idx + chunk_size]
                log_box.write(f"➡️ **Grupo #{group_id}** — {len(group)} etiqueta(s) · "
                              f"{width_in}×{height_in} in @ {dpi} dpi")

                pdf_bytes = call_labelary_pdf(
                    group,
                    width_in=width_in,
                    height_in=height_in,
                    dpi=dpi,
                    timeout=TIMEOUT_S,
                    rate_delay_s=rate_delay
                )

                if pdf_bytes:
                    merged_chunks.append(pdf_bytes)
                    finished_groups += 1
                    # progreso (basado en grupos completados)
                    frac = min(1.0, finished_groups / max(estimated_groups, 1))
                    prog.progress(frac)
                    st.success(f"✔ Grupo #{group_id} listo")
                    idx += chunk_size
                    group_id += 1
                    continue

                if chunk_size > 1:
                    chunk_size = max(1, chunk_size // 2)
                    estimated_groups = math.ceil((total - idx) / chunk_size) + (finished_groups)
                    st.warning(f"⚠️ Falló el grupo. Reduciendo chunk_size a **{chunk_size}** y reintentando…")
                    continue

                st.error(f"✗ Falló la etiqueta individual #{idx+1}. Se omitirá.")
                idx += 1  # salta etiqueta problemática

            # Unir todo en un solo PDF
            final_buf = merge_pdf_bytes(merged_chunks)
            st.success("🏁 Proceso terminado. PDF unificado listo.")
            st.download_button(
                label="⬇️ Descargar PDF unificado",
                data=final_buf,
                file_name="Etiquetas.pdf",
                mime="application/pdf"
            )
    except Exception as e:
        st.exception(e)

# with st.expander("ℹ️ Consejos"):
#     st.markdown(
#         """
# - Si tus coordenadas ZPL son ~812×1218 dots, eso es **4×6 in a 203 dpi** (elige 203).
# - Si aparece **HTTP 400/413**, el grupo es muy grande o hay ZPL inválido: el app reduce automáticamente el **chunk_size**.
# - **HTTP 429/5xx**: se reintenta con *backoff*.
# - Labelary devuelve un PDF por request; aquí se **unen** en uno solo con `pypdf`.
#         """
#     )
