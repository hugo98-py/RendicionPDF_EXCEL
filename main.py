# -*- coding: utf-8 -*-
"""
Created on Sun Aug 17 19:40:11 2025

@author: Hugo
"""

# app.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, json, base64, traceback, unicodedata
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote
from typing import Any, List, Tuple, Optional, Union, Dict
from datetime import datetime
from copy import copy
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage as gcs

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.styles import Alignment, Border, Font, Side

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor, white
from PIL import Image as PILImage, ImageOps

from docx import Document
from docx.enum.table import WD_ALIGN_VERTICAL, WD_ROW_HEIGHT_RULE, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Emu, Pt, RGBColor

# ───────────────────────────────── CONFIG ──────────────────────────────────
# Template en el root del repo (override con RENDIDOR_TEMPLATE_PATH)
TEMPLATE_XLSX_DEFAULT = Path(
    os.getenv("RENDIDOR_TEMPLATE_PATH", Path(__file__).parent / "FormatoRendicion.xlsx")
).resolve()

# Salida a /tmp por defecto (filesystem efímero de Render)
OUTPUT_DIR = Path(os.getenv("RENDIDOR_OUTPUT_DIR", "/tmp/rendidor")).resolve()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def default_fotos_dir() -> Path:
    p = OUTPUT_DIR / "fotos_rendicion"
    p.mkdir(parents=True, exist_ok=True)
    return p

DEFAULT_NAME_CAMPANA = "—"
DEFAULT_NAME_PERSONA = "—"

# Colección de campañas y campo interno que guarda el ID que llega a la API
CAMPANA_COLLECTION = os.getenv("RENDIDOR_CAMPANA_COLLECTION", "campana")
CAMPANA_ID_FIELD   = os.getenv("RENDIDOR_CAMPANA_ID_FIELD", "campanaID")
TZ_CHILE = ZoneInfo("America/Santiago")

# Logo para la portada del PDF, en el root del repo (override con RENDIDOR_LOGO_PATH)
LOGO_PATH = Path(os.getenv("RENDIDOR_LOGO_PATH", Path(__file__).parent / "logo_ams.png")).resolve()

# Paleta AMS (tomada del logo)
# Campos de "registros" donde puede venir el tipo de comprobante (boleta / factura)
TIPO_COMPROBANTE_FIELDS = ("tipoComprobante", "tipo", "comprobante", "tipoDocumento", "tipoDoc", "tipoBoleta")

AMS_GRIS, AMS_VERDE, AMS_AMARILLO, AMS_AZUL = "#58595B", "#7AB573", "#F2DA00", "#84BCE4"

# Word: página A4 con márgenes de 1,5 cm
MARGEN_DOCX_CM = 1.5
ANCHO_UTIL_CM  = 21.0 - 2 * MARGEN_DOCX_CM
ALTO_UTIL_CM   = 29.7 - 2 * MARGEN_DOCX_CM

MESES_ES = [
    "enero","febrero","marzo","abril","mayo","junio",
    "julio","agosto","septiembre","octubre","noviembre","diciembre"
]

# ─────────────────────────────── FastAPI init ───────────────────────────────
app = FastAPI(title="Rendidor API", version="1.1.0")

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"]
)

# ───────────────────────── Firestore / Storage init ─────────────────────────
def _get_sa_info() -> dict:
    b64 = os.getenv("FIREBASE_KEY_B64", "").strip()
    if not b64:
        raise RuntimeError(
            "FIREBASE_KEY_B64 no está definida. Sube tu service-account.json como base64 en esta variable."
        )
    try:
        return json.loads(base64.b64decode(b64))
    except Exception as e:
        raise RuntimeError(f"FIREBASE_KEY_B64 inválida: {e}")

def init_clients() -> Tuple[firestore.Client, gcs.Client]:
    sa = _get_sa_info()
    cred = credentials.Certificate(sa)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    fs_client = firestore.client()
    gcs_client = gcs.Client.from_service_account_info(sa)
    return fs_client, gcs_client

def _unique_path(path: Path) -> Path:
    path = Path(path)
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    i = 1
    while True:
        candidate = path.with_name(f"{stem} ({i}){suffix}")
        if not candidate.exists():
            return candidate
        i += 1

# ───────────────────────────── Helpers comunes ──────────────────────────────
def _extract_urls_from_fotos(fotos: Any) -> List[str]:
    urls: List[str] = []
    if not fotos: return urls
    def _add(x: Any):
        if not x: return
        if isinstance(x, str):
            urls.append(x.strip())
        elif isinstance(x, dict):
            for k in ("url", "href", "downloadURL", "path"):
                if k in x and isinstance(x[k], str):
                    urls.append(x[k].strip())
    if isinstance(fotos, list):
        for item in fotos: _add(item)
    else:
        _add(fotos)
    return list(dict.fromkeys(urls))

def _safe_ext_from_url(u: str, default=".jpg") -> str:
    try:
        name = unquote(Path(urlparse(u).path).name)
        if "." in name:
            return "." + name.split(".")[-1].split("?")[0][:8]
    except Exception:
        pass
    return default

def _safe_filename(base: str, idx: int, url: str) -> str:
    ext = _safe_ext_from_url(url)
    base = re.sub(r"[^a-zA-Z0-9_\-\.]+", "_", base)[:64]
    return f"{base}_{idx:03d}{ext}"

def _download_http(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk: f.write(chunk)

def _download_gs(gcs_client: gcs.Client, gs_url: str, dest: Path) -> None:
    path = gs_url[5:]
    bucket_name, _, blob_path = path.partition("/")
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))

def _is_gs(u: str) -> bool:
    return u.startswith("gs://")

def _parse_us_dt(x: Any) -> Optional[datetime]:
    if x is None: return None
    if isinstance(x, datetime): return x
    if isinstance(x, str):
        s = x.strip()
        for fmt in ("%m-%d-%Y %H:%M:%S", "%m-%d-%Y %H:%M", "%m-%d-%Y"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None

def _to_ddmmyyyy(dt: datetime) -> str:
    return dt.strftime("%d-%m-%Y")

def _clean_monto(x: Any) -> float:
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def mes_es(dt: datetime) -> str:
    return MESES_ES[dt.month - 1]

def _fmt_clp(x: Any) -> str:
    v = int(round(_clean_monto(x)))
    s = f"{abs(v):,}".replace(",", ".")
    return f"-$ {s}" if v < 0 else f"$ {s}"

def fetch_campana(campana_id: Optional[str], collection_name: str = CAMPANA_COLLECTION) -> dict:
    """Lee el documento de la campaña. Busca por el campo interno CAMPANA_ID_FIELD
    y, si no encuentra nada, prueba con campana_id como ID del documento."""
    if not campana_id:
        return {}
    fs, _ = init_clients()
    col = fs.collection(collection_name)
    for snap in col.where(CAMPANA_ID_FIELD, "==", campana_id).limit(1).stream():
        return snap.to_dict() or {}
    snap = col.document(campana_id).get()
    if snap.exists:
        return snap.to_dict() or {}
    print(f"[rendidor] campaña no encontrada: {CAMPANA_ID_FIELD}={campana_id}")
    return {}

def _slug_categoria(cat: str) -> str:
    """'Alimentación' -> 'Alimentacion' (para armar monto<Categoria>Total)."""
    s = unicodedata.normalize("NFKD", str(cat)).encode("ascii", "ignore").decode()
    return "".join(p[:1].upper() + p[1:] for p in re.split(r"[^A-Za-z0-9]+", s) if p)

def montos_por_categoria(camp: dict) -> Dict[str, float]:
    """Lee los campos monto<Categoria>Total de la campaña -> {'alimentacion': 50000, ...}."""
    out: Dict[str, float] = {}
    for k, v in (camp or {}).items():
        m = re.fullmatch(r"monto(.+)Total", str(k))
        if m and v is not None:
            out[m.group(1).lower()] = _clean_monto(v)
    return out

def saldos_por_categoria(camp: dict) -> Dict[str, float]:
    """Lee los campos saldoDisponible<Categoria> -> {'alimentacion': 30000, ..., 'total': 120000}."""
    out: Dict[str, float] = {}
    for k, v in (camp or {}).items():
        m = re.fullmatch(r"saldoDisponible(.+)", str(k))
        if m and v is not None:
            out[m.group(1).lower()] = _clean_monto(v)
    return out

def _buscar_categoria(d: Dict[str, float], cat: str) -> Optional[float]:
    """Busca la categoría tolerando singular/plural (Peaje ↔ saldoDisponiblePeajes)."""
    key = _slug_categoria(cat).lower()
    if key in d:
        return d[key]
    for k, v in d.items():
        if k != "total" and k.rstrip("s") == key.rstrip("s"):
            return v
    return None

def resumen_por_categoria(registros: List[dict]) -> List[Tuple[str, int, float]]:
    """[(categoria, n_boletas, monto)] solo de las categorías con registros."""
    acc: Dict[str, List[float]] = {}
    for r in registros:
        cat = (str(r.get("categoria") or "").strip()) or "Sin categoría"
        a = acc.setdefault(cat, [0, 0.0])
        a[0] += 1
        a[1] += float(r.get("Monto") or 0.0)
    return sorted(((k, int(v[0]), v[1]) for k, v in acc.items()), key=lambda t: (-t[2], t[0]))

# ───────────── Firestore lectura / normalización ─────────────
def fetch_registros(
    collection_name: str = "registros",
    fotos_field: str = "foto",
    campana_id: Optional[str] = None,
) -> Tuple[List[dict], Dict[str, Any]]:
    fs, _ = init_clients()
    registros: List[dict] = []
    nameCampana = None; namePersona = None; startDC = None; endDC = None
    montoTotal = 0.0

    q = fs.collection(collection_name)
    if campana_id:
        q = q.where("campanaID", "==", campana_id)

    for doc in q.stream():
        data = doc.to_dict() or {}
        d_dt = _parse_us_dt(data.get("date")) or _parse_us_dt(data.get("Fecha"))
        cat = data.get("categoria") or data.get("Detalle") or ""
        idb = data.get("idBoleta") or data.get("Boleta") or ""
        mon = _clean_monto(data.get("monto") or data.get("Gasto"))
        fotos = _extract_urls_from_fotos(data.get(fotos_field))
        tipo = next((str(data[k]) for k in TIPO_COMPROBANTE_FIELDS if data.get(k)), "")
        registros.append({
            "doc_id": doc.id, "date": d_dt, "categoria": cat,
            "idBoleta": idb, "Monto": mon, "fotos": fotos,
            "esFactura": "factura" in tipo.lower(),
        })
        if nameCampana is None: nameCampana = data.get("nameCampana")
        if namePersona is None: namePersona = data.get("namePersona")
        if startDC is None:    startDC = _parse_us_dt(data.get("startDateCampana"))
        if endDC   is None:    endDC   = _parse_us_dt(data.get("endDateCampana"))
        if not montoTotal:     montoTotal = _clean_monto(data.get("montoTotal"))

    fechas_validas = [r["date"] for r in registros if r["date"] is not None]
    if startDC is None and fechas_validas: startDC = min(fechas_validas)
    if endDC   is None and fechas_validas: endDC   = max(fechas_validas)

    meta = {
        "nameCampana": nameCampana or DEFAULT_NAME_CAMPANA,
        "namePersona": namePersona or DEFAULT_NAME_PERSONA,
        "startDateCampana": startDC,
        "endDateCampana": endDC,
        "montoTotal": montoTotal,
    }
    registros.sort(key=lambda r: (r["date"] or datetime(1900,1,1), r["doc_id"]))
    return registros, meta

# ─────── Excel: headers / estilo / escritura desde plantilla ───────
def _find_headers(ws: Worksheet, names=("Fecha","Detalle","Boleta","Gasto"), search_rows: int = 20) -> Dict[str,int]:
    headers: Dict[str,int] = {}; wanted = set(names)
    for r in range(1, search_rows + 1):
        for c in range(1, ws.max_column + 1):
            val = ws.cell(row=r, column=c).value
            if isinstance(val, str):
                v = val.strip()
                if v in wanted and v not in headers:
                    headers[v] = c
        if len(headers) == len(names): break
    defaults = {"Fecha":1, "Detalle":4, "Factura":5, "Boleta":6, "Gasto":7}
    for k in defaults: headers.setdefault(k, defaults[k])
    return headers

def _copy_row_style(ws, src_row: int, dst_row: int, cols: list[int]) -> None:
    try: ws.row_dimensions[dst_row].height = ws.row_dimensions[src_row].height
    except Exception: pass
    for col in cols:
        src = ws.cell(row=src_row, column=col)
        dst = ws.cell(row=dst_row, column=col)
        try:
            if src.has_style:
                try: dst.font = copy(src.font)
                except Exception: pass
                try: dst.fill = copy(src.fill)
                except Exception: pass
                try: dst.border = copy(src.border)
                except Exception: pass
                try: dst.alignment = copy(src.alignment)
                except Exception: pass
                try: dst.protection = copy(src.protection)
                except Exception: pass
                try: dst.number_format = src.number_format
                except Exception: pass
        except TypeError:
            try: dst.number_format = src.number_format
            except Exception: pass

NOTA_EXCEDENTES = (
    "Nota: cuando el gasto de una categoría supera su monto asignado, la diferencia "
    "(excedente) no se incluye en el Total Gastado de esta rendición. Dicho excedente es asumido "
    "por el profesional que rinde, de modo que se respetan los montos asignados a cada categoría "
    "y el Saldo favor / Contra refleja el monto efectivo a devolver."
)

def _write_detalle_excedentes(
    ws: Worksheet, registros: List[dict], meta: Dict[str, Any], top_row: int,
    first_row: int, last_row: int, detalle_col: str, gasto_col: str,
) -> Optional[str]:
    """Escribe en I:L el cuadro 'Detalle de excedentes por categoría' y la nota.
    Devuelve la celda con el total de excedentes, o None si la campaña no tiene
    montos asignados por categoría o ninguna categoría tuvo excedente
    (en esos casos no se escribe nada)."""
    asignados: Dict[str, float] = meta.get("montosCategoria") or {}
    if not asignados:
        return None

    # (categoria, asignado, gastado): primero las que tienen gastos, luego las asignadas sin gastos
    filas: List[Tuple[str, float, float]] = []
    vistas = set()
    for cat, _, monto in resumen_por_categoria(registros):
        vistas.add(_slug_categoria(cat).lower().rstrip("s"))
        filas.append((cat, _buscar_categoria(asignados, cat) or 0.0, monto))
    for k in sorted(asignados):
        if k == "total" or k.rstrip("s") in vistas or not asignados[k]:
            continue
        vistas.add(k.rstrip("s"))
        filas.append((k[:1].upper() + k[1:], asignados[k], 0.0))

    # Sin excedentes en ninguna categoría: no se dibuja el cuadro y el Excel queda normal
    if not any(monto > asig for _, asig, monto in filas):
        return None

    lab, val = ws.cell(row=top_row + 1, column=6), ws.cell(row=top_row + 1, column=7)   # estilo naranja
    plain = ws.cell(row=first_row, column=7)
    thin, med = Side(style="thin", color="000000"), Side(style="medium", color="000000")
    b_thin = Border(left=thin, right=thin, top=thin, bottom=thin)
    b_med  = Border(left=med, right=med, top=med, bottom=med)

    def _naranja(c, border, centrado=False):
        f = copy(lab.font); f.b = True
        c.font = f; c.fill = copy(lab.fill); c.border = border
        if centrado:
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    ws.merge_cells(start_row=top_row, start_column=9, end_row=top_row, end_column=12)
    ws.cell(row=top_row, column=9).value = "Detalle de excedentes por categoría"
    for col in range(9, 13):
        _naranja(ws.cell(row=top_row, column=col), b_med, True)
    for j, h in enumerate(("Categoría", "Monto asignado", "Monto gastado", "Excedente")):
        c = ws.cell(row=top_row + 1, column=9 + j); c.value = h
        _naranja(c, b_thin, True)

    r0 = top_row + 2
    rng_det = f"${detalle_col}${first_row}:${detalle_col}${last_row}"
    rng_gas = f"${gasto_col}${first_row}:${gasto_col}${last_row}"
    for i, (cat, asig, monto) in enumerate(filas):
        r = r0 + i
        ws.cell(row=r, column=9).value = cat
        ws.cell(row=r, column=10).value = float(asig)
        # "Sin categoría" no existe como texto en la columna Detalle: se escribe el valor
        ws.cell(row=r, column=11).value = (
            float(monto) if cat == "Sin categoría" else f"=SUMIF({rng_det},I{r},{rng_gas})"
        )
        ws.cell(row=r, column=12).value = f"=MAX(K{r}-J{r},0)"
        for col in range(9, 13):
            c = ws.cell(row=r, column=col)
            c.font = copy(plain.font); c.border = b_thin
            if col > 9: c.number_format = plain.number_format
    r1 = r0 + len(filas) - 1
    tr = r1 + 1
    ws.cell(row=tr, column=9).value = "Total"
    for L in "JKL":
        ws[f"{L}{tr}"].value = f"=SUM({L}{r0}:{L}{r1})" if filas else 0
    for col in range(9, 13):
        c = ws.cell(row=tr, column=col)
        _naranja(c, b_med)
        if col > 9: c.number_format = val.number_format

    nr = tr + 2
    ws.merge_cells(start_row=nr, start_column=9, end_row=nr + 3, end_column=12)
    nota = ws.cell(row=nr, column=9); nota.value = NOTA_EXCEDENTES
    nota.font = Font(name="Arial", size=9, italic=True)
    nota.alignment = Alignment(wrap_text=True, vertical="top")

    for L, w in zip("HIJKL", (3, 18, 16, 16, 14)):
        ws.column_dimensions[L].width = w
    ws.print_area = f"A1:L{nr + 3}"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth = 1; ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    return f"L{tr}"

def write_excel_from_template(
    template_path: Union[str, Path],
    registros: List[dict],
    meta: Dict[str, Any],
    start_row: int = 11,
    sheet_name: Optional[str] = None,
) -> Path:
    template_path = Path(template_path)
    if not template_path.exists():
        raise FileNotFoundError(f"No existe la plantilla: {template_path}")

    wb = load_workbook(template_path, data_only=False)
    ws = wb[sheet_name] if sheet_name and sheet_name in wb.sheetnames else wb.active

    cols = _find_headers(ws, names=("Fecha", "Detalle", "Factura", "Boleta", "Gasto"))

    nameCampana = meta.get("nameCampana") or DEFAULT_NAME_CAMPANA
    namePersona = meta.get("namePersona") or DEFAULT_NAME_PERSONA
    startDC: Optional[datetime] = meta.get("startDateCampana")
    endDC: Optional[datetime] = meta.get("endDateCampana")
    montoTotal = meta.get("montoTotal") or 0.0

    fechaRend: datetime = meta.get("fechaRendicion") or datetime.now(TZ_CHILE)

    ws["C4"] = meta.get("codProy") or nameCampana
    ws["C5"] = meta.get("responsable") or namePersona
    ws["C6"] = _to_ddmmyyyy(fechaRend)
    if startDC: ws["E4"] = _to_ddmmyyyy(startDC)
    if endDC:   ws["E5"] = _to_ddmmyyyy(endDC)
    ws["E7"] = _clean_monto(montoTotal)

    used_cols = [cols["Fecha"], cols["Detalle"], cols["Factura"], cols["Boleta"], cols["Gasto"]]
    n = len(registros)

    if n >= 2:
        # insert_rows no mueve las celdas combinadas: se desplazan a mano
        shift = n - 1
        below = [mr for mr in list(ws.merged_cells.ranges) if mr.min_row > start_row]
        for mr in below:
            ws.unmerge_cells(str(mr))
        ws.insert_rows(start_row + 1, amount=shift)
        for mr in below:
            ws.merge_cells(start_row=mr.min_row + shift, end_row=mr.max_row + shift,
                           start_column=mr.min_col, end_column=mr.max_col)

    for r in range(start_row, start_row + max(n, 1)):
        _copy_row_style(ws, start_row, r, used_cols)

    for idx, reg in enumerate(registros):
        row = start_row + idx
        dt = reg["date"]
        ws.cell(row=row, column=cols["Fecha"]).value = _to_ddmmyyyy(dt) if dt else ""
        ws.cell(row=row, column=cols["Detalle"]).value = reg.get("categoria", "")
        # El N° de documento va en "Factura" (E) o en "Boleta" (F) según el tipo de comprobante
        col_doc = cols["Factura"] if reg.get("esFactura") else cols["Boleta"]
        ws.cell(row=row, column=col_doc).value = reg.get("idBoleta", "")
        ws.cell(row=row, column=cols["Gasto"]).value = float(reg.get("Monto") or 0.0)

    # Bordes A..G
    thin = Side(style="thin", color="000000")
    box  = Border(left=thin, right=thin, top=thin, bottom=thin)
    for r in range(start_row, start_row + n):
        for c in range(1, 8):
            ws.cell(row=r, column=c).border = box

    # Totales dinámicos
    first_row = start_row
    last_row  = start_row + max(n, 1) - 1
    sum_row   = (start_row + n) if n >= 1 else (start_row + 1)
    gasto_col_idx    = cols["Gasto"]
    gasto_col_letter = get_column_letter(gasto_col_idx)
    sum_cell = f"G{sum_row}"

    try: prev_fmt = ws[sum_cell].number_format
    except Exception: prev_fmt = None

    if n >= 1:
        ws[sum_cell].value = f"=SUM({gasto_col_letter}{first_row}:{gasto_col_letter}{last_row})"
    else:
        ws[sum_cell].value = 0
    if prev_fmt:
        try: ws[sum_cell].number_format = prev_fmt
        except Exception: pass

    # Cuadro naranja: Total Gastado (G14), Fondo Asignado (G15), Saldo favor / Contra (G16)
    g14_new_row = 14 + max(n - 1, 0)
    g14_cell    = f"G{g14_new_row}"
    g16_new_row = 16 + max(n - 1, 0)
    g16_cell    = f"G{g16_new_row}"

    # Detalle de excedentes por categoría (a la derecha del cuadro naranja).
    # El sobregiro de una categoría lo asume el profesional: se descuenta del Total Gastado.
    exc_total_cell = _write_detalle_excedentes(
        ws, registros, meta, top_row=g14_new_row - 1,
        first_row=first_row, last_row=last_row,
        detalle_col=get_column_letter(cols["Detalle"]), gasto_col=gasto_col_letter,
    )

    # G14 = suma de comprobantes menos excedentes (o la suma tal cual si no hay excedentes)
    ws[g14_cell].value = f"={sum_cell}-{exc_total_cell}" if exc_total_cell else f"={sum_cell}"
    try: ws[g14_cell].number_format = ws[sum_cell].number_format
    except Exception: pass

    # G16 = E7 - G14
    ws[g16_cell].value = f"=E7-{g14_cell}"
    try: ws[g16_cell].number_format = ws[g14_cell].number_format
    except Exception: pass

    # Nombre de salida
    if startDC:
        startDay = str(int(startDC.strftime("%d"))); startMonth = startDC.strftime("%m"); startYear = startDC.strftime("%Y")
    else:
        startDay, startMonth, startYear = "01","01","1900"
    endDay = str(int(endDC.strftime("%d"))) if endDC else startDay

    out_name = f"{nameCampana} Rendición {startDay}-{endDay} {startMonth} {startYear}.xlsx"
    out_path = _unique_path(OUTPUT_DIR / out_name)
    try:
        wb.save(out_path)
    except PermissionError:
        out_path = _unique_path(out_path)
        wb.save(out_path)
    return out_path

# ─────────── Descarga fotos y armado de PDF por fecha ───────────
def download_images_grouped_by_date(
    registros: List[dict],
    gcs_client: gcs.Client,
    out_dir: Union[str, Path],
    max_workers: int = 8,
) -> Dict[str, List[Path]]:
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    tasks = []
    for reg in registros:
        dt = reg["date"] or datetime(1900, 1, 1)
        fecha_key = _to_ddmmyyyy(dt)
        for i, u in enumerate(reg.get("fotos", [])):
            fname = _safe_filename(f"{reg['doc_id']}_{fecha_key.replace('-', '')}", i, u)
            dest = out_dir / fecha_key / fname
            tasks.append((u, dest, fecha_key))

    results: List[Tuple[str, Path, str, bool, Optional[str]]] = []
    def _one(u: str, dest: Path, fkey: str):
        try:
            if _is_gs(u): _download_gs(gcs_client, u, dest)
            else:        _download_http(u, dest)
            return (u, dest, fkey, True, None)
        except Exception as e:
            return (u, dest, fkey, False, str(e))

    if tasks:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_one, u, d, fk) for (u, d, fk) in tasks]
            for f in as_completed(futs):
                results.append(f.result())

    grouped: Dict[str, List[Path]] = {}
    for (u, p, fkey, ok, err) in results:
        if ok: grouped.setdefault(fkey, []).append(p)
    for k in grouped: grouped[k].sort()

    def _key(k: str) -> datetime: return datetime.strptime(k, "%d-%m-%Y")
    grouped = dict(sorted(grouped.items(), key=lambda kv: _key(kv[0])))
    return grouped

def _fit_text(c, text: str, font: str, size: float, max_w: float) -> str:
    text = str(text)
    if c.stringWidth(text, font, size) <= max_w:
        return text
    while text and c.stringWidth(text + "…", font, size) > max_w:
        text = text[:-1]
    return text + "…"

def _franja_ams(c, x: float, y: float, w: float, h: float = 4) -> None:
    seg = w / 3
    for i, col in enumerate((AMS_VERDE, AMS_AMARILLO, AMS_AZUL)):
        c.setFillColor(HexColor(col)); c.rect(x + i * seg, y, seg, h, stroke=0, fill=1)

def _fecha_larga(fecha_key: str) -> str:
    """'05-03-2025' -> '5 de marzo de 2025' ('' si no se puede leer)."""
    try:
        dt = datetime.strptime(fecha_key, "%d-%m-%Y")
        return f"{dt.day} de {mes_es(dt)} de {dt.year}"
    except Exception:
        return ""

def _abrir_boleta(p: Path) -> PILImage.Image:
    """Abre la foto respetando la orientación EXIF (tal como se ve en el teléfono), en RGB/L."""
    with PILImage.open(p) as im:
        im = ImageOps.exif_transpose(im)
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        im.load()
    return im

def draw_separador_fecha(c, fecha_key: str, n_fotos: int) -> None:
    """Página que separa las boletas de cada día."""
    pw, ph = A4
    mx = 48
    if LOGO_PATH.exists():
        try:
            with PILImage.open(LOGO_PATH) as im:
                lw, lh = im.size
            w = 90; h = w * lh / lw
            c.drawImage(ImageReader(str(LOGO_PATH)), mx, ph - 40 - h, width=w, height=h, mask="auto")
        except Exception:
            traceback.print_exc()
    largo = _fecha_larga(fecha_key)
    cy = ph / 2
    c.setFillColor(HexColor("#8A8C8E")); c.setFont("Helvetica", 11)
    c.drawCentredString(pw / 2, cy + 58, "BOLETAS DEL DÍA")
    c.setFillColor(HexColor(AMS_GRIS)); c.setFont("Helvetica-Bold", 40)
    c.drawCentredString(pw / 2, cy + 12, fecha_key)
    _franja_ams(c, pw / 2 - 130, cy - 10, 260, 5)
    c.setFillColor(HexColor("#8A8C8E")); c.setFont("Helvetica", 12)
    pie = f"{n_fotos} respaldo" + ("" if n_fotos == 1 else "s")
    c.drawCentredString(pw / 2, cy - 36, f"{largo}  ·  {pie}" if largo else pie)
    c.setFillColor(HexColor("#000000"))
    c.showPage()

def _datos_portada(meta: Dict[str, Any], registros: List[dict]) -> Dict[str, Any]:
    """Datos de la portada (cabecera, tarjetas y tabla por categoría), comunes al PDF y al Word."""
    startDC, endDC = meta.get("startDateCampana"), meta.get("endDateCampana")
    fechaRend = meta.get("fechaRendicion") or datetime.now(TZ_CHILE)
    campos = [
        ("Código del proyecto",      meta.get("codProy") or "—"),
        ("Campaña",                  meta.get("nameCampana") or "—"),
        ("Responsable de rendición", meta.get("responsable") or "—"),
        ("Fecha de rendición",       _to_ddmmyyyy(fechaRend)),
    ]
    periodo = str(meta.get("periodoTexto") or "").strip()
    if not periodo and startDC:
        periodo = f"{_to_ddmmyyyy(startDC)} al {_to_ddmmyyyy(endDC or startDC)}"
    if periodo:
        campos.append(("Período de terreno", periodo))

    # ── Datos de la tabla ──
    resumen = resumen_por_categoria(registros)
    gastado = sum(m for _, _, m in resumen)
    asignados: Dict[str, float] = meta.get("montosCategoria") or {}
    saldos: Dict[str, float] = meta.get("saldosCategoria") or {}
    asignado = _clean_monto(meta.get("montoTotal"))
    saldo = saldos["total"] if "total" in saldos else asignado - gastado

    # Filas: (categoria, n_boletas, gastado, asignado, saldo, a_devolver)
    # "Monto a devolver" = saldo con piso en 0: el sobregiro lo asume el profesional.
    filas: List[Tuple[str, int, float, Optional[float], Optional[float], Optional[float]]] = []
    vistas = set()
    for cat, nb, monto in resumen:
        vistas.add(_slug_categoria(cat).lower().rstrip("s"))
        asig = _buscar_categoria(asignados, cat)
        sal = _buscar_categoria(saldos, cat)
        if sal is None and asig is not None:
            sal = asig - monto
        dev = max(sal, 0.0) if sal is not None else None
        filas.append((cat, nb, monto, asig, sal, dev))

    # Categorías con monto asignado pero sin boletas: su saldo completo también se devuelve,
    # así que deben estar en la tabla para que la suma de "Monto a devolver" sea correcta.
    for k in sorted(set(asignados) | set(saldos)):
        if k == "total" or k.rstrip("s") in vistas:
            continue
        vistas.add(k.rstrip("s"))
        asig = _buscar_categoria(asignados, k)
        sal = _buscar_categoria(saldos, k)
        if sal is None:
            sal = asig
        if not asig and not sal:
            continue
        filas.append((k[:1].upper() + k[1:], 0, 0.0, asig, sal, max(sal or 0.0, 0.0)))

    devs = [f[5] for f in filas if f[5] is not None]
    # Sin datos por categoría: se usa el saldo global con piso en 0
    a_devolver = sum(devs) if devs else max(saldo, 0.0)

    return {
        "campos": campos, "fechaRend": fechaRend, "filas": filas, "gastado": gastado,
        "asignado": asignado, "saldo": saldo, "a_devolver": a_devolver,
    }

def draw_portada(c, meta: Dict[str, Any], registros: List[dict]) -> None:
    pw, ph = A4
    mx = 48
    azul = HexColor(AMS_GRIS)          # color principal (títulos, tabla)
    gris, claro, linea = HexColor("#8A8C8E"), HexColor("#F4F5F5"), HexColor("#D9DADB")
    verde, amarillo, celeste = HexColor(AMS_VERDE), HexColor(AMS_AMARILLO), HexColor(AMS_AZUL)
    rojo = HexColor("#C0392B")

    d = _datos_portada(meta, registros)
    campos, fechaRend, filas = d["campos"], d["fechaRend"], d["filas"]
    gastado, asignado, saldo, a_devolver = d["gastado"], d["asignado"], d["saldo"], d["a_devolver"]

    # Encabezado: logo a la izquierda, título a la derecha, franja tricolor
    if LOGO_PATH.exists():
        try:
            with PILImage.open(LOGO_PATH) as im:
                lw, lh = im.size
            w = 150; h = w * lh / lw
            c.drawImage(ImageReader(str(LOGO_PATH)), mx, ph - 40 - h, width=w, height=h, mask="auto")
        except Exception:
            traceback.print_exc()
    c.setFillColor(azul)
    c.setFont("Helvetica-Bold", 22); c.drawRightString(pw - mx, ph - 78, "Rendición de Fondos")
    c.setFillColor(gris)
    c.setFont("Helvetica", 10.5);    c.drawRightString(pw - mx, ph - 96, "Respaldo de boletas y facturas")
    _franja_ams(c, mx, ph - 136, pw - 2 * mx)

    # Datos de cabecera
    y = ph - 170
    for i, (lab, val) in enumerate(campos):
        c.setFillColor(gris); c.setFont("Helvetica", 9)
        c.drawString(mx, y, lab.upper())
        destacado = i <= 1      # código del proyecto y campaña
        c.setFillColor(azul if destacado else HexColor("#111111"))
        fsz = 20 if destacado else 13
        c.setFont("Helvetica-Bold", fsz)
        c.drawString(mx, y - fsz - 2, _fit_text(c, val, "Helvetica-Bold", fsz, pw - 2 * mx))
        y -= fsz + 28

    # Tarjetas de resumen
    tarjetas = [("MONTO ASIGNADO", asignado, azul, celeste), ("TOTAL GASTADO", gastado, azul, amarillo),
                ("SALDO", saldo, rojo if saldo < 0 else azul, rojo if saldo < 0 else celeste),
                ("MONTO A DEVOLVER", a_devolver, azul, verde)]
    gap = 10; cw = (pw - 2 * mx - 3 * gap) / 4; chh = 58
    y -= 6
    for i, (lab, val, col, acento) in enumerate(tarjetas):
        x = mx + i * (cw + gap)
        c.setFillColor(claro);  c.rect(x, y - chh, cw, chh, stroke=0, fill=1)
        c.setFillColor(acento); c.rect(x, y - chh, 4, chh, stroke=0, fill=1)
        c.setFillColor(gris); c.setFont("Helvetica", 8.5); c.drawString(x + 12, y - 18, lab)
        c.setFillColor(col);  c.setFont("Helvetica-Bold", 14); c.drawString(x + 12, y - 42, _fmt_clp(val))
    y -= chh + 30

    # Tabla por categoría
    x0, x1 = mx, pw - mx
    # columnas (alineadas a la derecha): boletas, gastado, asignado, saldo, a devolver
    xb, xg, xt, xs, xd = x1 - 317, x1 - 242, x1 - 162, x1 - 97, x1 - 10
    rh = 22
    negro = HexColor("#111111")
    c.setFillColor(azul); c.rect(x0, y - rh, x1 - x0, rh, stroke=0, fill=1)
    c.setFillColor(white); c.setFont("Helvetica-Bold", 9)
    c.drawString(x0 + 10, y - 15, "Categoría")
    c.drawRightString(xb, y - 15, "N° boletas")
    c.drawRightString(xg, y - 15, "Monto gastado")
    c.drawRightString(xt, y - 15, "Monto asignado")
    c.drawRightString(xs, y - 15, "Saldo")
    c.drawRightString(xd, y - 15, "Monto a devolver")
    y -= rh

    if not filas:
        c.setFillColor(gris); c.setFont("Helvetica-Oblique", 10)
        c.drawString(x0 + 10, y - 15, "Sin registros de gasto"); y -= rh
    for i, (cat, nb, monto, asig, sal, dev) in enumerate(filas):
        if y - rh < 110:      # no pisar el pie de página
            c.setFillColor(gris); c.setFont("Helvetica-Oblique", 9)
            c.drawString(x0 + 10, y - 14, f"… y {len(filas) - i} categorías más (ver Excel)"); y -= rh
            break
        if i % 2 == 1:
            c.setFillColor(claro); c.rect(x0, y - rh, x1 - x0, rh, stroke=0, fill=1)
        c.setFillColor(negro); c.setFont("Helvetica", 9.5)
        c.drawString(x0 + 10, y - 15, _fit_text(c, cat, "Helvetica", 9.5, xb - x0 - 65))
        c.drawRightString(xb, y - 15, str(nb))
        c.drawRightString(xg, y - 15, _fmt_clp(monto))
        c.drawRightString(xt, y - 15, _fmt_clp(asig) if asig is not None else "—")
        if sal is not None and sal < 0:
            c.setFillColor(rojo)
        c.drawRightString(xs, y - 15, _fmt_clp(sal) if sal is not None else "—")
        c.setFillColor(negro)
        c.drawRightString(xd, y - 15, _fmt_clp(dev) if dev is not None else "—")
        y -= rh

    # Fila de totales
    c.setStrokeColor(azul); c.setLineWidth(1.2); c.line(x0, y, x1, y)
    c.setFillColor(azul); c.setFont("Helvetica-Bold", 10)
    c.drawString(x0 + 10, y - 15, "Total")
    c.drawRightString(xb, y - 15, str(len(registros)))
    c.drawRightString(xg, y - 15, _fmt_clp(gastado))
    c.drawRightString(xt, y - 15, _fmt_clp(asignado))
    if saldo < 0:
        c.setFillColor(rojo)
    c.drawRightString(xs, y - 15, _fmt_clp(saldo))
    c.setFillColor(azul)
    c.drawRightString(xd, y - 15, _fmt_clp(a_devolver))
    y -= rh
    c.setStrokeColor(linea); c.setLineWidth(0.6); c.line(x0, y, x1, y)

    # Pie
    c.setStrokeColor(linea); c.line(mx, 60, pw - mx, 60)
    c.setFillColor(gris); c.setFont("Helvetica", 8.5)
    c.drawString(mx, 46, f"Generado el {fechaRend.strftime('%d-%m-%Y %H:%M')}")
    c.drawRightString(pw - mx, 46, f"{len(registros)} registros")
    c.setFillColor(HexColor("#000000")); c.setStrokeColor(HexColor("#000000")); c.setLineWidth(1)
    c.showPage()

def build_pdf_from_grouped_images(
    grouped: Dict[str, List[Path]],
    out_pdf: Union[str, Path],
    meta: Optional[Dict[str, Any]] = None,
    registros: Optional[List[dict]] = None,
) -> Path:
    out_pdf = _unique_path(Path(out_pdf))
    c = canvas.Canvas(str(out_pdf), pagesize=A4)
    pw, ph = A4
    margin = 36

    if meta is not None:
        draw_portada(c, meta, registros or [])

    if not grouped:
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(pw/2, ph/2, "Sin fotos")
        c.showPage()
        c.save()
        return out_pdf

    for fecha_key, paths in grouped.items():
        draw_separador_fecha(c, fecha_key, len(paths))
        for p in paths:
            try:
                im = _abrir_boleta(p)
                w, h = im.size
                max_w = pw - 2 * margin
                max_h = ph - 2 * margin
                scale = min(max_w / w, max_h / h)
                new_w, new_h = w * scale, h * scale
                x = (pw - new_w) / 2
                y = (ph - new_h) / 2
                c.drawImage(ImageReader(im), x, y, width=new_w, height=new_h)
                c.showPage()
            except Exception:
                traceback.print_exc()
                continue

    c.save()
    return out_pdf

# ─────────── Word (.docx) con el mismo contenido del PDF ───────────
def _rgb(hex_color: str) -> RGBColor:
    return RGBColor.from_string(hex_color.lstrip("#").upper())

def _run(p, text: Any, size: float = 10, bold: bool = False, color: str = "#111111", italic: bool = False):
    r = p.add_run(str(text))
    r.font.size = Pt(size); r.bold = bold; r.italic = italic
    r.font.color.rgb = _rgb(color)
    return r

def _parrafo(contenedor, align=None, antes: float = 0, despues: float = 0, nueva_pagina: bool = False):
    p = contenedor.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(antes); pf.space_after = Pt(despues)
    pf.page_break_before = nueva_pagina
    if align is not None:
        p.alignment = align
    return p

def _bordes(cell, **lados: Tuple[int, str]) -> None:
    """_bordes(cell, left=(36, '#7AB573')): grosor en octavos de punto. Llamar antes de _sombrear."""
    tcPr = cell._tc.get_or_add_tcPr()
    b = tcPr.find(qn("w:tcBorders"))
    if b is None:
        b = OxmlElement("w:tcBorders"); tcPr.append(b)
    for lado in ("top", "left", "bottom", "right"):
        if lado in lados:
            sz, color = lados[lado]
            el = OxmlElement(f"w:{lado}")
            el.set(qn("w:val"), "single"); el.set(qn("w:sz"), str(sz))
            el.set(qn("w:space"), "0"); el.set(qn("w:color"), color.lstrip("#"))
            b.append(el)

def _sombrear(cell, hex_color: str) -> None:
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), hex_color.lstrip("#"))
    cell._tc.get_or_add_tcPr().append(shd)

def _tabla(doc, anchos_cm: List[float]):
    t = doc.add_table(rows=1, cols=len(anchos_cm))
    t.autofit = False
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for col, w in zip(t.columns, anchos_cm):
        col.width = Cm(w)
    for cell, w in zip(t.rows[0].cells, anchos_cm):
        cell.width = Cm(w)
    return t

def _franja_docx(doc, ancho_cm: float, alto_pt: float = 4) -> None:
    row = _tabla(doc, [ancho_cm / 3] * 3).rows[0]
    row.height = Pt(alto_pt); row.height_rule = WD_ROW_HEIGHT_RULE.EXACTLY
    for cell, col in zip(row.cells, (AMS_VERDE, AMS_AMARILLO, AMS_AZUL)):
        _sombrear(cell, col)
        cell.paragraphs[0].add_run().font.size = Pt(1)

def _logo_docx(p, ancho_pt: float) -> None:
    if LOGO_PATH.exists():
        try:
            p.add_run().add_picture(str(LOGO_PATH), width=Pt(ancho_pt))
        except Exception:
            traceback.print_exc()

def _portada_docx(doc, meta: Dict[str, Any], registros: List[dict]) -> None:
    d = _datos_portada(meta, registros)
    gris, claro, linea, rojo = "#8A8C8E", "#F4F5F5", "#D9DADB", "#C0392B"
    DER, IZQ = WD_ALIGN_PARAGRAPH.RIGHT, WD_ALIGN_PARAGRAPH.LEFT

    # Encabezado: logo a la izquierda, título a la derecha, franja tricolor
    izq, der = _tabla(doc, [ANCHO_UTIL_CM / 2] * 2).rows[0].cells
    _logo_docx(izq.paragraphs[0], 150)
    der.paragraphs[0].alignment = DER
    _run(der.paragraphs[0], "Rendición de Fondos", 22, True, AMS_GRIS)
    _run(_parrafo(der, DER, antes=2), "Respaldo de boletas y facturas", 10.5, color=gris)
    for c in (izq, der):
        c.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
    _parrafo(doc, despues=8)   # Word une dos tablas seguidas si no hay un párrafo entre ellas
    _franja_docx(doc, ANCHO_UTIL_CM)

    # Datos de cabecera
    for i, (lab, val) in enumerate(d["campos"]):
        _run(_parrafo(doc, antes=18 if i == 0 else 12), lab.upper(), 9, color=gris)
        destacado = i <= 1      # código del proyecto y campaña
        _run(_parrafo(doc, antes=2), val, 20 if destacado else 13, True, AMS_GRIS if destacado else "#111111")

    # Tarjetas de resumen (columnas angostas vacías hacen de separación)
    s = d["saldo"]
    tarjetas = [("MONTO ASIGNADO", d["asignado"], AMS_GRIS, AMS_AZUL),
                ("TOTAL GASTADO", d["gastado"], AMS_GRIS, AMS_AMARILLO),
                ("SALDO", s, rojo if s < 0 else AMS_GRIS, rojo if s < 0 else AMS_AZUL),
                ("MONTO A DEVOLVER", d["a_devolver"], AMS_GRIS, AMS_VERDE)]
    _parrafo(doc, despues=16)
    row = _tabla(doc, [4.2, 0.4, 4.2, 0.4, 4.2, 0.4, 4.2]).rows[0]
    row.height = Cm(1.9)
    for i, (lab, val, col, acento) in enumerate(tarjetas):
        cell = row.cells[i * 2]
        _bordes(cell, left=(36, acento))
        _sombrear(cell, claro)
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        _run(cell.paragraphs[0], lab, 8.5, color=gris)
        _run(_parrafo(cell, antes=4), _fmt_clp(val), 14, True, col)

    # Tabla por categoría
    anchos = [5.0, 1.9, 2.8, 2.9, 2.5, 2.9]
    _parrafo(doc, despues=18)
    t = _tabla(doc, anchos)

    def _llenar(cells, valores, size, bold=False, color="#111111", fondo=None, rojo_saldo=False, borde_sup=None):
        for j, (cell, v) in enumerate(zip(cells, valores)):
            cell.width = Cm(anchos[j])
            if borde_sup: _bordes(cell, top=borde_sup)
            if fondo: _sombrear(cell, fondo)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(4); p.paragraph_format.space_after = Pt(4)
            p.alignment = IZQ if j == 0 else DER
            _run(p, v, size, bold, rojo if (j == 4 and rojo_saldo) else color)

    _llenar(t.rows[0].cells, ("Categoría", "N° boletas", "Monto gastado", "Monto asignado", "Saldo", "Monto a devolver"),
            9, True, "#FFFFFF", AMS_GRIS)
    if not d["filas"]:
        cells = t.add_row().cells
        m = cells[0].merge(cells[-1])
        m.paragraphs[0].paragraph_format.space_before = Pt(4)
        _run(m.paragraphs[0], "Sin registros de gasto", 10, color=gris, italic=True)
    for i, (cat, nb, monto, asig, sal, dev) in enumerate(d["filas"]):
        _llenar(t.add_row().cells,
                (cat, nb, _fmt_clp(monto),
                 _fmt_clp(asig) if asig is not None else "—",
                 _fmt_clp(sal) if sal is not None else "—",
                 _fmt_clp(dev) if dev is not None else "—"),
                9.5, fondo=claro if i % 2 == 1 else None, rojo_saldo=sal is not None and sal < 0)
    _llenar(t.add_row().cells,
            ("Total", len(registros), _fmt_clp(d["gastado"]), _fmt_clp(d["asignado"]),
             _fmt_clp(s), _fmt_clp(d["a_devolver"])),
            10, True, AMS_GRIS, rojo_saldo=s < 0, borde_sup=(10, AMS_GRIS))

    # Pie
    _parrafo(doc, despues=28)
    izq, der = _tabla(doc, [ANCHO_UTIL_CM / 2] * 2).rows[0].cells
    for c in (izq, der):
        _bordes(c, top=(4, linea))
        c.paragraphs[0].paragraph_format.space_before = Pt(6)
    _run(izq.paragraphs[0], f"Generado el {d['fechaRend'].strftime('%d-%m-%Y %H:%M')}", 8.5, color=gris)
    der.paragraphs[0].alignment = DER
    _run(der.paragraphs[0], f"{len(registros)} registros", 8.5, color=gris)

def _separador_docx(doc, fecha_key: str, n_fotos: int) -> None:
    """Página que separa las boletas de cada día."""
    gris = "#8A8C8E"
    CENTRO = WD_ALIGN_PARAGRAPH.CENTER
    _logo_docx(_parrafo(doc, nueva_pagina=True), 90)
    _run(_parrafo(doc, CENTRO, antes=230), "BOLETAS DEL DÍA", 11, color=gris)
    _run(_parrafo(doc, CENTRO, antes=4, despues=10), fecha_key, 40, True, AMS_GRIS)
    _franja_docx(doc, 9.2, 5)
    largo = _fecha_larga(fecha_key)
    pie = f"{n_fotos} respaldo" + ("" if n_fotos == 1 else "s")
    _run(_parrafo(doc, CENTRO, antes=14), f"{largo}  ·  {pie}" if largo else pie, 12, color=gris)

def build_docx_from_grouped_images(
    grouped: Dict[str, List[Path]],
    out_docx: Union[str, Path],
    meta: Optional[Dict[str, Any]] = None,
    registros: Optional[List[dict]] = None,
) -> Path:
    out_docx = _unique_path(Path(out_docx))
    doc = Document()
    sec = doc.sections[0]
    sec.page_width, sec.page_height = Cm(21), Cm(29.7)
    sec.left_margin = sec.right_margin = sec.top_margin = sec.bottom_margin = Cm(MARGEN_DOCX_CM)
    normal = doc.styles["Normal"]
    normal.font.name = "Arial"; normal.font.size = Pt(10)
    normal.paragraph_format.space_before = Pt(0); normal.paragraph_format.space_after = Pt(0)

    if meta is not None:
        _portada_docx(doc, meta, registros or [])

    if not grouped:
        _run(_parrafo(doc, WD_ALIGN_PARAGRAPH.CENTER, antes=300, nueva_pagina=meta is not None),
             "Sin fotos", 18, True)
        doc.save(str(out_docx))
        return out_docx

    # Un poco menos que el alto útil para que la foto no empuje un salto de página extra
    max_w, max_h = Cm(ANCHO_UTIL_CM), Cm(ALTO_UTIL_CM - 1)
    for fecha_key, paths in grouped.items():
        _separador_docx(doc, fecha_key, len(paths))
        for p in paths:
            try:
                im = _abrir_boleta(p)
                im.thumbnail((2400, 2400))      # limita el peso del .docx
                w, h = im.size
                scale = min(max_w / w, max_h / h)
                buf = BytesIO(); im.save(buf, format="JPEG", quality=88); buf.seek(0)
                par = _parrafo(doc, WD_ALIGN_PARAGRAPH.CENTER, nueva_pagina=True)
                par.add_run().add_picture(buf, width=Emu(int(w * scale)), height=Emu(int(h * scale)))
            except Exception:
                traceback.print_exc()
                continue

    doc.save(str(out_docx))
    return out_docx

# ───────────────────────── Lógica de orquestación ──────────────────────────
def generar_rendicion_bundle(
    campana_id: Optional[str],
    template_path: Optional[str] = None,
    collection_name: str = "registros",
    fotos_field: str = "foto",
    start_row: int = 11,
    sheet_name: Optional[str] = None,
) -> Tuple[Path, Path, Path]:
    template = Path(template_path or TEMPLATE_XLSX_DEFAULT)
    fotos_dir = default_fotos_dir()

    registros, meta = fetch_registros(
        collection_name=collection_name,
        fotos_field=fotos_field,
        campana_id=campana_id,
    )

    # Cabecera desde la colección "campana" (con respaldo en lo que venga en registros)
    camp = fetch_campana(campana_id)
    meta["codProy"]        = camp.get("codProy") or meta["nameCampana"]
    meta["nameCampana"]    = camp.get("name") or meta["nameCampana"]
    meta["responsable"]    = camp.get("responsable") or meta["namePersona"]
    meta["fechaRendicion"] = datetime.now(TZ_CHILE)
    meta["periodoTexto"]   = camp.get("dateTimeTextFormat") or ""
    meta["montosCategoria"] = montos_por_categoria(camp)
    meta["saldosCategoria"] = saldos_por_categoria(camp)
    if camp.get("montoTotal") is not None:
        meta["montoTotal"] = _clean_monto(camp.get("montoTotal"))

    excel_out = write_excel_from_template(template, registros, meta, start_row=start_row, sheet_name=sheet_name)
    _, gcs_client = init_clients()
    grouped = download_images_grouped_by_date(registros, gcs_client, out_dir=fotos_dir, max_workers=8)

    startDC = meta.get("startDateCampana")
    endDC = meta.get("endDateCampana") or startDC
    if startDC:
        startDay = str(int(startDC.strftime("%d")))
        endDay = str(int((endDC or startDC).strftime("%d")))
        mes = mes_es(startDC); yy = startDC.strftime("%y")
        pdf_name = f"{meta.get('nameCampana', '—')} Rendición {startDay}-{endDay} {mes} {yy}.pdf"
    else:
        pdf_name = "Rendicion_Fotos.pdf"

    pdf_out = build_pdf_from_grouped_images(grouped, OUTPUT_DIR / pdf_name, meta=meta, registros=registros)
    word_out = build_docx_from_grouped_images(
        grouped, OUTPUT_DIR / (Path(pdf_name).stem + ".docx"), meta=meta, registros=registros
    )
    return excel_out, pdf_out, word_out

# ───────────────────────────────── Endpoints ────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/rendicion")
def rendicion(request: Request, campana_id: str):
    try:
        excel_out, pdf_out, word_out = generar_rendicion_bundle(
            campana_id=campana_id,
            template_path=None,
            collection_name="registros",
            fotos_field="foto",
            start_row=11,
            sheet_name=None,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    excel_url = str(request.url_for("download_file", kind="excel", filename=excel_out.name))
    pdf_url   = str(request.url_for("download_file", kind="pdf",   filename=pdf_out.name))
    word_url  = str(request.url_for("download_file", kind="word",  filename=word_out.name))
    return JSONResponse({"excel_url": excel_url, "pdf_url": pdf_url, "word_url": word_url})

MEDIA_TYPES = {
    "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "pdf":   "application/pdf",
    "word":  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}

@app.get("/download/{kind}/{filename}")
def download_file(kind: str, filename: str):
    if kind not in MEDIA_TYPES:
        raise HTTPException(status_code=400, detail="kind inválido")

    base_dir = OUTPUT_DIR
    file_path = (base_dir / filename).resolve()

    try:
        file_path.relative_to(base_dir)
    except Exception:
        raise HTTPException(status_code=400, detail="Ruta no permitida")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(path=str(file_path), media_type=MEDIA_TYPES[kind], filename=file_path.name)

# (en Render: START ➜ uvicorn app:app --host 0.0.0.0 --port $PORT)
