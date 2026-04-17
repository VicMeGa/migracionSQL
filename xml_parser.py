# =============================================================================
# xml_parser.py — Parser de archivos SpreadsheetML (Excel XML)
#
# Soporta dos formatos detectados automáticamente por el header:
#
#   FORMATO_9  (MEX → USA) — 9 columnas:
#     1:No.Paq  2:Destinatario  3:Telefono  4:Cant  5:Descripcion
#     6:Destino  7:Remitente  8:Fecha  9:Origen
#
#   FORMATO_11 (USA → MEX) — 11 columnas:
#     1:Fecha  2:#ENV  3:Remitente  4:Direccion  5:Telefono(emisor)
#     6:Destinatario  7:Telefono(receptor)  8:Paq  9:LB  10:Descripcion  11:DEST
# =============================================================================

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)

NS = "urn:schemas-microsoft-com:office:spreadsheet"
SS = f"{{{NS}}}"

FORMATO_9  = "MEX_USA"
FORMATO_11 = "USA_MEX"

MAX_VACIAS_CONSECUTIVAS = 10


# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def _extraer_celdas_de_fila(row_element, total_columnas):
    """
    Extrae valores de una fila respetando ss:Index.
    IMPORTANTE: siempre retorna el valor como STRING puro ignorando ss:Type,
    para evitar que Excel interprete folios alfanuméricos como fechas o números.
    """
    celdas = [None] * total_columnas
    posicion_actual = 1

    for cell in row_element.findall(f"{SS}Cell"):
        idx_attr = cell.get(f"{SS}Index")
        if idx_attr is not None:
            posicion_actual = int(idx_attr)

        if 1 <= posicion_actual <= total_columnas:
            data_el = cell.find(f"{SS}Data")
            if data_el is not None and data_el.text:
                # Guardamos también el tipo declarado por Excel para parsear
                # fechas correctamente sin confundirlas con folios
                tipo = cell.get(f"{SS}Type") or (data_el.get(f"{SS}Type") if data_el is not None else None)
                celdas[posicion_actual - 1] = (data_el.text, tipo)

        posicion_actual += 1

    return celdas


def _valor(celda):
    """Extrae solo el texto de una celda (texto, tipo)."""
    if celda is None:
        return None
    return celda[0] if isinstance(celda, tuple) else celda


def _tipo(celda):
    """Extrae el tipo declarado por Excel de una celda."""
    if celda is None:
        return None
    return celda[1] if isinstance(celda, tuple) else None


def _limpiar_telefono(valor):
    """Extrae el primer teléfono. Descarta textos sin dígitos."""
    if not valor:
        return None
    valor_str = str(valor).strip()
    if not any(c.isdigit() for c in valor_str):
        return None
    return valor_str.split(",")[0].strip() or None


def _ole_a_datetime(valor_str):
    """
    Convierte número serial OLE de Excel a datetime.
    Excel cuenta días desde 1899-12-30 (incluye el bug del año bisiesto 1900).
    Ejemplo: 46022.0 → 2025-12-31
    """
    try:
        numero = float(valor_str)
        if numero < 1:
            return None
        return datetime(1899, 12, 30) + timedelta(days=numero)
    except (ValueError, TypeError):
        return None


def _parsear_fecha(celda):
    """
    Parsea fecha desde una celda (texto, tipo).

    Formatos soportados:
      - DateTime ISO:  "2025-09-08T00:00:00.000"  (ss:Type=DateTime)
      - Fecha corta:   "12/31/2025" o "9/27/2025"  (ss:Type=String)
      - Número serial: "46021" o "46021.0"          (ss:Type=Number)

    Retorna (datetime, semana_iso, anio_iso) o (None, None, None).
    """
    texto = _valor(celda)
    tipo  = _tipo(celda)

    if not texto:
        return None, None, None

    texto = str(texto).strip()
    dt = None

    # ── Caso 1: tipo DateTime (ISO) ───────────────────────────────────────
    if tipo == "DateTime" or "T" in texto:
        try:
            dt = datetime.strptime(texto.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    # ── Caso 2: tipo Number → número serial OLE ───────────────────────────
    if dt is None and (tipo == "Number" or re.match(r"^\d+(\.\d+)?$", texto)):
        dt = _ole_a_datetime(texto)

    # ── Caso 3: string de fecha M/D/YYYY ─────────────────────────────────
    if dt is None:
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(texto, fmt)
                break
            except ValueError:
                continue

    if dt is None:
        logger.warning(f"No se pudo parsear fecha '{texto}' (tipo={tipo})")
        return None, None, None

    iso = dt.isocalendar()
    return dt, iso[1], iso[0]


def _parsear_folio(celda):
    """
    Extrae el folio como STRING puro, ignorando el tipo de Excel.

    Problema conocido: Excel puede guardar 'B1' como DateTime si la celda
    tiene formato de fecha aplicado accidentalmente. En ese caso ss:Type=DateTime
    y el texto viene como ISO "1900-01-01T00:00:00.000" (B=2, 1=1ero → día 2).
    En esos casos NO podemos recuperar el folio original — se usa 'SIN FOLIO'.

    Para números puros como "957.0" → "957".
    Para alfanuméricos como "B102" → "B102".
    """
    texto = _valor(celda)
    tipo  = _tipo(celda)

    if not texto:
        return None

    texto = str(texto).strip()

    # Si Excel interpretó el folio como DateTime, el valor es inútil
    # (no podemos saber si era "B1" o "T48" etc.)
    if tipo == "DateTime":
        logger.warning(
            f"Folio con ss:Type=DateTime detectado: '{texto}'. "
            f"Excel interpretó el folio como fecha. "
            f"Verifica el formato de esa celda en el Excel original."
        )
        # Intentar recuperar algo útil del valor numérico subyacente
        # si el texto tiene forma de fecha ISO (ej: "1900-01-02T00:00:00.000")
        # el número serial sería ~2, que podría ser el número del folio
        # pero sin la letra prefijo es imposible reconstruirlo fielmente
        return f"FOLIO-FECHA-{texto[:10]}"   # marcador para identificarlo en logs

    # Número con decimal → limpiar
    if re.match(r"^\d+\.0$", texto):
        return texto.split(".")[0]

    return texto if texto else None


def _parsear_cantidad(celda, numero_fila):
    """Convierte cantidad a int, default 1."""
    texto = _valor(celda)
    try:
        return int(float(texto)) if texto else 1
    except (ValueError, TypeError):
        logger.warning(f"Fila {numero_fila}: cantidad inválida '{texto}', se usa 1.")
        return 1


def _parsear_peso(celda):
    """Convierte peso a float, default 0.00."""
    texto = _valor(celda)
    try:
        return float(texto) if texto else 0.00
    except (ValueError, TypeError):
        return 0.00


def _determinar_tipo_producto(folio):
    if folio and str(folio).upper().startswith("C-"):
        return 12
    return 13


def _normalizar_nombre_sucursal(nombre):
    from config import ALIASES_NOMBRE
    if not nombre:
        return None
    nombre_upper = str(nombre).strip().upper()
    return ALIASES_NOMBRE.get(nombre_upper, nombre_upper)


# =============================================================================
# DETECCIÓN DE FORMATO
# =============================================================================

def _detectar_formato(rows):
    """
    Detecta FORMATO_9 o FORMATO_11 leyendo el header (fila 1).

    FORMATO_11: alguna celda del header contiene "FECHA", "DATE" o "# ENV"
    FORMATO_9:  alguna celda contiene "NO. PAQ", "PAQ", "NO PAQ"

    Busca en todas las celdas del header para no depender de la posición exacta
    (Excel puede saltar celdas con ss:Index y dejar None en posición 0).
    """
    if not rows:
        raise ValueError("El archivo XML no contiene filas.")

    def _leer_valores_fila(row, n_cols):
        celdas = _extraer_celdas_de_fila(row, n_cols)
        return [str(_valor(c)).strip().upper() for c in celdas if _valor(c) is not None]

    # Intentar detectar por header (fila 1)
    valores_header = _leer_valores_fila(rows[0], 12)
    logger.info(f"Header detectado: {valores_header[:6]}")

    indicadores_11 = {"FECHA", "DATE", "# ENV", "#ENV"}  # exclusivos de USA→MEX
    indicadores_9  = {"NO. PAQ.", "NO PAQ", "DESTINATARIO", "PAQ."}

    if any(v in indicadores_11 for v in valores_header):
        logger.info("Formato detectado: FORMATO_11 (USA → MEX, 11 columnas)")
        return FORMATO_11

    if any(v in indicadores_9 for v in valores_header):
        logger.info("Formato detectado: FORMATO_9 (MEX → USA, 9 columnas)")
        return FORMATO_9

    # Header vacío — escanear las primeras filas buscando indicadores de formato
    logger.warning("Header vacío o sin indicadores claros, escaneando filas de datos...")

    for fila_idx in range(1, min(4, len(rows))):
        vals = _leer_valores_fila(rows[fila_idx], 12)
        logger.info(f"Fila {fila_idx + 1} para detección: {vals[:5]}")

        # FORMATO_11: fila de header secundario contiene "REMITENTE" y "DIRECCION"
        if "REMITENTE" in vals and "DIRECCION" in vals:
            logger.info("Formato detectado por header secundario: FORMATO_11 (USA → MEX)")
            return FORMATO_11

        # FORMATO_11: primera columna es fecha ISO o con /
        if vals:
            col1 = vals[0]
            es_fecha = ("/" in col1 or "T00:00:00" in col1)
            col2 = vals[1] if len(vals) > 1 else ""
            # col2 es folio alfanumérico corto (B1, B102, etc.)
            es_folio_corto = bool(col2) and len(col2) <= 6 and any(c.isalpha() for c in col2)
            if es_fecha and es_folio_corto:
                logger.info("Formato detectado por datos: FORMATO_11 (USA → MEX)")
                return FORMATO_11

    logger.info("Formato detectado por datos: FORMATO_9 (MEX → USA)")
    return FORMATO_9


# =============================================================================
# EXTRACTORES POR FORMATO
# =============================================================================

def _extraer_registro_formato9(celdas, numero_fila):
    """
    FORMATO_9 (MEX → USA) — 9 columnas:
    1:Folio  2:Destinatario  3:Tel  4:Cant  5:Desc  6:Destino  7:Remitente  8:Fecha  9:Origen
    """
    folio = _parsear_folio(celdas[0])
    if not folio:
        return None

    fecha, semana, anio = _parsear_fecha(celdas[7])
    desc = _valor(celdas[4])
    dest = _valor(celdas[5])
    orig = _valor(celdas[8])

    return {
        "folio":             folio,
        "destinatario":      (_valor(celdas[1]) or "").strip() or None,
        "telefono_receptor": _limpiar_telefono(_valor(celdas[2])),
        "telefono_emisor":   None,
        "cantidad":          _parsear_cantidad(celdas[3], numero_fila),
        "descripcion":       desc.strip() if desc else None,
        "destino":           dest.strip().upper() if dest else None,
        "remitente":         (_valor(celdas[6]) or "").strip() or None,
        "domicilio_emisor":  None,   # no disponible en MEX→USA
        "domicilio_receptor":None,   # no disponible en MEX→USA
        "peso":              0.00,
        "fecha":             fecha,
        "semana":            semana,
        "anio":              anio,
        "origen_raw":        _normalizar_nombre_sucursal(orig),
        "tipo_producto_id":  _determinar_tipo_producto(folio),
        "numero_fila":       numero_fila,
        "formato":           FORMATO_9,
    }


def _extraer_registro_formato11(celdas, numero_fila):
    """
    FORMATO_11 (USA → MEX) — 11 columnas:
    1:Fecha  2:#ENV  3:Remitente  4:Direccion  5:Tel(emisor)
    6:Destinatario  7:Tel(receptor)  8:Paq  9:LB  10:Descripcion  11:DEST

    Lógica de sucursales:
      - sucursal_emisor  → prefijo del folio (ej: B → Brooklyn/Oxnard)
      - sucursal_receptor → DEST (col 11), sucursal MEX destino
    """
    folio = _parsear_folio(celdas[1])
    if not folio:
        return None

    fecha, semana, anio = _parsear_fecha(celdas[0])

    dest  = _valor(celdas[10])
    desc  = _valor(celdas[9])
    dir_  = _valor(celdas[3])

    return {
        "folio":             folio,
        "destinatario":      (_valor(celdas[5]) or "").strip() or None,
        "telefono_receptor": _limpiar_telefono(_valor(celdas[6])),
        "telefono_emisor":   _limpiar_telefono(_valor(celdas[4])),
        "cantidad":          _parsear_cantidad(celdas[7], numero_fila),
        "descripcion":       desc.strip() if desc else None,
        # destino = col 11 (DEST) = sucursal receptora MEX
        "destino":           dest.strip().upper() if dest else None,
        "remitente":         (_valor(celdas[2]) or "").strip() or None,
        # domicilio_emisor = col 4 (Direccion del remitente USA)
        "domicilio_emisor":  dir_.strip() if dir_ else None,
        "domicilio_receptor":None,   # receptor en MEX, no hay en XML
        "peso":              _parsear_peso(celdas[8]),
        "fecha":             fecha,
        "semana":            semana,
        "anio":              anio,
        # origen_raw = DEST normalizado → para resolver sucursal_receptor
        # sucursal_emisor se resolverá por prefijo de folio en db_helpers
        "origen_raw":        _normalizar_nombre_sucursal(dest),
        "tipo_producto_id":  _determinar_tipo_producto(folio),
        "numero_fila":       numero_fila,
        "formato":           FORMATO_11,
    }


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def parsear_xml(ruta_archivo):
    """
    Lee un archivo SpreadsheetML y retorna lista de dicts normalizados.
    Detecta automáticamente FORMATO_9 (MEX→USA) o FORMATO_11 (USA→MEX).
    """
    logger.info(f"Iniciando parseo de: {ruta_archivo}")

    try:
        tree = ET.parse(ruta_archivo)
    except ET.ParseError as e:
        raise ValueError(f"Error al parsear el XML '{ruta_archivo}': {e}")

    root = tree.getroot()
    worksheet = root.find(f".//{SS}Worksheet")
    if worksheet is None:
        raise ValueError("No se encontró Worksheet en el XML.")

    table = worksheet.find(f"{SS}Table")
    if table is None:
        raise ValueError("No se encontró Table en el XML.")

    rows = table.findall(f"{SS}Row")
    logger.info(f"Total de filas encontradas (incluyendo header): {len(rows)}")

    formato   = _detectar_formato(rows)
    total_cols = 11 if formato == FORMATO_11 else 9

    registros = []
    numero_fila_xml    = 0
    vacias_consecutivas = 0

    for row in rows:
        numero_fila_xml += 1
        idx_fila = row.get(f"{SS}Index")
        if idx_fila is not None:
            numero_fila_xml = int(idx_fila)

        if numero_fila_xml == 1:
            continue   # saltar header

        celdas = _extraer_celdas_de_fila(row, total_cols)

        # Detectar fila vacía (ghost row de Excel)
        fila_vacia = all(_valor(c) is None for c in celdas)
        if fila_vacia:
            vacias_consecutivas += 1
            if vacias_consecutivas >= MAX_VACIAS_CONSECUTIVAS:
                logger.info(
                    f"Fila {numero_fila_xml}: {vacias_consecutivas} filas vacías "
                    f"consecutivas. Fin de datos."
                )
                break
            continue

        vacias_consecutivas = 0

        # Omitir filas que parezcan headers secundarios (textos de columna, no datos)
        valores_fila = [str(_valor(c)).strip().upper() for c in celdas if _valor(c)]
        es_header_secundario = (
            "REMITENTE" in valores_fila and "DESTINATARIO" in valores_fila
        ) or (
            _valor(celdas[0]) and str(_valor(celdas[0])).strip().upper() in
            ("FECHA", "DATE", "NO. PAQ.", "NO PAQ", "# ENV", "#ENV", "PAQ.")
        )
        if es_header_secundario:
            logger.info(f"Fila {numero_fila_xml}: header secundario omitido.")
            continue

        registro = (
            _extraer_registro_formato11(celdas, numero_fila_xml)
            if formato == FORMATO_11
            else _extraer_registro_formato9(celdas, numero_fila_xml)
        )

        if registro is None:
            logger.warning(f"Fila {numero_fila_xml}: sin folio, se omite.")
            continue

        registros.append(registro)

    logger.info(f"Parseo completado: {len(registros)} registros (formato={formato}).")
    return registros