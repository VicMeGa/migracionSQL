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
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

NS = "urn:schemas-microsoft-com:office:spreadsheet"
SS = f"{{{NS}}}"

# Identificadores de formato
FORMATO_9  = "MEX_USA"   # 9 columnas
FORMATO_11 = "USA_MEX"   # 11 columnas

MAX_VACIAS_CONSECUTIVAS = 10


# =============================================================================
# FUNCIONES DE UTILIDAD (compartidas por ambos formatos)
# =============================================================================

def _extraer_celdas_de_fila(row_element, total_columnas):
    """
    Extrae valores de una fila respetando ss:Index (columnas saltadas = None).
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
                celdas[posicion_actual - 1] = data_el.text

        posicion_actual += 1

    return celdas


def _limpiar_telefono(valor):
    """Extrae el primer teléfono si hay varios separados por coma."""
    if not valor:
        return None
    # Algunos campos de teléfono contienen texto como "CLIENTE AVISA" → descartar
    valor_str = str(valor).strip()
    if not any(c.isdigit() for c in valor_str):
        return None
    primer_tel = valor_str.split(",")[0].strip()
    return primer_tel if primer_tel else None


def _parsear_fecha(valor):
    """
    Parsea fecha en múltiples formatos:
      - ISO SpreadsheetML: "2025-09-08T00:00:00.000"
      - Fecha corta USA:   "12/31/2025"  o  "9/27/2025"
    Retorna (datetime, semana_iso, anio_iso) o (None, None, None).
    """
    if not valor:
        return None, None, None

    valor_str = str(valor).strip()

    # Formato ISO (formato MEX→USA)
    if "T" in valor_str:
        try:
            dt = datetime.strptime(valor_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
            iso = dt.isocalendar()
            return dt, iso[1], iso[0]
        except ValueError:
            pass

    # Formato M/D/YYYY o MM/DD/YYYY (formato USA→MEX)
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(valor_str, fmt)
            iso = dt.isocalendar()
            return dt, iso[1], iso[0]
        except ValueError:
            continue

    logger.warning(f"No se pudo parsear fecha '{valor_str}'")
    return None, None, None


def _determinar_tipo_producto(folio):
    """ID 12 si folio empieza con 'C-', ID 13 para todo lo demás."""
    if folio and str(folio).upper().startswith("C-"):
        return 12
    return 13


def _normalizar_nombre_sucursal(nombre):
    """
    Normaliza variante del XML al nombre oficial en BD usando ALIASES_NOMBRE.
    """
    from config import ALIASES_NOMBRE
    if not nombre:
        return None
    nombre_upper = str(nombre).strip().upper()
    return ALIASES_NOMBRE.get(nombre_upper, nombre_upper)


def _limpiar_folio(folio_raw):
    """Convierte '957.0' → '957', mantiene 'B102', 'T48', etc."""
    if folio_raw is None:
        return None
    folio = str(folio_raw).strip()
    if re.match(r"^\d+\.0$", folio):
        folio = folio.split(".")[0]
    return folio if folio else None


# =============================================================================
# DETECCIÓN DE FORMATO
# =============================================================================

def _detectar_formato(rows):
    """
    Lee la primera fila (header) y determina el formato del archivo.

    FORMATO_9  detectado por: primera celda contiene 'PAQ', 'NO', 'NUM'
    FORMATO_11 detectado por: primera celda contiene 'FECHA'

    Returns:
        str: FORMATO_9 o FORMATO_11
    """
    if not rows:
        raise ValueError("El archivo XML no contiene filas.")

    celdas_header = _extraer_celdas_de_fila(rows[0], 12)
    # Limpiar y unir las primeras celdas para análisis
    header_vals = [
        str(c).strip().upper() for c in celdas_header if c is not None
    ]

    logger.info(f"Header detectado: {header_vals[:6]}")

    # FORMATO_11: primera columna es "FECHA"
    if header_vals and header_vals[0] in ("FECHA", "DATE"):
        logger.info("Formato detectado: FORMATO_11 (USA → MEX, 11 columnas)")
        return FORMATO_11

    # FORMATO_9: primera columna es el número de paquete
    logger.info("Formato detectado: FORMATO_9 (MEX → USA, 9 columnas)")
    return FORMATO_9


# =============================================================================
# EXTRACTORES POR FORMATO
# =============================================================================

def _extraer_registro_formato9(celdas, numero_fila):
    """
    Extrae un registro del formato MEX→USA (9 columnas).
    Col: 1:Folio  2:Destinatario  3:Tel  4:Cant  5:Desc  6:Destino  7:Remitente  8:Fecha  9:Origen
    """
    folio_raw    = celdas[0]
    destinatario = celdas[1]
    telefono_raw = celdas[2]
    cantidad_raw = celdas[3]
    descripcion  = celdas[4]
    destino_raw  = celdas[5]
    remitente    = celdas[6]
    fecha_raw    = celdas[7]
    origen_raw   = celdas[8]

    folio = _limpiar_folio(folio_raw)
    if not folio:
        return None

    fecha, semana, anio = _parsear_fecha(fecha_raw)

    return {
        "folio":             folio,
        "destinatario":      destinatario.strip() if destinatario else None,
        "telefono_receptor": _limpiar_telefono(telefono_raw),
        "telefono_emisor":   None,   # no disponible en este formato
        "cantidad":          _parsear_cantidad(cantidad_raw, numero_fila),
        "descripcion":       descripcion.strip() if descripcion else None,
        "destino":           destino_raw.strip().upper() if destino_raw else None,
        "remitente":         remitente.strip() if remitente else None,
        "domicilio_emisor":  None,   # no disponible en este formato
        "peso":              0.00,   # no disponible en este formato
        "fecha":             fecha,
        "semana":            semana,
        "anio":              anio,
        "origen_raw":        _normalizar_nombre_sucursal(origen_raw),
        "tipo_producto_id":  _determinar_tipo_producto(folio),
        "numero_fila":       numero_fila,
        "formato":           FORMATO_9,
    }


def _extraer_registro_formato11(celdas, numero_fila):
    """
    Extrae un registro del formato USA→MEX (11 columnas).
    Col: 1:Fecha  2:#ENV  3:Remitente  4:Direccion  5:Tel(emisor)
         6:Destinatario  7:Tel(receptor)  8:Paq  9:LB  10:Descripcion  11:DEST
    """
    fecha_raw    = celdas[0]
    folio_raw    = celdas[1]
    remitente    = celdas[2]
    direccion    = celdas[3]
    tel_emisor   = celdas[4]
    destinatario = celdas[5]
    tel_receptor = celdas[6]
    cantidad_raw = celdas[7]
    peso_raw     = celdas[8]
    descripcion  = celdas[9]
    dest_raw     = celdas[10]

    folio = _limpiar_folio(folio_raw)
    if not folio:
        return None

    fecha, semana, anio = _parsear_fecha(fecha_raw)

    # Peso: puede venir como "13.0" o "13" → float
    try:
        peso = float(peso_raw) if peso_raw else 0.00
    except (ValueError, TypeError):
        peso = 0.00

    return {
        "folio":             folio,
        "destinatario":      destinatario.strip() if destinatario else None,
        "telefono_receptor": _limpiar_telefono(tel_receptor),
        "telefono_emisor":   _limpiar_telefono(tel_emisor),
        "cantidad":          _parsear_cantidad(cantidad_raw, numero_fila),
        "descripcion":       descripcion.strip() if descripcion else None,
        "destino":           dest_raw.strip().upper() if dest_raw else None,
        "remitente":         remitente.strip() if remitente else None,
        "domicilio_emisor":  direccion.strip() if direccion else None,
        "peso":              peso,
        "fecha":             fecha,
        "semana":            semana,
        "anio":              anio,
        "origen_raw":        _normalizar_nombre_sucursal(dest_raw),  # DEST = sucursal MEX destino
        "tipo_producto_id":  _determinar_tipo_producto(folio),
        "numero_fila":       numero_fila,
        "formato":           FORMATO_11,
    }


def _parsear_cantidad(cantidad_raw, numero_fila):
    """Convierte cantidad a int, default 1."""
    try:
        return int(float(cantidad_raw)) if cantidad_raw else 1
    except (ValueError, TypeError):
        logger.warning(f"Fila {numero_fila}: cantidad inválida '{cantidad_raw}', se usa 1.")
        return 1


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def parsear_xml(ruta_archivo):
    """
    Lee un archivo SpreadsheetML y retorna lista de dicts normalizados.

    Detecta automáticamente FORMATO_9 (MEX→USA) o FORMATO_11 (USA→MEX).

    Campos comunes del dict resultante:
        folio, destinatario, telefono_receptor, telefono_emisor,
        cantidad, descripcion, destino, remitente,
        domicilio_emisor, peso, fecha, semana, anio,
        origen_raw, tipo_producto_id, numero_fila, formato

    Returns:
        list[dict]
    """
    logger.info(f"Iniciando parseo de: {ruta_archivo}")

    try:
        tree = ET.parse(ruta_archivo)
    except ET.ParseError as e:
        raise ValueError(f"Error al parsear el XML '{ruta_archivo}': {e}")

    root = tree.getroot()

    worksheet = root.find(f".//{SS}Worksheet")
    if worksheet is None:
        raise ValueError("No se encontró ninguna hoja (Worksheet) en el XML.")

    table = worksheet.find(f"{SS}Table")
    if table is None:
        raise ValueError("No se encontró la tabla (Table) en la hoja.")

    rows = table.findall(f"{SS}Row")
    logger.info(f"Total de filas encontradas (incluyendo header): {len(rows)}")

    # Detectar formato leyendo el header (fila 1)
    formato = _detectar_formato(rows)
    total_cols = 11 if formato == FORMATO_11 else 9

    registros = []
    numero_fila_xml = 0
    vacias_consecutivas = 0

    for row in rows:
        numero_fila_xml += 1

        idx_fila = row.get(f"{SS}Index")
        if idx_fila is not None:
            numero_fila_xml = int(idx_fila)

        # Saltar header (fila 1)
        if numero_fila_xml == 1:
            continue

        celdas = _extraer_celdas_de_fila(row, total_cols)

        # Detección de filas vacías (ghost rows de Excel)
        fila_vacia = all(c is None for c in celdas)
        if fila_vacia:
            vacias_consecutivas += 1
            if vacias_consecutivas >= MAX_VACIAS_CONSECUTIVAS:
                logger.info(
                    f"Fila {numero_fila_xml}: {vacias_consecutivas} filas vacías "
                    f"consecutivas. Fin de datos — deteniendo parseo."
                )
                break
            continue

        vacias_consecutivas = 0

        # Extraer según formato
        if formato == FORMATO_11:
            registro = _extraer_registro_formato11(celdas, numero_fila_xml)
        else:
            registro = _extraer_registro_formato9(celdas, numero_fila_xml)

        if registro is None:
            logger.warning(
                f"Fila {numero_fila_xml}: sin folio, se omite."
            )
            continue

        registros.append(registro)

    logger.info(f"Parseo completado: {len(registros)} registros válidos (formato={formato}).")
    return registros