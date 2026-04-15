# =============================================================================
# db_helpers.py — Operaciones de base de datos para la migración
#
# Responsabilidades:
#   - Resolver sucursal_id desde nombre o prefijo de folio
#   - firstOrCreate para clientes (receptor y emisor)
#   - Insertar envío y retornar su ID
#   - Insertar paquete (1 por envío)
# =============================================================================

import re
import logging
from config import (
    NOMBRE_A_SUCURSAL_ID,
    ETIQUETAS_UNICAS,
    SUCURSALES_SIN_ETIQUETA,
    BLOQUE_REGLA_200,
    DEFAULT_PESO,
    DEFAULT_PRECIO_TOTAL,
    DEFAULT_PAGO_EFECTIVO,
    DEFAULT_PAGO_TARJETA,
    DEFAULT_V_ESTIMADO,
    DEFAULT_IMPUESTO,
    DEFAULT_PRECIO,
    USER_ID_IMPORTACION,
)

logger = logging.getLogger(__name__)

# Contador global para la Regla de los 200
# Cada script lo inicializa en 0 antes de comenzar el loop principal
_contador_sin_etiqueta = 0


def reset_contador_sin_etiqueta():
    """Reinicia el contador de la Regla de los 200. Llamar antes del loop."""
    global _contador_sin_etiqueta
    _contador_sin_etiqueta = 0


def _lookup_sucursal_id(nombre_sucursal):
    """
    Busca el sucursal_id en el diccionario maestro.

    Args:
        nombre_sucursal (str): Nombre normalizado de la sucursal.

    Returns:
        int | None

    Raises:
        KeyError: Si el nombre existe en el dict pero tiene None (sin asignar).
        ValueError: Si el nombre no existe en el catálogo.
    """
    if nombre_sucursal not in NOMBRE_A_SUCURSAL_ID:
        raise ValueError(
            f"Sucursal '{nombre_sucursal}' no existe en NOMBRE_A_SUCURSAL_ID. "
            f"Verifica el catálogo en config.py."
        )
    sucursal_id = NOMBRE_A_SUCURSAL_ID[nombre_sucursal]
    if sucursal_id is None:
        raise KeyError(
            f"Sucursal '{nombre_sucursal}' existe en el catálogo pero su ID "
            f"no ha sido asignado (None). Edita NOMBRE_A_SUCURSAL_ID en config.py."
        )
    return sucursal_id


def _extraer_prefijo_folio(folio):
    """
    Extrae el prefijo alfabético de un folio.

    Casos:
        "T48"     → "T"
        "TJ-16"   → "TJ-"
        "957"     → None   (numérico puro)
        "C-T48"   → "C-"   (caja, pero la etiqueta real está después)
        "Q16"     → "Q"
        "A9"      → "A"
        "CM2"     → "CM"

    Returns:
        str | None
    """
    if not folio:
        return None

    folio_str = str(folio).strip()

    # Caso especial: folio de caja "C-XXX" → extraer el prefijo tras "C-"
    if folio_str.upper().startswith("C-"):
        resto = folio_str[2:]
        match = re.match(r"^([A-Za-z]+[-]?)", resto)
        if match:
            return match.group(1).upper()
        return None

    # Caso general: extraer letras iniciales + posible guión
    # Ejemplos: "TJ-16" → "TJ-", "T48" → "T", "CM2" → "CM"
    match = re.match(r"^([A-Za-z]+[-]?)", folio_str)
    if match:
        return match.group(1).upper()

    return None  # numérico puro


def resolver_sucursal_id(registro, etiquetas_contexto):
    """
    Determina el sucursal_id del EMISOR usando el siguiente orden de prioridad:

    1. Columna `origen_raw` del XML (nombre explícito de la sucursal).
    2. Prefijo del folio → buscar en etiquetas_contexto (dict ambiguo del script)
       y luego en ETIQUETAS_UNICAS.
    3. Regla de los 200: si folio es numérico puro y origen está vacío.

    Args:
        registro (dict): Fila normalizada del parser.
        etiquetas_contexto (dict): ETIQUETAS_AMBIGUAS_MEX o ETIQUETAS_AMBIGUAS_USA
                                   según el script que se ejecuta.

    Returns:
        int: sucursal_id resuelto.

    Raises:
        ValueError/KeyError: Si no se puede resolver y el ID no está configurado.
    """
    global _contador_sin_etiqueta

    folio    = registro["folio"]
    origen   = registro["origen_raw"]   # ya normalizado por el parser

    # ── Prioridad 1: columna Origen tiene valor ───────────────────────────
    if origen:
        try:
            return _lookup_sucursal_id(origen)
        except (KeyError, ValueError) as e:
            logger.warning(
                f"Fila {registro['numero_fila']}: origen '{origen}' no resuelto "
                f"({e}). Intentando por prefijo de folio."
            )

    # ── Prioridad 2: prefijo del folio ────────────────────────────────────
    prefijo = _extraer_prefijo_folio(folio)

    if prefijo:
        # Primero buscar en el diccionario de ambigüedades del script actual
        nombre_por_ambiguo = etiquetas_contexto.get(prefijo)
        if nombre_por_ambiguo:
            try:
                return _lookup_sucursal_id(nombre_por_ambiguo)
            except (KeyError, ValueError) as e:
                logger.warning(
                    f"Fila {registro['numero_fila']}: prefijo ambiguo '{prefijo}' "
                    f"→ '{nombre_por_ambiguo}' no resuelto ({e})."
                )

        # Luego buscar en etiquetas únicas (sin ambigüedad)
        nombre_por_unico = ETIQUETAS_UNICAS.get(prefijo)
        if nombre_por_unico:
            try:
                return _lookup_sucursal_id(nombre_por_unico)
            except (KeyError, ValueError) as e:
                logger.warning(
                    f"Fila {registro['numero_fila']}: prefijo único '{prefijo}' "
                    f"→ '{nombre_por_unico}' no resuelto ({e})."
                )

    # ── Prioridad 3: Regla de los 200 (folio numérico puro sin origen) ────
    bloque = _contador_sin_etiqueta // BLOQUE_REGLA_200
    _contador_sin_etiqueta += 1

    if bloque < len(SUCURSALES_SIN_ETIQUETA):
        nombre_bloque = SUCURSALES_SIN_ETIQUETA[bloque]
        logger.debug(
            f"Fila {registro['numero_fila']}: Regla 200 → bloque {bloque} "
            f"→ '{nombre_bloque}' (contador={_contador_sin_etiqueta - 1})"
        )
        return _lookup_sucursal_id(nombre_bloque)

    # Sin resolución posible → error controlado
    raise ValueError(
        f"Fila {registro['numero_fila']}: no se pudo resolver sucursal para "
        f"folio='{folio}', origen='{origen}'. "
        f"Bloque de Regla 200 ({bloque}) excede el catálogo disponible."
    )


def resolver_sucursal_destino(destino_raw):
    """
    Mapea el nombre de ciudad destino del XML al sucursal_id receptor.

    Estrategia:
      1. Normalizar a mayúsculas y buscar en ALIASES_NOMBRE (config.py).
      2. Si hay alias, buscar en NOMBRE_A_SUCURSAL_ID.
      3. Si no hay alias, intentar lookup directo en NOMBRE_A_SUCURSAL_ID.
      4. Si nada resuelve → retornar DESCONOCIDO (ID=48) para no romper FK.

    Args:
        destino_raw (str | None): Valor de la columna 6 del XML.

    Returns:
        int: sucursal_id siempre (nunca None para evitar FK NOT NULL).
    """
    from config import ALIASES_NOMBRE, NOMBRE_A_SUCURSAL_ID

    FALLBACK_ID = NOMBRE_A_SUCURSAL_ID["DESCONOCIDO"]  # ID=48

    if not destino_raw:
        return FALLBACK_ID

    destino = destino_raw.strip().upper()

    # Paso 1: buscar en aliases
    nombre_resuelto = ALIASES_NOMBRE.get(destino, destino)

    # Paso 2: buscar ID en catálogo maestro
    sucursal_id = NOMBRE_A_SUCURSAL_ID.get(nombre_resuelto)

    if sucursal_id:
        return sucursal_id

    # Paso 3: intentar lookup directo con el string original en mayúsculas
    sucursal_id = NOMBRE_A_SUCURSAL_ID.get(destino)
    if sucursal_id:
        return sucursal_id

    # Paso 4: fallback a DESCONOCIDO — registra warning pero nunca falla
    logger.warning(
        f"Destino '{destino_raw.strip()}' no resuelto en catálogo. "
        f"Se asigna DESCONOCIDO (ID={FALLBACK_ID})."
    )
    return FALLBACK_ID


# =============================================================================
# OPERACIONES DE BASE DE DATOS
# =============================================================================

def first_or_create_cliente(cursor, nombre, telefono, origen, sucursal_id):
    """
    Busca un cliente por nombre_completo + telefono_celular.
    Si existe, retorna su ID. Si no, lo inserta y retorna el nuevo ID.

    Args:
        cursor:      Cursor de base de datos activo.
        nombre:      nombre_completo del cliente.
        telefono:    telefono_celular del cliente.
        origen:      'mex' o 'usa'.
        sucursal_id: ID de la sucursal asociada al cliente.

    Returns:
        tuple(int, bool): (cliente_id, fue_creado)
    """
    # nombre_completo es NOT NULL en BD
    if not nombre:
        nombre = "SIN NOMBRE"

    # Búsqueda por nombre + teléfono
    # Normalizar teléfono: NOT NULL en BD → usar 'SIN DATOS' si no viene en XML
    telefono = telefono if telefono else 'SIN DATOS'

    cursor.execute(
        """
        SELECT id FROM clientes
        WHERE nombre_completo = %s
          AND telefono_celular = %s
        LIMIT 1
        """,
        (nombre, telefono)
    )
    row = cursor.fetchone()

    if row:
        return row["id"], False  # (id, fue_creado=False)

    # No existe → insertar con RETURNING para obtener el ID generado (psycopg2 / PostgreSQL)
    cursor.execute(
        """
        INSERT INTO clientes (
            nombre_completo,
            telefono_celular,
            origen,
            sucursal_id,
            estatus,
            direccion,
            creado_por,
            actualizado_por,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s,
            true,
            'SIN DATOS',
            %s, %s,
            NOW(), NOW()
        )
        RETURNING id
        """,
        (
            nombre,
            telefono,
            origen,
            sucursal_id,
            USER_ID_IMPORTACION,
            USER_ID_IMPORTACION,
        )
    )
    nuevo_id = cursor.fetchone()["id"]
    return nuevo_id, True  # (id, fue_creado=True)


def insertar_envio(cursor, datos):
    """
    Inserta un registro en la tabla `envios`.

    Args:
        cursor: Cursor de base de datos activo.
        datos (dict): Campos del envío. Claves esperadas:
            folio, pais_origen, semana, anio, tipo_producto_id,
            cantidad_paquetes, sucursal_emisor, sucursal_receptor,
            cliente_emisor, cliente_receptor,
            domicilio_emisor, domicilio_receptor,
            peso, precio_total, pago_efectivo, pago_tarjeta,
            is_entregado, creado_por

    Returns:
        int: ID del envío insertado (lastrowid).
    """
    cursor.execute(
        """
        INSERT INTO envios (
            folio,
            pais_origen,
            semana,
            anio,
            tipo_producto_id,
            cantidad_paquetes,
            sucursal_emisor,
            sucursal_receptor,
            cliente_emisor,
            cliente_receptor,
            domicilio_emisor,
            domicilio_receptor,
            peso,
            precio_total,
            pago_efectivo,
            pago_tarjeta,
            is_entregado,
            creado_por,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            NOW(), NOW()
        )
        RETURNING id
        """,
        (
            datos["folio"],
            datos["pais_origen"],
            datos.get("semana"),
            datos.get("anio"),
            datos["tipo_producto_id"],
            datos["cantidad_paquetes"],
            datos["sucursal_emisor"],
            datos["sucursal_receptor"],
            datos["cliente_emisor"],
            datos["cliente_receptor"],
            datos.get("domicilio_emisor") or 'SIN DATOS',
            datos.get("domicilio_receptor") or 'SIN DATOS',
            float(datos.get("peso") or DEFAULT_PESO),
            float(datos.get("precio_total",  DEFAULT_PRECIO_TOTAL)),
            float(datos.get("pago_efectivo", DEFAULT_PAGO_EFECTIVO)),
            float(datos.get("pago_tarjeta",  DEFAULT_PAGO_TARJETA)),
            False,                              # is_entregado = False por default
            datos["creado_por"],
        )
    )
    return cursor.fetchone()["id"]


def insertar_paquete(cursor, envio_id, descripcion, cantidad):
    """
    Inserta UN registro en la tabla `paquetes` para el envío dado.

    Regla confirmada: 1 paquete por envío.
    La descripción se guarda completa (con ~, códigos 1/T, 1/B, etc.).

    Args:
        cursor:      Cursor de base de datos activo.
        envio_id:    ID del envío padre (FK).
        descripcion: Texto completo de la descripción del XML.
        cantidad:    Número de bultos (columna 4 del XML).

    Returns:
        int: ID del paquete insertado.
    """
    # descripcion es NOT NULL en BD
    descripcion = descripcion if descripcion else "SIN DESCRIPCION"

    cursor.execute(
        """
        INSERT INTO paquetes (
            envio_id,
            descripcion,
            cantidad,
            peso,
            v_estimado,
            impuesto,
            precio,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            NOW(), NOW()
        )
        RETURNING id
        """,
        (
            envio_id,
            descripcion,          # string completo, solo .strip() aplicado
            cantidad,
            float(DEFAULT_PESO),
            float(DEFAULT_V_ESTIMADO),
            float(DEFAULT_IMPUESTO),
            float(DEFAULT_PRECIO),
        )
    )
    return cursor.fetchone()["id"]