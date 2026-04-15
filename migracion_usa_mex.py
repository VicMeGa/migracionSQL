#!/usr/bin/env python3
# =============================================================================
# migracion_usa_mex.py — Script B: Migración USA → MEX
#
# Uso:
#   python migracion_usa_mex.py /ruta/al/archivo.xml
#
# Contexto:
#   - pais_origen  = 'usa'
#   - origen de clientes nuevos = 'usa'
#   - Etiquetas ambiguas resuelven hacia sucursales USA
#   - Actualiza envios_usa_sequence al finalizar
#
# Antes de ejecutar:
#   1. Completa todos los IDs en config.py (NOMBRE_A_SUCURSAL_ID)
#   2. Configura DB_CONFIG y USER_ID_IMPORTACION en config.py
#   3. pip install PyMySQL
# =============================================================================

import sys
import logging
import psycopg2
import psycopg2.extras

from config import DB_CONFIG, USER_ID_IMPORTACION, ETIQUETAS_AMBIGUAS_USA
from xml_parser import parsear_xml
from db_helpers import (
    reset_contador_sin_etiqueta,
    resolver_sucursal_id,
    resolver_sucursal_destino,
    first_or_create_cliente,
    insertar_envio,
    insertar_paquete,
)
from sync_sequences import ejecutar_sincronizacion_completa

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DE LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("migracion_usa_mex.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("migracion_usa_mex")

# -----------------------------------------------------------------------------
# CONSTANTES DEL SCRIPT
# -----------------------------------------------------------------------------
PAIS_ORIGEN    = "usa"
ORIGEN_CLIENTE = "usa"
ETIQUETAS_CTX  = ETIQUETAS_AMBIGUAS_USA   # diccionario de desambiguación USA


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def migrar(ruta_xml):
    """
    Ejecuta la migración completa desde el archivo XML.

    Flujo por registro:
        1. Resolver sucursal_emisor_id (origen del envío)
        2. Resolver sucursal_receptor_id (destino del envío)
        3. firstOrCreate cliente_receptor
        4. firstOrCreate cliente_emisor
        5. INSERT envio → capturar envio_id
        6. INSERT paquete (1 por envío, descripción completa)
        7. commit individual por fila

    Al finalizar:
        - Ejecutar sincronización de los 3 contadores de secuencia.
        - Imprimir resumen de resultados.
    """
    # Validación de configuración crítica
    if USER_ID_IMPORTACION is None:
        logger.error(
            "USER_ID_IMPORTACION es None. "
            "Edita config.py antes de ejecutar."
        )
        sys.exit(1)

    # ── Parseo del XML ────────────────────────────────────────────────────
    logger.info(f"Leyendo XML: {ruta_xml}")
    try:
        registros = parsear_xml(ruta_xml)
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"No se pudo parsear el XML: {e}")
        sys.exit(1)

    if not registros:
        logger.warning("El XML no contiene registros válidos. Abortando.")
        sys.exit(0)

    logger.info(f"Total de registros a procesar: {len(registros)}")

    # ── Conexión a la base de datos ───────────────────────────────────────
    try:
        conexion = psycopg2.connect(
            host     = DB_CONFIG["host"],
            port     = DB_CONFIG["port"],
            user     = DB_CONFIG["user"],
            password = DB_CONFIG["password"],
            dbname   = DB_CONFIG["database"],
            options  = "-c client_encoding=UTF8",
        )
        conexion.autocommit = False
    except psycopg2.Error as e:
        logger.error(f"No se pudo conectar a la base de datos: {e}")
        sys.exit(1)

    logger.info("Conexión a BD establecida.")

    # ── Contadores de resumen ─────────────────────────────────────────────
    total_exitosos        = 0
    total_errores         = 0
    clientes_creados      = 0
    clientes_reutilizados = 0
    filas_con_error       = []

    # Reiniciar el contador global de la Regla de los 200
    reset_contador_sin_etiqueta()

    # ── Loop principal ────────────────────────────────────────────────────
    for registro in registros:
        fila = registro["numero_fila"]
        cursor = conexion.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute("SAVEPOINT sp_fila")
            # ── 1. Resolver sucursal emisor ───────────────────────────────
            sucursal_emisor_id = resolver_sucursal_id(registro, ETIQUETAS_CTX)

            # ── 2. Resolver sucursal receptor ─────────────────────────────
            # resolver_sucursal_destino nunca retorna None — fallback a DESCONOCIDO
            sucursal_receptor_id = resolver_sucursal_destino(registro["destino"])

            # ── 3. firstOrCreate cliente receptor ────────────────────────
            cliente_receptor_id, creado_r = first_or_create_cliente(
                cursor,
                nombre     = registro["destinatario"],
                telefono   = registro.get("telefono_receptor"),
                origen     = ORIGEN_CLIENTE,
                sucursal_id= sucursal_receptor_id or sucursal_emisor_id,
            )
            if creado_r:
                clientes_creados += 1
            else:
                clientes_reutilizados += 1

            # ── 4. firstOrCreate cliente emisor ──────────────────────────
            cliente_emisor_id, creado_e = first_or_create_cliente(
                cursor,
                nombre     = registro["remitente"],
                telefono   = registro.get("telefono_emisor"),
                origen     = ORIGEN_CLIENTE,
                sucursal_id= sucursal_emisor_id,
            )
            if creado_e:
                clientes_creados += 1
            else:
                clientes_reutilizados += 1

            # ── 5. Insertar envío ─────────────────────────────────────────
            datos_envio = {
                "folio":            registro["folio"],
                "pais_origen":      PAIS_ORIGEN,
                "semana":           registro["semana"],
                "anio":             registro["anio"],
                "tipo_producto_id": registro["tipo_producto_id"],
                "cantidad_paquetes":registro["cantidad"],
                "sucursal_emisor":  sucursal_emisor_id,
                "sucursal_receptor":sucursal_receptor_id,
                "cliente_emisor":   cliente_emisor_id,
                "cliente_receptor": cliente_receptor_id,
                "domicilio_emisor":  registro.get("domicilio_emisor"),
                "domicilio_receptor": None,
                "peso":             registro.get("peso", 0.00),
                "creado_por":       USER_ID_IMPORTACION,
            }
            envio_id = insertar_envio(cursor, datos_envio)

            # ── 6. Insertar paquete (1 por envío) ─────────────────────────
            insertar_paquete(
                cursor,
                envio_id    = envio_id,
                descripcion = registro["descripcion"],
                cantidad    = registro["cantidad"],
            )

            # ── 7. Commit individual ──────────────────────────────────────
            cursor.execute("RELEASE SAVEPOINT sp_fila")
            conexion.commit()
            total_exitosos += 1

            logger.debug(
                f"Fila {fila} OK | folio={registro['folio']} | "
                f"envio_id={envio_id} | "
                f"receptor={'NUEVO' if creado_r else 'existente'} | "
                f"emisor={'NUEVO' if creado_e else 'existente'}"
            )

        except (psycopg2.Error, ValueError, KeyError) as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_fila")
            total_errores += 1
            filas_con_error.append({
                "fila":  fila,
                "folio": registro.get("folio"),
                "error": str(e),
            })
            logger.error(
                f"Fila {fila} ERROR (folio={registro.get('folio')}): {e}"
            )

        finally:
            cursor.close()

    # ── Sincronización de secuencias ──────────────────────────────────────
    logger.info("")
    logger.info("Iniciando sincronización de secuencias ...")
    try:
        ejecutar_sincronizacion_completa(conexion)
    except Exception as e:
        logger.error(
            f"ADVERTENCIA: La sincronización de secuencias falló: {e}. "
            f"Ejecuta sync_sequences.py manualmente para corregirlo."
        )

    conexion.close()

    # ── Reporte final ─────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN DE MIGRACIÓN USA → MEX")
    logger.info("=" * 60)
    logger.info(f"  Registros procesados : {len(registros)}")
    logger.info(f"  Envíos insertados    : {total_exitosos}")
    logger.info(f"  Errores              : {total_errores}")
    logger.info(f"  Clientes creados     : {clientes_creados}")
    logger.info(f"  Clientes reutilizados: {clientes_reutilizados}")

    if filas_con_error:
        logger.info("")
        logger.info("FILAS CON ERROR:")
        for err in filas_con_error:
            logger.info(
                f"  Fila {err['fila']:>4} | folio={err['folio']:<12} | {err['error']}"
            )

    logger.info("=" * 60)
    logger.info("Migración finalizada.")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python migracion_usa_mex.py /ruta/al/archivo.xml")
        sys.exit(1)

    ruta = sys.argv[1]
    migrar(ruta)