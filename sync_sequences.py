# =============================================================================
# sync_sequences.py — Sincronización de secuencias post-migración
#
# Ejecutar SIEMPRE al final de cada script de migración.
# Actualiza los 3 contadores para que Laravel no colisione con los
# folios importados al generar nuevos envíos.
#
# Contadores a actualizar:
#   1. sucursales_folios.ultimo_folio  → MAX del número de folio por sucursal
#   2. envios_mex_sequence.last_value  → MAX(folio_pais) de envíos MEX
#   3. envios_usa_sequence.last_value  → MAX(folio_pais) de envíos USA
# =============================================================================

import logging

logger = logging.getLogger(__name__)


def sincronizar_sucursales_folios(cursor):
    """
    Actualiza sucursales_folios.ultimo_folio con el valor máximo
    del número extraído de los folios importados, por sucursal.

    Lógica SQL:
      - Extrae la parte numérica del folio con REGEXP_REPLACE.
      - Ignora folios temporales ('TEMP-%').
      - Usa GREATEST para no retroceder un contador ya existente.

    Retorna el número de sucursales actualizadas.
    """
    logger.info("Sincronizando sucursales_folios.ultimo_folio ...")

    sql = """
        UPDATE sucursales_folios sf
        SET ultimo_folio = GREATEST(
            COALESCE(sf.ultimo_folio, 0),
            COALESCE(m.max_folio, 0)
        )
        FROM (
            SELECT
                sucursal_emisor,
                MAX(
                    NULLIF(
                        REGEXP_REPLACE(folio, '[^0-9]', '', 'g'),
                        ''
                    )::BIGINT
                ) AS max_folio
            FROM envios
            WHERE folio NOT LIKE 'TEMP-%'
              AND folio IS NOT NULL
              AND folio ~ '[0-9]'
            GROUP BY sucursal_emisor
        ) m
        WHERE sf.sucursal_id = m.sucursal_emisor
    """
    cursor.execute(sql)
    filas_afectadas = cursor.rowcount
    logger.info(
        f"sucursales_folios actualizado: {filas_afectadas} sucursal(es) modificada(s)."
    )
    return filas_afectadas


def sincronizar_mex_sequence(cursor):
    """
    Actualiza envios_mex_sequence.last_value con el MAX(folio_pais)
    de los envíos con pais_origen = 'mex'.

    COALESCE protege contra el caso en que no existan envíos MEX aún
    (retorna el valor actual sin retrocederlo).
    """
    logger.info("Sincronizando envios_mex_sequence.last_value ...")

    sql = """
        UPDATE envios_mex_sequence
        SET last_value = (
            SELECT COALESCE(
                MAX(folio_pais),
                (SELECT last_value FROM envios_mex_sequence LIMIT 1)
            )
            FROM envios
            WHERE pais_origen = 'mex'
              AND folio_pais IS NOT NULL
        )
    """
    cursor.execute(sql)
    logger.info(
        f"envios_mex_sequence actualizado: {cursor.rowcount} fila(s) modificada(s)."
    )


def sincronizar_usa_sequence(cursor):
    """
    Actualiza envios_usa_sequence.last_value con el MAX(folio_pais)
    de los envíos con pais_origen = 'usa'.
    """
    logger.info("Sincronizando envios_usa_sequence.last_value ...")

    sql = """
        UPDATE envios_usa_sequence
        SET last_value = (
            SELECT COALESCE(
                MAX(folio_pais),
                (SELECT last_value FROM envios_usa_sequence LIMIT 1)
            )
            FROM envios
            WHERE pais_origen = 'usa'
              AND folio_pais IS NOT NULL
        )
    """
    cursor.execute(sql)
    logger.info(
        f"envios_usa_sequence actualizado: {cursor.rowcount} fila(s) modificada(s)."
    )


def ejecutar_sincronizacion_completa(conexion):
    """
    Punto de entrada principal. Ejecuta los 3 UPDATEs dentro de
    una transacción única. Si cualquiera falla, hace rollback total.

    Args:
        conexion: Objeto de conexión MySQLdb/PyMySQL activo.
    """
    logger.info("=" * 60)
    logger.info("INICIANDO SINCRONIZACIÓN POST-MIGRACIÓN")
    logger.info("=" * 60)

    cursor = conexion.cursor()
    try:
        sincronizar_sucursales_folios(cursor)
        sincronizar_mex_sequence(cursor)
        sincronizar_usa_sequence(cursor)

        conexion.commit()
        logger.info("✅ Sincronización completada y confirmada (commit).")

    except Exception as e:
        conexion.rollback()
        logger.error(
            f"❌ Error durante la sincronización: {e}. "
            f"Se realizó rollback. Los contadores NO fueron actualizados."
        )
        raise

    finally:
        cursor.close()