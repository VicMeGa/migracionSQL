#!/usr/bin/env python3
# =============================================================================
# limpiar_duplicados.py — Eliminación segura de registros duplicados exactos
#
# Un duplicado exacto es cuando coinciden:
#   folio + semana + anio + cliente_emisor + cliente_receptor +
#   cantidad_paquetes + sucursal_emisor + sucursal_receptor
#
# Conserva siempre el registro con el ID más bajo (el primero insertado).
#
# Uso:
#   python limpiar_duplicados.py           ← modo simulación (no borra nada)
#   python limpiar_duplicados.py --ejecutar ← borra los duplicados
# =============================================================================

import sys
import logging
import psycopg2
import psycopg2.extras

sys.path.insert(0, '.')
from config import DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("limpiar_duplicados")

# -----------------------------------------------------------------------------
# SQL
# -----------------------------------------------------------------------------

SQL_CONTAR_GRUPOS = """
    SELECT COUNT(*) AS grupos
    FROM (
        SELECT folio, semana, anio,
               cliente_emisor, cliente_receptor,
               cantidad_paquetes, sucursal_emisor, sucursal_receptor
        FROM envios
        GROUP BY folio, semana, anio,
                 cliente_emisor, cliente_receptor,
                 cantidad_paquetes, sucursal_emisor, sucursal_receptor
        HAVING COUNT(*) > 1
    ) sub
"""

SQL_CONTAR_REGISTROS_A_ELIMINAR = """
    SELECT COUNT(*) AS total
    FROM envios
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM envios
        GROUP BY folio, semana, anio,
                 cliente_emisor, cliente_receptor,
                 cantidad_paquetes, sucursal_emisor, sucursal_receptor
    )
"""

SQL_CONTAR_PAQUETES_A_ELIMINAR = """
    SELECT COUNT(*) AS total
    FROM paquetes
    WHERE envio_id IN (
        SELECT id FROM envios
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM envios
            GROUP BY folio, semana, anio,
                     cliente_emisor, cliente_receptor,
                     cantidad_paquetes, sucursal_emisor, sucursal_receptor
        )
    )
"""

SQL_ELIMINAR_PAQUETES = """
    DELETE FROM paquetes
    WHERE envio_id IN (
        SELECT id FROM envios
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM envios
            GROUP BY folio, semana, anio,
                     cliente_emisor, cliente_receptor,
                     cantidad_paquetes, sucursal_emisor, sucursal_receptor
        )
    )
"""

SQL_ELIMINAR_ENVIOS = """
    DELETE FROM envios
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM envios
        GROUP BY folio, semana, anio,
                 cliente_emisor, cliente_receptor,
                 cantidad_paquetes, sucursal_emisor, sucursal_receptor
    )
"""

SQL_DETALLE_GRUPOS = """
    SELECT
        e.folio, e.semana, e.anio,
        COUNT(*) AS copias,
        MIN(e.id) AS id_conservar,
        MAX(e.id) AS id_eliminar,
        ce.nombre_completo AS emisor,
        cr.nombre_completo AS receptor
    FROM envios e
    LEFT JOIN clientes ce ON ce.id = e.cliente_emisor
    LEFT JOIN clientes cr ON cr.id = e.cliente_receptor
    GROUP BY e.folio, e.semana, e.anio,
             e.cliente_emisor, e.cliente_receptor,
             e.cantidad_paquetes, e.sucursal_emisor, e.sucursal_receptor,
             ce.nombre_completo, cr.nombre_completo
    HAVING COUNT(*) > 1
    ORDER BY e.semana, e.anio, e.folio
    LIMIT 20
"""

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    modo_ejecucion = "--ejecutar" in sys.argv

    logger.info("=" * 60)
    logger.info(f"LIMPIEZA DE DUPLICADOS — modo={'EJECUCIÓN' if modo_ejecucion else 'SIMULACIÓN (dry-run)'}")
    logger.info("=" * 60)

    if not modo_ejecucion:
        logger.info("")
        logger.info("⚠️  Ejecutando en modo SIMULACIÓN.")
        logger.info("   Para borrar los duplicados usa: python limpiar_duplicados.py --ejecutar")
        logger.info("")

    # Conectar
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            dbname=DB_CONFIG["database"],
        )
        conn.autocommit = False
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        sys.exit(1)

    try:
        # ── Diagnóstico previo ────────────────────────────────────────────
        cursor.execute(SQL_CONTAR_GRUPOS)
        grupos = cursor.fetchone()["grupos"]

        cursor.execute(SQL_CONTAR_REGISTROS_A_ELIMINAR)
        envios_a_eliminar = cursor.fetchone()["total"]

        cursor.execute(SQL_CONTAR_PAQUETES_A_ELIMINAR)
        paquetes_a_eliminar = cursor.fetchone()["total"]

        logger.info(f"  Grupos duplicados detectados:  {grupos}")
        logger.info(f"  Envíos a eliminar:             {envios_a_eliminar}")
        logger.info(f"  Paquetes a eliminar:           {paquetes_a_eliminar}")

        if grupos == 0:
            logger.info("")
            logger.info("✅ No hay duplicados en la base de datos. Nada que limpiar.")
            return

        # Mostrar muestra de los primeros 20 grupos
        logger.info("")
        logger.info("Muestra de los primeros 20 grupos a limpiar:")
        logger.info(f"  {'Folio':<10} {'Sem':<5} {'Año':<6} {'Copias':<8} {'ID conservar':<14} {'ID eliminar':<12} {'Emisor':<25} {'Receptor'}")
        logger.info("  " + "-" * 110)
        cursor.execute(SQL_DETALLE_GRUPOS)
        for row in cursor.fetchall():
            logger.info(
                f"  {str(row['folio']):<10} {str(row['semana']):<5} {str(row['anio']):<6} "
                f"{str(row['copias']):<8} {str(row['id_conservar']):<14} {str(row['id_eliminar']):<12} "
                f"{str(row['emisor'] or '')[:24]:<25} {str(row['receptor'] or '')[:30]}"
            )

        if not modo_ejecucion:
            logger.info("")
            logger.info("── SIMULACIÓN COMPLETADA ──────────────────────────────────")
            logger.info(f"  Se eliminarían {envios_a_eliminar} envíos y {paquetes_a_eliminar} paquetes.")
            logger.info(f"  Para ejecutar: python limpiar_duplicados.py --ejecutar")
            return

        # ── Ejecución real ────────────────────────────────────────────────
        logger.info("")
        logger.info("Iniciando eliminación...")

        cursor.execute("SAVEPOINT antes_limpieza")

        # Paso 1: paquetes
        cursor.execute(SQL_ELIMINAR_PAQUETES)
        paquetes_eliminados = cursor.rowcount
        logger.info(f"  Paso 1 ✅ Paquetes eliminados: {paquetes_eliminados}")

        # Paso 2: envíos
        cursor.execute(SQL_ELIMINAR_ENVIOS)
        envios_eliminados = cursor.rowcount
        logger.info(f"  Paso 2 ✅ Envíos eliminados:   {envios_eliminados}")

        # Verificar que quedaron cero duplicados
        cursor.execute(SQL_CONTAR_GRUPOS)
        grupos_restantes = cursor.fetchone()["grupos"]

        if grupos_restantes == 0:
            conn.commit()
            logger.info("")
            logger.info("=" * 60)
            logger.info("✅ LIMPIEZA COMPLETADA Y CONFIRMADA")
            logger.info(f"   Envíos eliminados:   {envios_eliminados}")
            logger.info(f"   Paquetes eliminados: {paquetes_eliminados}")
            logger.info(f"   Duplicados restantes: 0")
            logger.info("=" * 60)
        else:
            # Algo salió mal — rollback
            cursor.execute("ROLLBACK TO SAVEPOINT antes_limpieza")
            conn.commit()
            logger.error("")
            logger.error(f"❌ Verificación fallida: aún quedan {grupos_restantes} grupos duplicados.")
            logger.error("   Se realizó ROLLBACK. La base de datos no fue modificada.")
            sys.exit(1)

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error durante la limpieza: {e}")
        logger.error("   Se realizó ROLLBACK. La base de datos no fue modificada.")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()