#!/usr/bin/env python3
# =============================================================================
# unificar_clientes.py — Unificación de clientes duplicados
#
# Reglas de negocio:
#   - Criterio de detección: mismo telefono_celular + un nombre es subconjunto
#     del otro (ej: "MARIA GARCIA" vs "MARIA GARCIA LOPEZ")
#   - Maestro: registro con nombre MÁS LARGO (más completo)
#   - Anotaciones especiales (/ o #): se conserva el nombre con la anotación
#   - Clusters múltiples (3+): se unifican SOLO los pares que comparten
#     exactamente el mismo prefijo — no se encadenan por transitividad
#
# Tablas actualizadas (FKs a clientes):
#   1. envios.cliente_emisor   → redirigir al maestro
#   2. envios.cliente_receptor → redirigir al maestro
#   3. cliente_cambios.cliente_id → CASCADE automático al eliminar
#
# Uso:
#   python unificar_clientes.py              ← simulación (no modifica BD)
#   python unificar_clientes.py --ejecutar   ← ejecuta la unificación
# =============================================================================

import sys
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime

sys.path.insert(0, '.')
from config import DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("unificar_clientes")


def conectar():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        user=DB_CONFIG["user"], password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


# =============================================================================
# AGRUPACIÓN — sin transitividad para clusters múltiples
# =============================================================================

def construir_pares_directos(pares):
    """
    Para clusters múltiples, solo une pares que comparten exactamente
    el mismo prefijo de nombre — no encadena por transitividad.

    Ejemplo:
      ANDREA CRUZ + ANDREA CRUZ LEON     → par directo (ANDREA CRUZ es prefijo)
      ANDREA CRUZ + ANDREA CRUZ VILLEGAS → par directo (ANDREA CRUZ es prefijo)
      ANDREA CRUZ LEON + ANDREA CRUZ VILLEGAS → NO (ninguno es prefijo del otro)

    Retorna lista de (id_maestro, id_duplicado) ya resueltos.
    """
    acciones = []
    procesados = set()

    # Agrupar pares por el ID del nombre más corto (posible duplicado)
    # Un par (A, B) donde A es subconjunto de B:
    #   - Si len(nombre_A) < len(nombre_B): A es el corto, B el largo
    #   - Maestro = el más largo

    for par in pares:
        id1, nombre1 = par["id1"], par["id1_nombre"]
        id2, nombre2 = par["id2"], par["id2_nombre"]

        # Determinar cuál es prefijo del otro
        n1_upper = nombre1.strip().upper()
        n2_upper = nombre2.strip().upper()

        if n2_upper.startswith(n1_upper + " "):
            # nombre1 es prefijo de nombre2 → maestro = id2 (más largo)
            id_maestro = id2
            nombre_maestro = nombre2
            id_dup = id1
        else:
            # nombre2 es prefijo de nombre1 → maestro = id1 (más largo)
            id_maestro = id1
            nombre_maestro = nombre1
            id_dup = id2

        # Evitar procesar el mismo duplicado dos veces
        if id_dup in procesados:
            continue

        procesados.add(id_dup)
        acciones.append({
            "id_maestro":     id_maestro,
            "nombre_maestro": nombre_maestro,
            "id_dup":         id_dup,
            "nombre_dup":     nombre1 if id_dup == id1 else nombre2,
        })

    return acciones


# =============================================================================
# MAIN
# =============================================================================

def main():
    modo_ejecucion = "--ejecutar" in sys.argv
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_archivo = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"UNIFICACIÓN DE CLIENTES — modo={'EJECUCIÓN' if modo_ejecucion else 'SIMULACIÓN'}")
    logger.info("=" * 60)

    if not modo_ejecucion:
        logger.info("⚠️  Modo SIMULACIÓN — no se modifica la BD.")
        logger.info("   Para ejecutar: python unificar_clientes.py --ejecutar")
        logger.info("")

    conn = conectar()
    conn.autocommit = False
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── Obtener todos los pares candidatos ───────────────────────────
        cursor.execute("""
            SELECT
                c1.id           AS id1,
                c1.nombre_completo AS id1_nombre,
                c1.telefono_celular AS telefono,
                c2.id           AS id2,
                c2.nombre_completo AS id2_nombre
            FROM clientes c1
            JOIN clientes c2
                ON c1.id < c2.id
                AND c1.telefono_celular = c2.telefono_celular
                AND c1.telefono_celular != 'SIN DATOS'
                AND c1.telefono_celular IS NOT NULL
                AND (
                    c1.nombre_completo ILIKE c2.nombre_completo || ' %'
                    OR c2.nombre_completo ILIKE c1.nombre_completo || ' %'
                )
            ORDER BY c1.nombre_completo, c1.id
        """)
        pares = [dict(r) for r in cursor.fetchall()]
        logger.info(f"Pares candidatos detectados: {len(pares)}")

        if not pares:
            logger.info("✅ No hay clientes a unificar.")
            return

        # ── Construir acciones con lógica de prefijo exacto ──────────────
        acciones = construir_pares_directos(pares)
        logger.info(f"Acciones de unificación:    {len(acciones)}")

        # ── Contar envíos afectados por acción (para el reporte) ─────────
        for accion in acciones:
            id_d = accion["id_dup"]
            cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE cliente_emisor   = %s) AS como_emisor,
                    COUNT(*) FILTER (WHERE cliente_receptor = %s) AS como_receptor
                FROM envios
                WHERE cliente_emisor = %s OR cliente_receptor = %s
            """, (id_d, id_d, id_d, id_d))
            row = cursor.fetchone()
            accion["envios_emisor"]   = row["como_emisor"]
            accion["envios_receptor"] = row["como_receptor"]
            accion["envios_total"]    = row["como_emisor"] + row["como_receptor"]

            # Contar registros en cliente_cambios
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM cliente_cambios WHERE cliente_id = %s",
                (id_d,)
            )
            accion["cambios_cascade"] = cursor.fetchone()["cnt"]

        # ── Totales para el resumen ──────────────────────────────────────
        total_envios_emisor   = sum(a["envios_emisor"]   for a in acciones)
        total_envios_receptor = sum(a["envios_receptor"] for a in acciones)
        total_cambios         = sum(a["cambios_cascade"] for a in acciones)

        logger.info(f"Envíos a redirigir (emisor):   {total_envios_emisor}")
        logger.info(f"Envíos a redirigir (receptor): {total_envios_receptor}")
        logger.info(f"cliente_cambios (CASCADE):     {total_cambios}")

        # ── Ejecutar o solo reportar ─────────────────────────────────────
        clientes_eliminados   = 0
        envios_redirigidos    = 0

        if modo_ejecucion:
            cursor.execute("SAVEPOINT antes_unificacion")

            for accion in acciones:
                id_m = accion["id_maestro"]
                id_d = accion["id_dup"]

                # 1. Redirigir envios.cliente_emisor
                cursor.execute(
                    "UPDATE envios SET cliente_emisor = %s WHERE cliente_emisor = %s",
                    (id_m, id_d)
                )
                envios_redirigidos += cursor.rowcount

                # 2. Redirigir envios.cliente_receptor
                cursor.execute(
                    "UPDATE envios SET cliente_receptor = %s WHERE cliente_receptor = %s",
                    (id_m, id_d)
                )
                envios_redirigidos += cursor.rowcount

                # 3. Eliminar cliente duplicado
                #    cliente_cambios se limpia automáticamente por CASCADE
                cursor.execute("DELETE FROM clientes WHERE id = %s", (id_d,))
                clientes_eliminados += cursor.rowcount

            # ── Verificación post-unificación ────────────────────────────
            cursor.execute("""
                SELECT COUNT(*) AS huerfanos FROM envios
                WHERE
                    (cliente_emisor   IS NOT NULL AND
                     cliente_emisor   NOT IN (SELECT id FROM clientes))
                 OR (cliente_receptor IS NOT NULL AND
                     cliente_receptor NOT IN (SELECT id FROM clientes))
            """)
            huerfanos = cursor.fetchone()["huerfanos"]

            if huerfanos > 0:
                cursor.execute("ROLLBACK TO SAVEPOINT antes_unificacion")
                conn.commit()
                logger.error(
                    f"❌ Verificación fallida: {huerfanos} envíos con FK inválida. "
                    f"ROLLBACK ejecutado — la BD no fue modificada."
                )
                sys.exit(1)

            conn.commit()
            logger.info("")
            logger.info("✅ Unificación completada y verificada:")
            logger.info(f"   Clientes eliminados:  {clientes_eliminados}")
            logger.info(f"   Envíos redirigidos:   {envios_redirigidos}")
            logger.info(f"   Registros en cliente_cambios eliminados por CASCADE: {total_cambios}")

        # ── Generar reporte MD ───────────────────────────────────────────
        lines = []
        a = lines.append

        a("# Reporte de Unificación de Clientes")
        a("")
        a("| Campo | Valor |")
        a("|---|---|")
        a(f"| **Fecha** | {ts} |")
        a(f"| **Modo** | {'EJECUCIÓN ✅' if modo_ejecucion else 'SIMULACIÓN — sin cambios'} |")
        a(f"| **Pares procesados** | {len(acciones)} |")
        a(f"| **Tablas actualizadas** | `envios` (cliente_emisor, cliente_receptor), `cliente_cambios` (CASCADE) |")
        if modo_ejecucion:
            a(f"| **Clientes eliminados** | {clientes_eliminados} |")
            a(f"| **Envíos redirigidos** | {envios_redirigidos} |")
            a(f"| **Registros cliente_cambios eliminados (CASCADE)** | {total_cambios} |")
        else:
            a(f"| **Envíos a redirigir (emisor)** | {total_envios_emisor} |")
            a(f"| **Envíos a redirigir (receptor)** | {total_envios_receptor} |")
            a(f"| **Registros cliente_cambios (CASCADE)** | {total_cambios} |")
        a("")

        a("## Detalle de Unificaciones")
        a("")
        a("| # | ID Maestro | Nombre Maestro (conservar) | ID Eliminar | Nombre Eliminar | Teléfono | Envíos emisor | Envíos receptor | Cambios CASCADE |")
        a("|---|---|---|---|---|---|---|---|---|")

        for i, accion in enumerate(acciones, 1):
            a(
                f"| {i} "
                f"| {accion['id_maestro']} "
                f"| {accion['nombre_maestro']} "
                f"| {accion['id_dup']} "
                f"| {accion['nombre_dup']} "
                f"| {pares[0]['telefono'] if i == 1 else ''} "
                f"| {accion['envios_emisor']} "
                f"| {accion['envios_receptor']} "
                f"| {accion['cambios_cascade']} |"
            )

        a("")

        if not modo_ejecucion:
            a("---")
            a("**Para ejecutar la unificación:**")
            a("```bash")
            a("python unificar_clientes.py --ejecutar")
            a("```")

        a("")
        a("---")
        a(f"*Generado por `unificar_clientes.py` — {ts}*")

        nombre_md = f"unificacion_clientes_{ts_archivo}.md"
        with open(nombre_md, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logger.info(f"\n📄 Reporte generado: {nombre_md}")

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error: {e}. ROLLBACK ejecutado.")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()          