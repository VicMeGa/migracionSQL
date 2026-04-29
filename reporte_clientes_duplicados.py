#!/usr/bin/env python3
# =============================================================================
# reporte_clientes_duplicados.py — Reporte de clientes candidatos a unificación
#
# Detecta pares de clientes donde:
#   - Mismo telefono_celular
#   - Uno de los nombres es subconjunto del otro (ej: "MARIA GARCIA" vs
#     "MARIA GARCIA LOPEZ")
#
# NO modifica la base de datos. Solo genera un reporte .md para revisión.
#
# Uso:
#   python reporte_clientes_duplicados.py
# =============================================================================

import sys
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '.')
from config import DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("reporte_duplicados")


def conectar():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        user=DB_CONFIG["user"], password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


# =============================================================================
# QUERIES
# =============================================================================

SQL_PARES = """
    SELECT
        c1.id           AS id1,
        c1.nombre_completo AS nombre1,
        c1.telefono_celular AS tel1,
        c1.sucursal_id  AS suc1,
        c1.origen       AS origen1,
        c2.id           AS id2,
        c2.nombre_completo AS nombre2,
        c2.telefono_celular AS tel2,
        c2.sucursal_id  AS suc2,
        c2.origen       AS origen2
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
"""

SQL_ENVIOS_CLIENTE = """
    SELECT
        COUNT(*) FILTER (WHERE cliente_emisor = %s)  AS como_emisor,
        COUNT(*) FILTER (WHERE cliente_receptor = %s) AS como_receptor
    FROM envios
    WHERE cliente_emisor = %s OR cliente_receptor = %s
"""

SQL_SUCURSAL_NOMBRE = """
    SELECT nombre FROM sucursales WHERE id = %s
"""


# =============================================================================
# AGRUPACIÓN EN CLUSTERS
# =============================================================================

def agrupar_en_clusters(pares):
    """
    Agrupa pares en clusters usando Union-Find.
    Si A=B y B=C, los tres quedan en el mismo cluster.
    """
    parent = {}

    def find(x):
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for par in pares:
        union(par["id1"], par["id2"])

    # Agrupar IDs por raíz
    clusters = {}
    ids_en_pares = set()
    for par in pares:
        ids_en_pares.add(par["id1"])
        ids_en_pares.add(par["id2"])

    for id_ in ids_en_pares:
        raiz = find(id_)
        clusters.setdefault(raiz, set()).add(id_)

    return [sorted(ids) for ids in clusters.values()]


def determinar_maestro(cluster_ids, clientes_por_id):
    """
    Determina el registro maestro del cluster:
    - Nombre más largo (más completo)
    - En caso de empate, ID más bajo
    """
    candidatos = [clientes_por_id[id_] for id_ in cluster_ids if id_ in clientes_por_id]
    candidatos.sort(key=lambda c: (-len(c["nombre_completo"]), c["id"]))
    return candidatos[0]


def clasificar_caso(nombres):
    """
    Clasifica el tipo de caso para el reporte.
    """
    nombres_upper = [n.upper().strip() for n in nombres]

    # Verificar si alguno contiene / o # (anotaciones especiales)
    tiene_especial = any("/" in n or "#" in n for n in nombres_upper)
    if tiene_especial:
        return "⚠️ NOMBRE CON ANOTACIÓN ESPECIAL"

    # Verificar si hay variación de tildes (AMALiA vs AMALIA)
    nombres_sin_tildes = []
    for n in nombres_upper:
        n2 = n.replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U")
        n2 = n2.replace("á","A").replace("é","E").replace("í","I").replace("ó","O").replace("ú","U")
        n2 = n2.replace("i","I")  # caso AMALiA
        nombres_sin_tildes.append(n2)

    if len(set(nombres_sin_tildes)) < len(nombres_sin_tildes):
        return "⚠️ VARIACIÓN DE TILDES / CAPITALIZACIÓN"

    # Caso normal
    if len(nombres) == 2:
        return "✅ PAR SIMPLE (1 apellido vs 2 apellidos)"
    else:
        return f"🔗 CLUSTER DE {len(nombres)} REGISTROS"


# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("Conectando a BD...")
    conn = conectar()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Obtener todos los pares
    logger.info("Buscando pares candidatos...")
    cursor.execute(SQL_PARES)
    pares = cursor.fetchall()
    logger.info(f"  Pares encontrados: {len(pares)}")

    if not pares:
        logger.info("No hay clientes candidatos a unificación.")
        return

    # Obtener nombres de sucursales
    cursor.execute("SELECT id, nombre FROM sucursales")
    sucursales = {row["id"]: row["nombre"] for row in cursor.fetchall()}

    # Recolectar todos los clientes involucrados
    ids_involucrados = set()
    for par in pares:
        ids_involucrados.add(par["id1"])
        ids_involucrados.add(par["id2"])

    cursor.execute(
        "SELECT id, nombre_completo, telefono_celular, sucursal_id, origen "
        "FROM clientes WHERE id = ANY(%s)",
        (list(ids_involucrados),)
    )
    clientes_por_id = {row["id"]: dict(row) for row in cursor.fetchall()}

    # Contar envíos por cliente
    logger.info("Contando envíos por cliente...")
    for id_ in ids_involucrados:
        cursor.execute(SQL_ENVIOS_CLIENTE, (id_, id_, id_, id_))
        row = cursor.fetchone()
        clientes_por_id[id_]["envios_emisor"]   = row["como_emisor"]
        clientes_por_id[id_]["envios_receptor"]  = row["como_receptor"]
        clientes_por_id[id_]["envios_total"]     = row["como_emisor"] + row["como_receptor"]

    # Agrupar en clusters
    clusters = agrupar_en_clusters(pares)
    logger.info(f"  Clusters detectados: {len(clusters)}")

    # Clasificar clusters
    cluster_simple   = [c for c in clusters if len(c) == 2]
    cluster_multiple = [c for c in clusters if len(c) >  2]
    logger.info(f"  Pares simples (2):  {len(cluster_simple)}")
    logger.info(f"  Clusters múltiples: {len(cluster_multiple)}")

    # ==========================================================================
    # GENERAR REPORTE MD
    # ==========================================================================
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_archivo = datetime.now().strftime("%Y%m%d_%H%M%S")

    lines = []
    a = lines.append

    a("# Reporte de Clientes Candidatos a Unificación")
    a("")
    a("| Campo | Valor |")
    a("|---|---|")
    a(f"| **Fecha** | {ts} |")
    a(f"| **Total pares detectados** | {len(pares)} |")
    a(f"| **Clusters a unificar** | {len(clusters)} |")
    a(f"| **Clientes involucrados** | {len(ids_involucrados)} |")
    a(f"| **Pares simples (2 registros)** | {len(cluster_simple)} |")
    a(f"| **Clusters múltiples (3+)** | {len(cluster_multiple)} |")
    a("")
    a("> **Criterio:** Mismo `telefono_celular` y uno de los nombres es subconjunto del otro.")
    a("> El script NO modifica la BD — solo reporta.")
    a("")

    # ── Clusters múltiples primero (más atención requerida) ────────────────
    if cluster_multiple:
        a("## ⚠️ Clusters Múltiples (requieren revisión manual)")
        a("")
        a("Estos casos tienen 3 o más registros que se encadenan. Revisar con cuidado.")
        a("")

        for i, cluster_ids in enumerate(cluster_multiple, 1):
            maestro = determinar_maestro(cluster_ids, clientes_por_id)
            nombres = [clientes_por_id[id_]["nombre_completo"] for id_ in cluster_ids if id_ in clientes_por_id]
            tipo = clasificar_caso(nombres)

            a(f"### Cluster M{i} — {tipo}")
            a("")
            a(f"| ID | Nombre | Teléfono | Sucursal | Envíos | Rol maestro |")
            a(f"|---|---|---|---|---|---|")

            for id_ in cluster_ids:
                if id_ not in clientes_por_id:
                    continue
                c = clientes_por_id[id_]
                suc_nombre = sucursales.get(c["sucursal_id"], f"ID={c['sucursal_id']}")
                es_maestro = "⭐ MAESTRO" if id_ == maestro["id"] else "→ fusionar"
                a(f"| {id_} | {c['nombre_completo']} | {c['telefono_celular']} | {suc_nombre} | {c['envios_total']} | {es_maestro} |")

            a("")

    # ── Pares simples ──────────────────────────────────────────────────────
    a("## Pares Simples")
    a("")
    a("| # | ID Maestro | Nombre Maestro | ID Fusionar | Nombre Fusionar | Teléfono | Envíos Maestro | Envíos Fusionar | Tipo |")
    a("|---|---|---|---|---|---|---|---|---|")

    for i, cluster_ids in enumerate(cluster_simple, 1):
        if not all(id_ in clientes_por_id for id_ in cluster_ids):
            continue
        maestro = determinar_maestro(cluster_ids, clientes_por_id)
        otros   = [id_ for id_ in cluster_ids if id_ != maestro["id"]]
        nombres = [clientes_por_id[id_]["nombre_completo"] for id_ in cluster_ids]
        tipo    = clasificar_caso(nombres)

        for id_otro in otros:
            c_otro = clientes_por_id[id_otro]
            a(
                f"| {i} "
                f"| {maestro['id']} "
                f"| {maestro['nombre_completo']} "
                f"| {id_otro} "
                f"| {c_otro['nombre_completo']} "
                f"| {maestro['telefono_celular']} "
                f"| {maestro['envios_total']} "
                f"| {c_otro['envios_total']} "
                f"| {tipo} |"
            )

    a("")
    a("---")
    a(f"*Generado por `reporte_clientes_duplicados.py` — {ts}*")

    # Guardar
    nombre_md = f"reporte_duplicados_clientes_{ts_archivo}.md"
    with open(nombre_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"\n📄 Reporte generado: {nombre_md}")
    logger.info(f"   Clusters a unificar: {len(clusters)}")
    logger.info(f"   Clientes involucrados: {len(ids_involucrados)}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()