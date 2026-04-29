#!/usr/bin/env python3
# =============================================================================
# validar_migracion.py — Validación completa post-migración
#
# Compara TODOS los campos del XML contra la BD para ambos formatos.
# Genera reporte en Markdown con:
#   - Resumen ejecutivo
#   - Validación 1: Conteo (XML vs BD)
#   - Validación 2: Todos los campos por registro
#   - Validación 3: Duplicados en BD
#
# Uso:
#   python validar_migracion.py archivo.xml
#
# Salida:
#   validacion_ARCHIVO_YYYYMMDD_HHMMSS.md
# =============================================================================

import sys
import re
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '.')
from config import DB_CONFIG, NOMBRE_A_SUCURSAL_ID
from xml_parser import parsear_xml, FORMATO_9, FORMATO_11

# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("validar")

# Invertir para lookup ID → nombre
ID_A_SUCURSAL = {v: k for k, v in NOMBRE_A_SUCURSAL_ID.items()}


# =============================================================================
# CONEXIÓN
# =============================================================================

def conectar():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        user=DB_CONFIG["user"], password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


# =============================================================================
# HELPERS
# =============================================================================

def normalizar(valor):
    """Normaliza un valor para comparación: strip, upper, sin espacios dobles."""
    if valor is None:
        return ""
    return re.sub(r"\s+", " ", str(valor).strip().upper())


def formatear_fecha(dt):
    if dt is None:
        return "NULL"
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d")
    return str(dt)[:10]


def porcentaje(parte, total):
    if total == 0:
        return "0.0%"
    return f"{parte / total * 100:.1f}%"


# =============================================================================
# VALIDACIÓN 1 — CONTEO
# =============================================================================

def validar_conteo(cursor, registros_xml):
    logger.info("Ejecutando Validación 1: Conteo...")

    total_xml = len(registros_xml)

    # Clave única por registro: folio + semana + anio
    claves_xml = []
    for r in registros_xml:
        claves_xml.append((str(r["folio"]), r.get("semana"), r.get("anio"), r["numero_fila"]))

    # Todos los envíos de la BD agrupados
    cursor.execute("""
        SELECT folio, semana, anio, COUNT(*) AS cnt
        FROM envios
        GROUP BY folio, semana, anio
    """)
    bd_map = {
        (str(row["folio"]), row["semana"], row["anio"]): row["cnt"]
        for row in cursor.fetchall()
    }

    encontrados  = [(f, s, a, n) for f, s, a, n in claves_xml if (f, s, a) in bd_map]
    faltantes    = [(f, s, a, n) for f, s, a, n in claves_xml if (f, s, a) not in bd_map]

    return {
        "total_xml":    total_xml,
        "encontrados":  len(encontrados),
        "faltantes":    faltantes,
        "bd_map":       bd_map,
    }


# =============================================================================
# VALIDACIÓN 2 — TODOS LOS CAMPOS
# =============================================================================

def validar_campos(cursor, registros_xml, formato):
    logger.info("Ejecutando Validación 2: Campos...")

    resultados = []   # lista de dicts por registro
    id_a_nombre = ID_A_SUCURSAL

    for r in registros_xml:
        folio  = str(r["folio"])
        semana = r.get("semana")
        anio   = r.get("anio")

        cursor.execute("""
            SELECT
                e.id, e.folio, e.semana, e.anio,
                e.cantidad_paquetes, e.peso,
                e.sucursal_emisor, e.sucursal_receptor,
                e.domicilio_emisor, e.domicilio_receptor,
                e.created_at,
                ce.nombre_completo  AS nombre_emisor,
                ce.telefono_celular AS tel_emisor,
                cr.nombre_completo  AS nombre_receptor,
                cr.telefono_celular AS tel_receptor,
                p.descripcion       AS descripcion_paquete
            FROM envios e
            LEFT JOIN clientes ce ON ce.id = e.cliente_emisor
            LEFT JOIN clientes cr ON cr.id = e.cliente_receptor
            LEFT JOIN paquetes p  ON p.envio_id = e.id
            WHERE e.folio = %s AND e.semana = %s AND e.anio = %s
            LIMIT 1
        """, (folio, semana, anio))

        bd = cursor.fetchone()

        if bd is None:
            resultados.append({
                "fila":     r["numero_fila"],
                "folio":    folio,
                "semana":   semana,
                "anio":     anio,
                "estado":   "FALTANTE",
                "diffs":    [],
            })
            continue

        diffs = []

        def check(campo, val_xml, val_bd, modo="texto"):
            vx = normalizar(val_xml)
            vb = normalizar(val_bd)
            if not vx or not vb:
                return   # no comparar si alguno es vacío/NULL
            if modo == "numero":
                try:
                    if abs(float(val_xml) - float(val_bd)) > 0.01:
                        diffs.append((campo, str(val_xml), str(val_bd)))
                except (ValueError, TypeError):
                    pass
            elif modo == "fecha":
                vx2 = formatear_fecha(val_xml)
                vb2 = formatear_fecha(val_bd)
                if vx2 != vb2:
                    diffs.append((campo, vx2, vb2))
            else:
                if vx != vb:
                    diffs.append((campo, str(val_xml)[:80], str(val_bd)[:80]))

        # ── Campos comunes MEX→USA y USA→MEX ─────────────────────────────
        check("folio",             folio,                    bd["folio"])
        check("semana",            semana,                   bd["semana"],            "numero")
        check("anio",              anio,                     bd["anio"],              "numero")
        check("cantidad_paquetes", r.get("cantidad"),        bd["cantidad_paquetes"], "numero")
        check("created_at",        r.get("fecha"),           bd["created_at"],        "fecha")
        check("nombre_receptor",   r.get("destinatario"),    bd["nombre_receptor"])
        check("tel_receptor",      r.get("telefono_receptor"),bd["tel_receptor"])
        check("nombre_emisor",     r.get("remitente"),       bd["nombre_emisor"])
        check("descripcion",       r.get("descripcion"),     bd["descripcion_paquete"])

        # Sucursal emisor: comparar resolviendo el XML a ID y comparando con BD
        if r.get("origen_raw"):
            from db_helpers import _lookup_sucursal_id, resolver_sucursal_id
            from config import ALIASES_NOMBRE, NOMBRE_A_SUCURSAL_ID
            # Resolver el nombre/etiqueta del XML a ID
            origen_upper = str(r["origen_raw"]).strip().upper()
            nombre_resuelto = ALIASES_NOMBRE.get(origen_upper, origen_upper)
            id_xml_emisor = NOMBRE_A_SUCURSAL_ID.get(nombre_resuelto)
            id_bd_emisor  = bd["sucursal_emisor"]
            if id_xml_emisor and id_bd_emisor and id_xml_emisor != id_bd_emisor:
                diffs.append((
                    "sucursal_emisor",
                    f"{r['origen_raw']} (ID={id_xml_emisor})",
                    f"{id_a_nombre.get(id_bd_emisor, '?')} (ID={id_bd_emisor})"
                ))

        # Sucursal receptor: comparar resolviendo etiqueta/nombre del XML a ID
        if r.get("destino"):
            from config import ALIASES_NOMBRE, NOMBRE_A_SUCURSAL_ID
            dest_upper = str(r["destino"]).strip().upper()
            nombre_dest = ALIASES_NOMBRE.get(dest_upper, dest_upper)
            id_xml_receptor = NOMBRE_A_SUCURSAL_ID.get(nombre_dest)
            id_bd_receptor  = bd["sucursal_receptor"]
            if id_xml_receptor and id_bd_receptor and id_xml_receptor != id_bd_receptor:
                diffs.append((
                    "sucursal_receptor",
                    f"{r['destino']} (ID={id_xml_receptor})",
                    f"{id_a_nombre.get(id_bd_receptor, '?')} (ID={id_bd_receptor})"
                ))

        # ── Campos exclusivos USA→MEX ────────────────────────────────────
        if formato == FORMATO_11:
            check("peso",           r.get("peso"),            bd["peso"],             "numero")
            check("domicilio_emisor", r.get("domicilio_emisor"), bd["domicilio_emisor"])
            check("tel_emisor",     r.get("telefono_emisor"), bd["tel_emisor"])

        resultados.append({
            "fila":   r["numero_fila"],
            "folio":  folio,
            "semana": semana,
            "anio":   anio,
            "estado": "OK" if not diffs else "DISCREPANCIA",
            "diffs":  diffs,
        })

    ok           = sum(1 for r in resultados if r["estado"] == "OK")
    faltantes    = sum(1 for r in resultados if r["estado"] == "FALTANTE")
    discrepancias= sum(1 for r in resultados if r["estado"] == "DISCREPANCIA")

    logger.info(f"  OK: {ok} | Discrepancias: {discrepancias} | Faltantes: {faltantes}")
    return resultados, ok, discrepancias, faltantes


# =============================================================================
# VALIDACIÓN 3 — DUPLICADOS
# =============================================================================

def validar_duplicados(cursor):
    """
    Detecta duplicados EXACTOS: registros donde coinciden todos los campos
    relevantes (folio, semana, anio, cliente_emisor, cliente_receptor,
    cantidad_paquetes, sucursal_emisor, sucursal_receptor).

    Un folio repetido en la misma semana con distinto destinatario NO es
    duplicado — es un envío diferente legítimo.
    """
    logger.info("Ejecutando Validación 3: Duplicados exactos...")

    cursor.execute("""
        SELECT
            e.folio,
            e.semana,
            e.anio,
            e.cliente_emisor,
            e.cliente_receptor,
            e.cantidad_paquetes,
            e.sucursal_emisor,
            e.sucursal_receptor,
            COUNT(*) AS cnt,
            MIN(e.id) AS primer_id,
            MAX(e.id) AS ultimo_id,
            ce.nombre_completo AS nombre_emisor,
            cr.nombre_completo AS nombre_receptor
        FROM envios e
        LEFT JOIN clientes ce ON ce.id = e.cliente_emisor
        LEFT JOIN clientes cr ON cr.id = e.cliente_receptor
        GROUP BY
            e.folio, e.semana, e.anio,
            e.cliente_emisor, e.cliente_receptor,
            e.cantidad_paquetes,
            e.sucursal_emisor, e.sucursal_receptor,
            ce.nombre_completo, cr.nombre_completo
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, e.folio
    """)
    dups = cursor.fetchall()
    logger.info(f"  Duplicados exactos encontrados: {len(dups)}")
    return dups


# =============================================================================
# GENERADOR DE REPORTE MARKDOWN
# =============================================================================

def generar_md(ruta_xml, formato, conteo, campos_res, ok, disc, falt, dups, ts):

    total     = conteo["total_xml"]
    encontr   = conteo["encontrados"]
    faltantes = conteo["faltantes"]
    n_dups    = len(dups)

    # Calificación general
    tasa_exito = encontr / total * 100 if total > 0 else 0
    if tasa_exito >= 99:
        calificacion = "✅ EXCELENTE"
    elif tasa_exito >= 95:
        calificacion = "⚠️ BUENA"
    elif tasa_exito >= 85:
        calificacion = "⚠️ ACEPTABLE"
    else:
        calificacion = "❌ REQUIERE REVISIÓN"

    tipo_migracion = "USA → MEX" if formato == FORMATO_11 else "MEX → USA"

    lines = []
    a = lines.append

    a(f"# Reporte de Validación de Migración")
    a(f"")
    a(f"| Campo | Valor |")
    a(f"|---|---|")
    a(f"| **Archivo XML** | `{Path(ruta_xml).name}` |")
    a(f"| **Tipo de migración** | {tipo_migracion} |")
    a(f"| **Fecha de validación** | {ts} |")
    a(f"| **Calificación general** | {calificacion} |")
    a(f"")

    # ── Resumen ejecutivo ──────────────────────────────────────────────────
    a(f"## Resumen Ejecutivo")
    a(f"")
    a(f"| Métrica | Valor |")
    a(f"|---|---|")
    a(f"| Registros en XML | {total} |")
    a(f"| Encontrados en BD | {encontr} ({porcentaje(encontr, total)}) |")
    a(f"| Faltantes en BD | {len(faltantes)} ({porcentaje(len(faltantes), total)}) |")
    a(f"| Registros correctos (todos los campos) | {ok} ({porcentaje(ok, total)}) |")
    a(f"| Registros con discrepancias | {disc} ({porcentaje(disc, total)}) |")
    a(f"| Duplicados en BD | {n_dups} |")
    a(f"")

    # ── Validación 1: Conteo ───────────────────────────────────────────────
    a(f"## Validación 1 — Conteo de Registros")
    a(f"")
    if not faltantes:
        a(f"✅ Todos los registros del XML fueron encontrados en la base de datos.")
    else:
        a(f"❌ **{len(faltantes)} registros del XML no se encontraron en BD:**")
        a(f"")
        a(f"| Fila XML | Folio | Semana | Año |")
        a(f"|---|---|---|---|")
        for folio, semana, anio, fila in faltantes[:100]:
            a(f"| {fila} | `{folio}` | {semana} | {anio} |")
        if len(faltantes) > 100:
            a(f"")
            a(f"*... y {len(faltantes) - 100} más. Consulta el log para la lista completa.*")
    a(f"")

    # ── Validación 2: Campos ───────────────────────────────────────────────
    a(f"## Validación 2 — Verificación de Campos")
    a(f"")

    campos_con_diff = {}
    for res in campos_res:
        for campo, val_xml, val_bd in res["diffs"]:
            campos_con_diff[campo] = campos_con_diff.get(campo, 0) + 1

    if not campos_con_diff:
        a(f"✅ Todos los campos verificados coinciden con el XML.")
    else:
        a(f"### Resumen de discrepancias por campo")
        a(f"")
        a(f"| Campo | # Discrepancias |")
        a(f"|---|---|")
        for campo, cnt in sorted(campos_con_diff.items(), key=lambda x: -x[1]):
            a(f"| `{campo}` | {cnt} |")
        a(f"")

        a(f"### Detalle de discrepancias (primeras 200)")
        a(f"")
        a(f"| Fila XML | Folio | Semana | Año | Campo | Valor XML | Valor BD |")
        a(f"|---|---|---|---|---|---|---|")
        count = 0
        for res in campos_res:
            if res["estado"] == "DISCREPANCIA":
                for campo, val_xml, val_bd in res["diffs"]:
                    a(f"| {res['fila']} | `{res['folio']}` | {res['semana']} | {res['anio']} | `{campo}` | {val_xml} | {val_bd} |")
                    count += 1
                    if count >= 200:
                        break
            if count >= 200:
                break
        if disc > 200:
            a(f"")
            a(f"*... y más discrepancias. Ajusta el límite en el script si necesitas verlas todas.*")
    a(f"")

    # ── Validación 3: Duplicados ───────────────────────────────────────────
    a(f"## Validación 3 — Duplicados en Base de Datos")
    a(f"")
    if not dups:
        a(f"✅ No se encontraron registros duplicados exactos en la base de datos.")
        a(f"")
        a(f"> Un duplicado exacto es cuando coinciden: folio, semana, año, emisor, receptor, cantidad, sucursal emisora y sucursala receptora.")
    else:
        a(f"⚠️ **{n_dups} grupos de registros duplicados exactos encontrados:**")
        a(f"")
        a(f"> Un duplicado exacto coincide en todos los campos relevantes: folio, semana, año, emisor, receptor, cantidad, sucursales.")
        a(f"")
        a(f"| Folio | Semana | Año | Emisor | Receptor | Cant. | # Copias | IDs en BD |")
        a(f"|---|---|---|---|---|---|---|---|")
        for d in dups[:100]:
            ids = f"{d['primer_id']}…{d['ultimo_id']}"
            emisor  = (str(d['nombre_emisor'] or ''))[:25]
            receptor= (str(d['nombre_receptor'] or ''))[:25]
            a(f"| `{d['folio']}` | {d['semana']} | {d['anio']} | {emisor} | {receptor} | {d['cantidad_paquetes']} | {d['cnt']} | {ids} |")
        if n_dups > 100:
            a(f"")
            a(f"*... y {n_dups - 100} más.*")
        a(f"")
        a(f"**SQL para eliminar duplicados exactos (conserva el registro con menor ID):**")
        a(f"```sql")
        a(f"-- Paso 1: eliminar paquetes de los duplicados")
        a(f"DELETE FROM paquetes WHERE envio_id IN (")
        a(f"    SELECT id FROM envios e")
        a(f"    WHERE id NOT IN (")
        a(f"        SELECT MIN(id)")
        a(f"        FROM envios")
        a(f"        GROUP BY folio, semana, anio,")
        a(f"                 cliente_emisor, cliente_receptor,")
        a(f"                 cantidad_paquetes, sucursal_emisor, sucursal_receptor")
        a(f"    )")
        a(f");")
        a(f"-- Paso 2: eliminar los envíos duplicados")
        a(f"DELETE FROM envios WHERE id NOT IN (")
        a(f"    SELECT MIN(id)")
        a(f"    FROM envios")
        a(f"    GROUP BY folio, semana, anio,")
        a(f"             cliente_emisor, cliente_receptor,")
        a(f"             cantidad_paquetes, sucursal_emisor, sucursal_receptor")
        a(f");")
        a(f"```")
    a(f"")

    # ── Pie ───────────────────────────────────────────────────────────────
    a(f"---")
    a(f"*Generado por `validar_migracion.py` — {ts}*")

    return "\n".join(lines)


# =============================================================================
# MAIN
# =============================================================================

def validar(ruta_xml):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_archivo = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"VALIDACIÓN: {ruta_xml}")
    logger.info("=" * 60)

    # Parsear XML
    registros_xml = parsear_xml(ruta_xml)
    formato = registros_xml[0]["formato"] if registros_xml else FORMATO_9
    tipo = "USA → MEX" if formato == FORMATO_11 else "MEX → USA"
    logger.info(f"Formato detectado: {tipo} | Registros: {len(registros_xml)}")

    # Conectar
    try:
        conn = conectar()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        sys.exit(1)

    # Ejecutar validaciones
    conteo      = validar_conteo(cursor, registros_xml)
    campos_res, ok, disc, falt = validar_campos(cursor, registros_xml, formato)
    dups        = validar_duplicados(cursor)

    # Generar MD
    md = generar_md(
        ruta_xml, formato, conteo,
        campos_res, ok, disc, falt, dups, ts
    )

    # Guardar archivo
    nombre_base = Path(ruta_xml).stem.replace(" ", "_")[:30]
    nombre_md   = f"validacion_{nombre_base}_{ts_archivo}.md"
    with open(nombre_md, "w", encoding="utf-8") as f:
        f.write(md)

    logger.info(f"\n📄 Reporte generado: {nombre_md}")

    # Resumen en consola
    total = conteo["total_xml"]
    logger.info("\n" + "=" * 60)
    logger.info("RESUMEN FINAL")
    logger.info("=" * 60)
    logger.info(f"  Tipo:              {tipo}")
    logger.info(f"  Total XML:         {total}")
    logger.info(f"  Encontrados BD:    {conteo['encontrados']} ({porcentaje(conteo['encontrados'], total)})")
    logger.info(f"  Faltantes BD:      {len(conteo['faltantes'])}")
    logger.info(f"  Campos correctos:  {ok} ({porcentaje(ok, total)})")
    logger.info(f"  Discrepancias:     {disc}")
    logger.info(f"  Duplicados BD:     {len(dups)}")
    logger.info("=" * 60)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python validar_migracion.py archivo.xml")
        sys.exit(1)
    validar(sys.argv[1])