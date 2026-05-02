# 📦 Tulcingo — Scripts de Migración de Datos

Scripts de Python para la migración masiva de datos históricos desde archivos Excel (SpreadsheetML `.xml`) hacia la base de datos PostgreSQL del sistema **Tulcingo Back** (backend Laravel).

---

## 📁 Estructura del repositorio

```
migration/
├── config.py                      # Configuración central: BD, IDs de sucursales, aliases
├── xml_parser.py                  # Parser de archivos SpreadsheetML (detecta formato automáticamente)
├── db_helpers.py                  # Operaciones de BD: firstOrCreate, inserciones, resolución de sucursales
├── sync_sequences.py              # Sincronización de secuencias de folios post-migración
├── migracion_mex_usa.py           # Script A: migración México → USA
├── migracion_usa_mex.py           # Script B: migración USA → México
├── validar_migracion.py           # Validación post-migración (genera reporte .md)
├── limpiar_duplicados.py          # Detección y eliminación de registros duplicados
├── reporte_clientes_duplicados.py # Reporte de clientes candidatos a unificación
└── unificar_clientes.py           # Unificación de clientes duplicados
```

---

## ⚙️ Requisitos

- Python 3.10+
- PostgreSQL (base de datos del sistema Tulcingo Back)

```bash
pip install psycopg2-binary
```

---

## 🔧 Configuración inicial

Antes de ejecutar cualquier script, edita `config.py`:

```python
DB_CONFIG = {
    "host":     "192.168.0.1",   # IP del servidor PostgreSQL
    "port":     5432,
    "user":     "user",
    "password": "tu_password",
    "database": "nombre_bd",
}

USER_ID_IMPORTACION = 1   # ID del usuario Laravel que firma los registros importados
```

El archivo también contiene:
- `NOMBRE_A_SUCURSAL_ID` — mapeo de nombre de sucursal → ID en BD
- `ALIASES_NOMBRE` — variantes de nombres del XML → nombre oficial
- `ETIQUETAS_AMBIGUAS_MEX/USA` — resolución de etiquetas con conflicto por contexto

> ⚠️ Verifica que los IDs en `NOMBRE_A_SUCURSAL_ID` coincidan con los IDs reales de tu tabla `sucursales` antes de migrar.

---

## 📋 Formatos de Excel soportados

El parser detecta automáticamente el formato del archivo:

| Formato | Dirección | Columnas | Detectado por |
|---|---|---|---|
| **FORMATO_9** | MEX → USA | 9 columnas | Header contiene `NO. PAQ.` o `DESTINATARIO` |
| **FORMATO_11** | USA → MEX | 11 columnas | Header contiene `FECHA` o `# ENV` |

### FORMATO_9 (MEX → USA)
`No.Paq | Destinatario | Telefono | Cant | Descripcion | Destino | Remitente | Fecha | Origen`

### FORMATO_11 (USA → MEX)
`Fecha | #ENV | Remitente | Direccion | Telefono | Destinatario | Telefono | Paq | LB | Descripcion | DEST`

---

## 🚀 Flujo de migración

Ejecutar en este orden exacto:

### 1. Migrar archivos

```bash
# México → USA
python migracion_mex_usa.py 'BASE DE DATOS SEPT-DEC 2025.xml'
python migracion_mex_usa.py 'BASE DE DATO MEX-NY JAN-MAR 2025.xml'

# USA → México
python migracion_usa_mex.py 'base de datos - NYC-MEX.xml'
python migracion_usa_mex.py 'base de datos - NYC-MEX-2.xml'
```

Cada script genera un archivo `.log` con el detalle de registros procesados, errores y resumen final.

### 2. Validar migración

```bash
python validar_migracion.py 'BASE DE DATOS SEPT-DEC 2025.xml'
python validar_migracion.py 'BASE DE DATO MEX-NY JAN-MAR 2025.xml'
python validar_migracion.py 'base de datos - NYC-MEX.xml'
python validar_migracion.py 'base de datos - NYC-MEX-2.xml'
```

Genera un reporte `validacion_ARCHIVO_YYYYMMDD_HHMMSS.md` con:
- ✅ Conteo de registros (XML vs BD)
- ✅ Verificación de todos los campos por registro
- ✅ Detección de duplicados exactos

### 3. Limpiar duplicados

```bash
# Primero simular (no modifica BD)
python limpiar_duplicados.py

# Ejecutar cuando el conteo sea correcto
python limpiar_duplicados.py --ejecutar
```

### 4. Reportar clientes duplicados

```bash
python reporte_clientes_duplicados.py
```

Genera `reporte_duplicados_clientes_YYYYMMDD.md` con los pares de clientes candidatos a unificación.

### 5. Unificar clientes

```bash
# Primero simular
python unificar_clientes.py

# Ejecutar cuando el reporte se vea correcto
python unificar_clientes.py --ejecutar
```

### 6. Validar estado final

```bash
# Repetir validación sobre todos los archivos
python validar_migracion.py 'BASE DE DATOS SEPT-DEC 2025.xml'
# ... etc
```

---

## 🧩 Descripción de cada script

### `config.py`
Fuente de verdad de toda la configuración. Contiene la conexión a BD, el mapeo completo de sucursales (nombre → ID), aliases de variantes del XML y la resolución de etiquetas ambiguas por contexto MEX/USA.

### `xml_parser.py`
Parsea archivos SpreadsheetML exportados desde Excel. Maneja:
- Detección automática de formato (9 o 11 columnas)
- Columnas vacías con `ss:Index`
- Fechas en formato ISO, corto (`M/D/YYYY`) y número serial OLE
- Folios que Excel interpreta como fechas
- Filas vacías al final del archivo (early exit)
- Headers secundarios embebidos en los datos

### `db_helpers.py`
Contiene toda la lógica de acceso a BD:
- `first_or_create_cliente` — busca cliente por nombre+teléfono, lo crea si no existe
- `resolver_sucursal_id` — resuelve sucursal emisora por nombre, etiqueta o prefijo de folio
- `resolver_sucursal_destino` — resuelve sucursal receptora desde el campo destino del XML
- `obtener_origen_sucursal` — determina el origen ('mex'/'usa') de un cliente según su sucursal
- `insertar_envio` — INSERT en tabla `envios` con fecha del XML como `created_at`
- `insertar_paquete` — INSERT en tabla `paquetes` (1 registro por envío, descripción completa)

### `sync_sequences.py`
Actualiza los contadores de folios tras la migración para que el sistema Laravel no genere conflictos:
- `sucursales_folios.ultimo_folio` → MAX del número de folio por sucursal
- `envios_mex_sequence.last_value` → MAX de `folio_pais` en envíos MEX
- `envios_usa_sequence.last_value` → MAX de `folio_pais` en envíos USA

### `migracion_mex_usa.py` / `migracion_usa_mex.py`
Scripts ejecutables para cada dirección de envío. Comparten la misma arquitectura:
- Transacciones por fila con `SAVEPOINT` (un error no detiene el batch)
- `firstOrCreate` para clientes emisor y receptor
- El `origen` del cliente se deriva de su sucursal asignada (no del script que corre)
- Reporte final con conteo de éxitos, errores, clientes creados vs reutilizados

### `validar_migracion.py`
Compara cada registro del XML contra la BD y genera un reporte Markdown con:
- Validación 1: conteo (¿están todos?)
- Validación 2: campos (¿son correctos?)
- Validación 3: duplicados exactos con SQL de limpieza incluido

### `limpiar_duplicados.py`
Detecta y elimina duplicados exactos (mismos folio+semana+año+clientes+sucursales). Modo `--ejecutar` incluye verificación post-limpieza con rollback automático si algo falla.

### `reporte_clientes_duplicados.py`
Genera un reporte de clientes con mismo teléfono y nombre que es subconjunto del otro. Clasifica pares simples y clusters múltiples. No modifica la BD.

### `unificar_clientes.py`
Fusiona pares de clientes duplicados:
- Conserva el registro con nombre más largo (más completo)
- Redirige todas las FKs en `envios` (cliente_emisor, cliente_receptor)
- `cliente_cambios` se limpia automáticamente por CASCADE
- Corrige el `origen` del cliente maestro según su sucursal
- Modo `--ejecutar` con verificación de huérfanos y rollback automático

---

## 🔑 Reglas de negocio importantes

**Generación de folios:** Los folios se reinician cada semana. La unicidad lógica es `folio + semana + año`.

**Resolución de sucursales:** Para envíos USA→MEX, la sucursal emisora se determina por el **prefijo del folio** (ej: `B` → Brooklyn/Oxnard, `Q` → Queens, `M` → Manhattan), no por el campo `DEST` que contiene la sucursal receptora.

**Origen del cliente:** Se asigna según la sucursal que le corresponde — si la sucursal es USA, el cliente es `origen='usa'`, y viceversa — independientemente del script que procese el archivo.

**Remitente desconocido:** El sistema permite envíos con remitente no identificado. Se registran con nombre `DESCONOCIDO`.

**Sucursales faltantes:** Cualquier sucursal no encontrada en el catálogo se asigna a `DESCONOCIDO` (ID configurable en `config.py`).

**Clientes duplicados:** Dos clientes son el mismo si coinciden `nombre_completo` + `telefono_celular`. Clientes con mismo nombre pero diferente teléfono se tratan como personas distintas.

---

## ⚠️ Notas conocidas

- **CUAUCEN:** Sucursal que aparece en algunos archivos pero no existe en el sistema. Se asigna automáticamente a `DESCONOCIDO`.
- **Folios como fechas:** Excel puede interpretar folios alfanuméricos cortos como fechas. El parser los detecta y los marca como `FOLIO-FECHA-YYYY-MM-DD` en el log.
- **Registro `LISTA MENSUAL...`:** Un registro basura en el archivo NYC-MEX donde el folio es texto libre. No afecta la operación.

---

## 📝 Logs generados

| Archivo | Generado por |
|---|---|
| `migracion_mex_usa.log` | `migracion_mex_usa.py` |
| `migracion_usa_mex.log` | `migracion_usa_mex.py` |
| `validacion_ARCHIVO_FECHA.md` | `validar_migracion.py` |
| `reporte_duplicados_clientes_FECHA.md` | `reporte_clientes_duplicados.py` |
| `unificacion_clientes_FECHA.md` | `unificar_clientes.py` |