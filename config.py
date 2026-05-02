# =============================================================================
# config.py — Configuración maestra para ambos scripts de migración
# IDs basados en BD limpia actual (59 sucursales, MAX id=82)
# Las sucursales CAJAS - NO existen en esta BD — sus etiquetas
# (CM, CPR, CQ, CSI, CW) se mapean a las sucursales USA equivalentes
# =============================================================================

# -----------------------------------------------------------------------------
# 1. CONEXIÓN A BASE DE DATOS
# -----------------------------------------------------------------------------
DB_CONFIG = {
    "host":     "192.168.0.115",
    #"host":     "100.96.225.107",
    "port":     5432,
    "user":     "tulcingo_user",
    "password": "123456",
    "database": "paimon",
}

USER_ID_IMPORTACION = 1   # ← ID del usuario Laravel importador

# -----------------------------------------------------------------------------
# 2. CATÁLOGO DE TIPO DE PRODUCTO
# -----------------------------------------------------------------------------
TIPO_PRODUCTO_CAJA    = 12
TIPO_PRODUCTO_DEFAULT = 13

# -----------------------------------------------------------------------------
# 3. DICCIONARIO MAESTRO: Nombre oficial → sucursal_id
#    Fuente: SELECT id, nombre FROM sucursales ORDER BY id (59 sucursales)
# -----------------------------------------------------------------------------
NOMBRE_A_SUCURSAL_ID = {
    # ── USA ──────────────────────────────────────────────────────────────────
    "Oxnard":               14,
    "QUEENS":               19,
    "MANHATTAN":            20,
    "WESTBURY":             22,
    "PORT RICHMOND":        23,
    "STATEN ISLAND":        24,
    "X CORREO":             28,
    "Corona Manhattan":     58,
    "BRONX":                60,
    # CAJAS - no existen → mapear a sucursal USA equivalente
    "CAJAS - BROOKLYN":     14,   # → Oxnard (Brooklyn)
    "CAJAS - MANHATTAN":    20,   # → MANHATTAN
    "CAJAS - PORT RICHMOND":23,   # → PORT RICHMOND
    "CAJAS - QUEENS":       19,   # → QUEENS
    "CAJAS - STATEN ISLAND":24,   # → STATEN ISLAND
    "CAJAS - WESTBURY":     22,   # → WESTBURY
    "CONNECTICUT":          48,   # No existe → DESCONOCIDO

    # ── México ────────────────────────────────────────────────────────────────
    "Tlaxcala":             15,
    "TULCINGO":             18,
    "HUAMUXTITLAN":         25,
    "CHILA DE LA SAL":      26,
    "ZAPOTITLAN":           27,
    "ACAXTLAHUACAN":        29,
    "AXOCHIAPAN":           30,
    "CUALAC":               31,
    "CHIAUTLA":             32,
    "CUALAC MIREYA":        33,
    "CHILAPA":              34,
    "ACATLAN":              35,
    "DISTRITO FEDERAL":     36,
    "PROGRESO":             37,
    "PUEBLA MAYORAZGO":     38,
    "ATENCINGO":            39,
    "CUAUTLA AÑO DE JUAREZ":40,
    "HUEHUETLAN":           41,
    "IXCAMILPA":            42,
    "ALPOYECA":             43,
    "CUAUTLA CENTRO":       44,
    "TOLTECAMILA":          45,
    "MATAMOROS":            46,
    "IZUCAR PANCHO":        47,
    "DESCONOCIDO":          48,
    "Acatlán":              52,
    "Atlixco-Matamoros":    55,
    "Año Nuevo":            56,
    "CHILPANCINGO GRO.":    61,
    "HUAJUAPAN":            62,
    "IXCATEOPAN":           63,
    "JOJUTLA":              64,
    "MOMOXPAN":             65,
    "OLINALA-ELENA":        66,
    "OLINALA-VICTOR":       67,
    "POCHUTLA":             68,
    "PUEBLA - VW":          69,
    "SAN PEDRO OCOTLAN":    70,
    "SANTA ANA RAYON":      71,
    "TEHUITZINGO":          72,
    "TEPEACA":              73,
    "TEPETLAPA GUERRERO":   74,
    "TLALT-MARILU":         75,
    "TLALTEPEXI":           76,
    "TLAPA":                77,
    "TLAPA JAVIER":         78,
    "XICOTLAN":             79,
    "XIHUITLIPA":           80,
    "XOCHI":                81,
    "YUPILTEPEC":           82,
}

# -----------------------------------------------------------------------------
# 4. ALIASES: variante del XML → nombre oficial en NOMBRE_A_SUCURSAL_ID
# -----------------------------------------------------------------------------
ALIASES_NOMBRE = {
    # ── TULCINGO ──────────────────────────────────────────────────────────────
    "TULCINGO":                     "TULCINGO",
    "TULCINGO DEL VALLE":           "TULCINGO",

    # ── ZAPOTITLAN ────────────────────────────────────────────────────────────
    "ZAPOTITLAN":                   "ZAPOTITLAN",
    "ZAPOTITLAN SALINAS":           "ZAPOTITLAN",

    # ── CUAUTLA AÑO DE JUAREZ ────────────────────────────────────────────────
    "CUAUTLA AÑO DE JUAREZ":        "CUAUTLA AÑO DE JUAREZ",
    "AÑO DE JUAREZ":                "CUAUTLA AÑO DE JUAREZ",
    "CUAUTLA DE JUAREZ":            "CUAUTLA AÑO DE JUAREZ",
    "CUAUTLA JUAREZ":               "CUAUTLA AÑO DE JUAREZ",
    "AÑO NUEVO DE JUAREZ":          "CUAUTLA AÑO DE JUAREZ",
    "CUAUTLA AÑO JUAREZ":           "CUAUTLA AÑO DE JUAREZ",

    # ── CUALAC MIREYA ─────────────────────────────────────────────────────────
    "CUALAC MIREYA":                "CUALAC MIREYA",
    "CUALAC-MIREYA":                "CUALAC MIREYA",
    "CUALAC - MIREYA":              "CUALAC MIREYA",

    # ── CUALAC ────────────────────────────────────────────────────────────────
    "CUALAC":                       "CUALAC",

    # ── MATAMOROS ────────────────────────────────────────────────────────────
    "MATAMOROS":                    "MATAMOROS",
    "I DE MATAMOROS":               "MATAMOROS",
    "IZUCAR DE MATAMOROS":          "MATAMOROS",
    "IZUCAR MATAMOROS":             "MATAMOROS",

    # ── IXCAMILPA ────────────────────────────────────────────────────────────
    "IXCAMILPA":                    "IXCAMILPA",
    "IXCAMILPA GRO.":               "IXCAMILPA",
    "IXCAMILPA GRO":                "IXCAMILPA",

    # ── PUEBLA MAYORAZGO ─────────────────────────────────────────────────────
    "PUEBLA MAYORAZGO":             "PUEBLA MAYORAZGO",
    "PUE MAYORAZGO":                "PUEBLA MAYORAZGO",
    "PUEBLA - VW":                  "PUEBLA - VW",
    "ATLX PMAYORAZGO":              "Atlixco-Matamoros",
    "CHL PMAYORAZGO":               "CHILAPA",

    # ── CHIAUTLA ──────────────────────────────────────────────────────────────
    "CHIAUTLA":                     "CHIAUTLA",
    "CHIAUTLA DE TAPIA":            "CHIAUTLA",
    "CHIAUTLA TAPIA":               "CHIAUTLA",

    # ── ACAXTLAHUACAN ────────────────────────────────────────────────────────
    "ACAXTLAHUACAN":                "ACAXTLAHUACAN",

    # ── ACATLAN ──────────────────────────────────────────────────────────────
    "ACATLAN":                      "ACATLAN",
    "ACATLÁN":                      "ACATLAN",
    "ACATLAN DE OSORIO":            "ACATLAN",

    # ── CHILAPA ──────────────────────────────────────────────────────────────
    "CHILAPA":                      "CHILAPA",
    "CHILAPA DE ALVAREZ":           "CHILAPA",

    # ── HUAMUXTITLAN ─────────────────────────────────────────────────────────
    "HUAMUXTITLAN":                 "HUAMUXTITLAN",
    "HUAMUX-LT":                    "HUAMUXTITLAN",
    "HUAMUX LT":                    "HUAMUXTITLAN",
    "HUAMUX":                       "HUAMUXTITLAN",

    # ── TOLTECAMILA ──────────────────────────────────────────────────────────
    "TOLTECAMILA":                  "TOLTECAMILA",

    # ── DISTRITO FEDERAL ─────────────────────────────────────────────────────
    "DISTRITO FEDERAL":             "DISTRITO FEDERAL",
    "DF":                           "DISTRITO FEDERAL",
    "CDMX":                         "DISTRITO FEDERAL",

    # ── HUEHUETLAN ───────────────────────────────────────────────────────────
    "HUEHUETLAN":                   "HUEHUETLAN",
    "HUEHUETLAN EL CHICO":          "HUEHUETLAN",

    # ── ALPOYECA ─────────────────────────────────────────────────────────────
    "ALPOYECA":                     "ALPOYECA",

    # ── AXOCHIAPAN ───────────────────────────────────────────────────────────
    "AXOCHIAPAN":                   "AXOCHIAPAN",

    # ── CUAUTLA CENTRO ───────────────────────────────────────────────────────
    "CUAUTLA CENTRO":               "CUAUTLA CENTRO",
    "CUAUTLA":                      "CUAUTLA CENTRO",

    # ── CHILA DE LA SAL ──────────────────────────────────────────────────────
    "CHILA DE LA SAL":              "CHILA DE LA SAL",
    "CHILA":                        "CHILA DE LA SAL",

    # ── Nuevas MEX ────────────────────────────────────────────────────────────
    "CHILPANCINGO GRO.":            "CHILPANCINGO GRO.",
    "CHILPANCINGO GRO":             "CHILPANCINGO GRO.",
    "CHILPANCINGO":                 "CHILPANCINGO GRO.",
    "HUAJUAPAN":                    "HUAJUAPAN",
    "HUAJUAPAN DE LEON":            "HUAJUAPAN",
    "IXCATEOPAN":                   "IXCATEOPAN",
    "JOJUTLA":                      "JOJUTLA",
    "MOMOXPAN":                     "MOMOXPAN",
    "OLINALA-ELENA":                "OLINALA-ELENA",
    "OLINALA ELENA":                "OLINALA-ELENA",
    "OLINALA-VICTOR":               "OLINALA-VICTOR",
    "OLINALA VICTOR":               "OLINALA-VICTOR",
    "POCHUTLA":                     "POCHUTLA",
    "SAN PEDRO OCOTLAN":            "SAN PEDRO OCOTLAN",
    "SANTA ANA RAYON":              "SANTA ANA RAYON",
    "TEHUITZINGO":                  "TEHUITZINGO",
    "TEPEACA":                      "TEPEACA",
    "TEPETLAPA GUERRERO":           "TEPETLAPA GUERRERO",
    "TEPETLAPA":                    "TEPETLAPA GUERRERO",
    "TLALT-MARILU":                 "TLALT-MARILU",
    "TLALTEPEXI":                   "TLALTEPEXI",
    "TLAPA":                        "TLAPA",
    "TLAPA JAVIER":                 "TLAPA JAVIER",
    "XICOTLAN":                     "XICOTLAN",
    "XIHUITLIPA":                   "XIHUITLIPA",
    "XOCHI":                        "XOCHI",
    "YUPILTEPEC":                   "YUPILTEPEC",

    # ── USA ───────────────────────────────────────────────────────────────────
    "MANHATTAN":                    "MANHATTAN",
    "CORONA MANHATTAN":             "Corona Manhattan",
    "QUEENS":                       "QUEENS",
    "BROOKLYN":                     "Oxnard",
    "BRONX":                        "BRONX",
    "PORT RICHMOND":                "PORT RICHMOND",
    "STATEN ISLAND":                "STATEN ISLAND",
    "WESTBURY":                     "WESTBURY",
    "X CORREO":                     "X CORREO",
    "XCORREO":                      "X CORREO",
    "OXNARD":                       "Oxnard",
    "CONNECTICUT":                  "CONNECTICUT",
    # CAJAS - mapear a sucursal USA equivalente
    "CAJAS - BROOKLYN":             "CAJAS - BROOKLYN",
    "CAJAS - MANHATTAN":            "CAJAS - MANHATTAN",
    "CAJAS - PORT RICHMOND":        "CAJAS - PORT RICHMOND",
    "CAJAS - QUEENS":               "CAJAS - QUEENS",
    "CAJAS - STATEN ISLAND":        "CAJAS - STATEN ISLAND",
    "CAJAS - WESTBURY":             "CAJAS - WESTBURY",
    "ANIO":                         "Año Nuevo",

    # ── Etiquetas cortas de BD ────────────────────────────────────────────────
    "CB":       "CAJAS - BROOKLYN",
    "CM":       "CAJAS - MANHATTAN",
    "CPR":      "CAJAS - PORT RICHMOND",
    "CQ":       "CAJAS - QUEENS",
    "CSI":      "CAJAS - STATEN ISLAND",
    "CW":       "CAJAS - WESTBURY",
    "CNT":      "CONNECTICUT",
    "BRON":     "BRONX",
    "ACA":      "ACATLAN",
    "ACAX":     "ACAXTLAHUACAN",
    "ALPO":     "ALPOYECA",
    "ATEN":     "ATENCINGO",
    "AXO":      "AXOCHIAPAN",
    "CHI":      "CHIAUTLA",
    "CUA":      "CUALAC",
    "DIS":      "DISTRITO FEDERAL",
    "HUA":      "HUAMUXTITLAN",
    "HUE":      "HUEHUETLAN",
    "IZU":      "IZUCAR PANCHO",
    "MAT":      "MATAMOROS",
    "PRO":      "PROGRESO",
    "PUE":      "PUEBLA MAYORAZGO",
    "TOL":      "TOLTECAMILA",
    "TUL":      "TULCINGO",
    "ZAP":      "ZAPOTITLAN",
    "TL":       "Tlaxcala",
    "DES":      "DESCONOCIDO",
    "ANiO":     "Año Nuevo",
    "ATX-MAY":  "Atlixco-Matamoros",
    "V":        "CHILPANCINGO GRO.",
    "U":        "HUAJUAPAN",
    "JCH-":     "JOJUTLA",
    "MOMOXPAN": "MOMOXPAN",
    "O":        "OLINALA-ELENA",
    "O-":       "OLINALA-VICTOR",
    "POC":      "POCHUTLA",
    "P":        "PUEBLA - VW",
    "OT":       "SAN PEDRO OCOTLAN",
    "R":        "SANTA ANA RAYON",
    "TP":       "TEPEACA",
    "S":        "TEPETLAPA GUERRERO",
    "T":        "TLAPA",
    "TJ-":      "TLAPA JAVIER",
    "XICO-":    "XICOTLAN",
    "J":        "XIHUITLIPA",
    "X":        "XOCHI",
    "Y":        "YUPILTEPEC",
    "E":        "PROGRESO",
    "XMAIL":    "X CORREO",
    "W":        "TEHUITZINGO",
    "Z":        "TLALT-MARILU",
    "Q":        "TLALTEPEXI",
    "M":        "MATAMOROS",
    "B":        "AXOCHIAPAN",
    "PR":       "PORT RICHMOND",
}

# -----------------------------------------------------------------------------
# 5. RESOLUCIÓN DE ETIQUETAS DUPLICADAS POR CONTEXTO
# -----------------------------------------------------------------------------

# Script A — MEX→USA
ETIQUETAS_AMBIGUAS_MEX = {
    "M":    "MATAMOROS",
    "Z":    "TLALT-MARILU",
    "W":    "TEHUITZINGO",
    "Q":    "TLALTEPEXI",
    "CM":   "CAJAS - MANHATTAN",   # no existe → MANHATTAN
    "O":    "OLINALA-ELENA",
    "O-":   "OLINALA-VICTOR",
    "B":    "AXOCHIAPAN",
    "J":    "ALPOYECA",
}

# Script B — USA→MEX
ETIQUETAS_AMBIGUAS_USA = {
    "M":    "MANHATTAN",
    "Z":    "STATEN ISLAND",
    "W":    "WESTBURY",
    "Q":    "QUEENS",
    "CM":   "CAJAS - MANHATTAN",   # → MANHATTAN
    "O":    "OLINALA-ELENA",
    "O-":   "OLINALA-VICTOR",
    "B":    "Oxnard",
    "J":    "ALPOYECA",
    "CPR":  "PORT RICHMOND",
    "CQ":   "QUEENS",
    "CSI":  "STATEN ISLAND",
    "CW":   "WESTBURY",
    "CB":   "Oxnard",
}

# -----------------------------------------------------------------------------
# 6. ETIQUETAS ÚNICAS
# -----------------------------------------------------------------------------
ETIQUETAS_UNICAS = {
    "TUL":      "TULCINGO",
    "ZAP":      "ZAPOTITLAN",
    "ACAX":     "ACAXTLAHUACAN",
    "ACA":      "ACATLAN",
    "ALPO":     "ALPOYECA",
    "ATEN":     "ATENCINGO",
    "AXO":      "AXOCHIAPAN",
    "BRON":     "BRONX",
    "CUA":      "CUALAC",
    "DIS":      "DISTRITO FEDERAL",
    "HUA":      "HUAMUXTITLAN",
    "HUE":      "HUEHUETLAN",
    "IXC":      "IXCAMILPA",
    "IZU":      "IZUCAR PANCHO",
    "MAT":      "MATAMOROS",
    "PR":       "PORT RICHMOND",
    "PRO":      "PROGRESO",
    "PUE":      "PUEBLA MAYORAZGO",
    "TOL":      "TOLTECAMILA",
    "XCORREO":  "X CORREO",
    "TL":       "Tlaxcala",
    "DES":      "DESCONOCIDO",
    "ANiO":     "Año Nuevo",
    "ATX-MAY":  "Atlixco-Matamoros",
    "T":        "TLAPA",
    "TJ-":      "TLAPA JAVIER",
    "L":        "TOLTECAMILA",
    "A":        "ACAXTLAHUACAN",
    "U":        "HUAJUAPAN",
    "D":        "ACATLAN",
    "G":        "ATENCINGO",
    "H":        "CUAUTLA AÑO DE JUAREZ",
    "BX":       "BRONX",
    "CH":       "CHIAUTLA",
    "CP":       "CHILAPA",
    "V":        "CHILPANCINGO GRO.",
    "CHM":      "CHILAPA",
    "CNT":      "DESCONOCIDO",     # CONNECTICUT no existe → DESCONOCIDO
    "C":        "CUALAC",
    "K":        "CUAUTLA CENTRO",
    "DF":       "DISTRITO FEDERAL",
    "N":        "HUAMUXTITLAN",
    "HUEH":     "HUEHUETLAN",
    "IX":       "IXCAMILPA",
    "IXC":      "IXCATEOPAN",
    "MP":       "IZUCAR PANCHO",
    "JCH-":     "JOJUTLA",
    "MOMOXPAN": "MOMOXPAN",
    "POC":      "POCHUTLA",
    "F":        "PUEBLA MAYORAZGO",
    "P":        "PUEBLA - VW",
    "OT":       "SAN PEDRO OCOTLAN",
    "R":        "SANTA ANA RAYON",
    "TP":       "TEPEACA",
    "S":        "TEPETLAPA GUERRERO",
    "XICO-":    "XICOTLAN",
    "X":        "XOCHI",
    "Y":        "YUPILTEPEC",
    "E":        "PROGRESO",
    "XMAIL":    "X CORREO",
    # Etiquetas CAJAS - → sucursal USA equivalente
    "CB":       "CAJAS - BROOKLYN",
    "CM":       "CAJAS - MANHATTAN",
    "CPR":      "CAJAS - PORT RICHMOND",
    "CQ":       "CAJAS - QUEENS",
    "CSI":      "CAJAS - STATEN ISLAND",
    "CW":       "CAJAS - WESTBURY",
}

# -----------------------------------------------------------------------------
# 7. REGLA DE LOS 200 — fallback a DESCONOCIDO
# -----------------------------------------------------------------------------
SUCURSALES_SIN_ETIQUETA = ["DESCONOCIDO"]
BLOQUE_REGLA_200 = 999999

# -----------------------------------------------------------------------------
# 8. VALORES DEFAULT
# -----------------------------------------------------------------------------
DEFAULT_PESO          = 0.00
DEFAULT_PRECIO_TOTAL  = 0.00
DEFAULT_PAGO_EFECTIVO = 0.00
DEFAULT_PAGO_TARJETA  = 0.00
DEFAULT_V_ESTIMADO    = 0.00
DEFAULT_IMPUESTO      = 0.00
DEFAULT_PRECIO        = 0.00
DEFAULT_CANTIDAD      = 1