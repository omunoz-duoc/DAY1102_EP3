"""Dashboards de la defensa (Plotly Dash), una app con dos páginas:

  /comercial    lee Gold: cascada de conciliación, ventas por canal y modalidad, perfil Cliente 360
  /operaciones  lee Serving cada 3 s: checkout por minuto y dispositivo + panel de stock por ubicación

Las consultas usan conexiones de solo lectura (tnr.apps.consultas)."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

from tnr.apps import consultas as q
from tnr.config import Config

# Paleta de referencia (validada: 3 slots all-pairs, light). Color sigue a la entidad, no al rango.
SURFACE, PAGE = "#fcfcfb", "#f9f9f7"
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
SERIES = {"mobile": "#2a78d6", "desktop": "#eb6834", "tablet": "#1baf7a"}
BLUE, ORANGE = "#2a78d6", "#eb6834"
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
REFRESCO_MS = 3000

CSS = f"""
:root {{ color-scheme: light; }}
body {{ margin:0; background:{PAGE}; color:{INK};
       font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }}
.wrap {{ max-width: 1200px; margin: 0 auto; padding: 16px; }}
header {{ display:flex; align-items:baseline; gap:24px; flex-wrap:wrap; padding: 8px 0 16px; }}
header h1 {{ font-size: 20px; margin:0; }}
nav a {{ margin-right:16px; color:{INK_2}; text-decoration:none; font-weight:500; padding-bottom:2px; }}
nav a.activo {{ color:{INK}; border-bottom:2px solid {INK}; }}
.filtros {{ display:flex; gap:16px; flex-wrap:wrap; align-items:flex-end; margin-bottom:16px; }}
.filtros label {{ display:block; font-size:12px; color:{INK_2}; margin-bottom:4px; }}
.filtros input {{ font: inherit; padding:6px 8px; border:1px solid {AXIS}; border-radius:6px; width: 220px; }}
.kpis {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:12px; margin-bottom:16px; }}
.tile {{ background:{SURFACE}; border:1px solid {GRID}; border-radius:10px; padding:12px 14px; }}
.tile .k {{ font-size:12px; color:{INK_2}; }}
.tile .v {{ font-size:28px; font-weight:600; font-variant-numeric: tabular-nums; margin-top:4px; }}
.tile .s {{ font-size:12px; color:{MUTED}; margin-top:2px; }}
.card {{ background:{SURFACE}; border:1px solid {GRID}; border-radius:10px; padding:12px 14px; margin-bottom:16px; }}
.card h2 {{ font-size:15px; margin:0 0 4px; }}
.card .sub {{ font-size:12px; color:{INK_2}; margin-bottom:8px; }}
.grid2 {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap:16px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; font-variant-numeric: tabular-nums; }}
th {{ text-align:left; color:{INK_2}; font-weight:500; border-bottom:1px solid {AXIS}; padding:6px 8px; }}
td {{ border-bottom:1px solid {GRID}; padding:6px 8px; }}
td.n, th.n {{ text-align:right; }}
.tabla {{ overflow-x:auto; }}
.badge {{ display:inline-flex; align-items:center; gap:6px; font-size:12px; font-weight:500; }}
.dot {{ width:10px; height:10px; border-radius:50%; display:inline-block; }}
.estado {{ font-size:13px; color:{INK_2}; margin-bottom:12px; }}
dl {{ display:grid; grid-template-columns: max-content 1fr; gap:4px 16px; margin:0; font-size:13px; }}
dt {{ color:{INK_2}; }} dd {{ margin:0; font-variant-numeric: tabular-nums; }}
"""

# Estados con ícono + etiqueta: el color nunca va solo
ESTADOS = {
    "cerrada": ("✓", "Cerrada", STATUS["good"]),
    "provisional": ("◐", "Provisional", BLUE),
    "reproceso_pendiente": ("↻", "Reproceso pendiente", STATUS["warning"]),
    "ok": ("✓", "OK", STATUS["good"]),
    "reposicion": ("⚠", "Bajo umbral", STATUS["warning"]),
    "negativo": ("⛔", "Saldo negativo", STATUS["critical"]),
}


def badge(clave: str) -> html.Span:
    icono, etiqueta, color = ESTADOS[clave]
    return html.Span([html.Span(className="dot", style={"background": color}), f"{icono} {etiqueta}"],
                     className="badge")


def clp(v) -> str:
    if v is None:
        return "—"
    return ("−" if v < 0 else "") + "$" + f"{abs(int(v)):,}".replace(",", ".")


def mm(v) -> str:
    if v is None:
        return "—"
    signo = "−" if v < 0 else ""
    return f"{signo}${abs(v) / 1e6:,.0f} M".replace(",", ".")


ETAPAS = {"crudo_bronze": "Recibido<br>(Bronze)", "tras_deduplicacion": "Duplicados",
          "tras_validacion": "Rechazos<br>de calidad", "confirmadas_al_corte": "No<br>confirmadas",
          "notas_credito_aplicadas": "Notas de<br>crédito", "venta_neta": "Venta<br>neta"}
REF_ETAPAS = {"ficha_docente": "Ficha docente", "reproduccion_reglas_pipeline": "Reglas del pipeline sobre XLSX docente",
              "diferencia_vs_ficha": "Diferencia vs ficha"}


def num(v) -> str:
    if v is None:
        return "—"
    return ("−" if v < 0 else "") + f"{abs(int(v)):,}".replace(",", ".")


def tile(k: str, v: str, s: str = "") -> html.Div:
    return html.Div([html.Div(k, className="k"), html.Div(v, className="v"), html.Div(s, className="s")],
                    className="tile")


def base_layout(fig: go.Figure, *, height: int = 320, ytitle: str = "", yformat: str | None = None) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=8), paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family="Inter, system-ui, sans-serif", size=12, color=INK_2),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=AXIS, font=dict(color=INK)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None),
        barcornerradius=4, bargap=0.35,
    )
    fig.update_xaxes(showgrid=False, linecolor=AXIS, tickcolor=AXIS, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, gridwidth=1, zeroline=True, zerolinecolor=AXIS, title=ytitle,
                     tickformat=yformat)
    return fig


def _rango_local(cfg: Config, desde: str, hasta: str) -> list[str] | None:
    """Rango del eje temporal = intervalo elegido (medio minuto de margen para centrar las barras)."""
    try:
        zone = ZoneInfo(cfg.tz)
        lo, hi = (datetime.fromisoformat(x).replace(tzinfo=zone) if datetime.fromisoformat(x).tzinfo is None
                  else datetime.fromisoformat(x).astimezone(zone) for x in (desde, hasta))
    except (TypeError, ValueError):
        return None
    from datetime import timedelta
    return [(lo - timedelta(seconds=30)).isoformat(), (hi - timedelta(seconds=30)).isoformat()]


def tabla(columnas: list[tuple[str, str, bool]], filas: list[dict]) -> html.Div:
    """columnas: (clave, título, numérica)."""
    head = html.Thead(html.Tr([html.Th(t, className="n" if n else None) for _, t, n in columnas]))
    body = html.Tbody([html.Tr([html.Td(f.get(c), className="n" if n else None) for c, _, n in columnas])
                       for f in filas])
    return html.Div(html.Table([head, body]), className="tabla")


# ====================================================================== páginas
def pagina_comercial(cfg: Config) -> html.Div:
    cortes = q.cortes_publicados(cfg)
    if not cortes:
        return html.Div("No hay publicaciones comerciales. Ejecuta `python -m tnr publish comercial`.", className="card")
    return html.Div([
        html.Div([html.Div([html.Label("Fecha de corte"),
                            dcc.Dropdown(cortes, cortes[0], id="corte", clearable=False, style={"width": 220})])],
                 className="filtros"),
        html.Div(id="kpis-comercial", className="kpis"),
        html.Div([
            html.Div([html.H2("Conciliación de ventas"),
                      html.Div("Escenario ampliado: de las filas recibidas a la venta neta publicada", className="sub"),
                      dcc.Graph(id="cascada", config={"displayModeBar": False}),
                      html.Div(id="referencia")], className="card"),
            html.Div([html.H2("Venta neta por canal y modalidad"),
                      html.Div("Ventas confirmadas al corte, CLP", className="sub"),
                      dcc.Graph(id="canal-modalidad", config={"displayModeBar": False}),
                      html.Div(id="canal-tabla")], className="card"),
        ], className="grid2"),
        html.Div([html.H2("Perfil Cliente 360"),
                  html.Div("Solo se publica customer_id; la identidad queda en relaciones restringidas", className="sub"),
                  dcc.Dropdown(id="cliente", placeholder="Buscar customer_id…", style={"maxWidth": 320}),
                  html.Div(id="perfil", style={"marginTop": 12})], className="card"),
    ])


def pagina_operaciones(cfg: Config) -> html.Div:
    zone = ZoneInfo(cfg.tz)
    ini = datetime.fromisoformat(cfg.sim_inicio).astimezone(zone).strftime("%Y-%m-%dT%H:%M")
    fin = datetime.fromisoformat(cfg.sim_fin).astimezone(zone).strftime("%Y-%m-%dT%H:%M")
    return html.Div([
        dcc.Interval(id="tick", interval=REFRESCO_MS),
        html.Div([
            html.Div([html.Label(f"Desde (hora local {cfg.tz})"), dcc.Input(id="desde", value=ini, debounce=True, style={"width": 200})]),
            html.Div([html.Label("Hasta (excluido)"), dcc.Input(id="hasta", value=fin, debounce=True, style={"width": 200})]),
            html.Div([html.Label("Corte de stock"), dcc.Input(id="corte-stock", value="2025-12-26T12:05",
                                                              debounce=True, style={"width": 200})]),
        ], className="filtros"),
        html.Div(id="estado-web", className="estado"),
        html.Div(id="kpis-web", className="kpis"),
        html.Div([
            html.Div([html.H2("Intentos de checkout por minuto"),
                      html.Div("Por dispositivo, checkout_request con código HTTP válido", className="sub"),
                      dcc.Graph(id="intentos", config={"displayModeBar": False})], className="card"),
            html.Div([html.H2("Tasa de error 5xx por minuto"),
                      html.Div("Errores ÷ intentos, sumados entre dispositivos antes de dividir", className="sub"),
                      dcc.Graph(id="tasa", config={"displayModeBar": False})], className="card"),
        ], className="grid2"),
        html.Div([html.H2("Ventanas de checkout"),
                  html.Div("Gold si la ventana está cerrada, Silver si está abierta", className="sub"),
                  html.Div(id="tabla-web")], className="card"),
        html.Div([html.H2("Stock disponible por SKU y ubicación"), html.Div(id="estado-stock", className="sub"),
                  html.Div(id="tabla-stock")], className="card"),
    ])


# ====================================================================== app
def create_app(cfg: Config | None = None) -> Dash:
    cfg = cfg or Config.from_env()
    app = Dash("tnr-dashboards", title="TNR 360 · Dashboards", suppress_callback_exceptions=True)
    app.index_string = app.index_string.replace("{%css%}", "{%css%}<style>" + CSS + "</style>")
    app.layout = html.Div([
        dcc.Location(id="url"),
        html.Header([html.H1("TechNova Retail 360"), html.Nav(id="nav")]),
        html.Main(id="pagina"),
    ], className="wrap")

    @app.callback(Output("pagina", "children"), Output("nav", "children"), Input("url", "pathname"))
    def router(path):
        operaciones = (path or "").startswith("/operaciones")
        nav = [dcc.Link("Comercial", href="/comercial", className="" if operaciones else "activo"),
               dcc.Link("Operaciones", href="/operaciones", className="activo" if operaciones else "")]
        return (pagina_operaciones(cfg) if operaciones else pagina_comercial(cfg)), nav

    # ---------------------------------------------------------------- comercial
    @app.callback(Output("kpis-comercial", "children"), Output("cascada", "figure"), Output("referencia", "children"),
                  Output("canal-modalidad", "figure"), Output("canal-tabla", "children"),
                  Output("cliente", "options"), Input("corte", "value"))
    def comercial(corte):
        k = q.kpis_comerciales(cfg, corte)
        pub = k["publicacion"] or {}
        kpis = [
            tile("Venta neta", mm(k["neto"]), f"Bruto {mm(k['bruto'])}"),
            tile("Ventas confirmadas", num(k["ventas"]), f"Corte {corte}"),
            tile("Notas de crédito aplicadas", mm(k["notas"]), clp(k["notas"])),
            tile("Ventas sin cliente vinculado", num(k["sin_cliente"]),
                 f"{k['neto_sin_cliente'] / k['neto']:.1%} del neto, incluidas en el total" if k["neto"] else ""),
            tile("Publicación", pub.get("version_reglas", "—"), pub.get("publish_id", "")),
        ]
        conc = q.conciliacion(cfg, corte)
        amp = conc[conc["escenario"] == "ampliado"].sort_values("orden")
        montos = amp["monto_clp"].tolist()
        etapas = [ETAPAS.get(e, e) for e in amp["etapa"]]
        y = [montos[0]] + [montos[i] - montos[i - 1] for i in (1, 2, 3)] + [montos[4], montos[5]]
        medidas = ["absolute", "relative", "relative", "relative", "relative", "total"]
        fig = go.Figure(go.Waterfall(
            x=etapas, y=[v / 1e6 for v in y], measure=medidas, cliponaxis=False, connector=dict(line=dict(color=AXIS, width=1)),
            decreasing=dict(marker=dict(color=ORANGE)), increasing=dict(marker=dict(color=BLUE)),
            totals=dict(marker=dict(color=BLUE)),
            text=[mm(v) for v in y], textposition="outside", textfont=dict(color=INK_2),
            customdata=amp[["filas", "descripcion"]].values,
            hovertemplate="<b>%{x}</b><br>%{text}<br>Filas: %{customdata[0]:,}<br>%{customdata[1]}<extra></extra>"))
        base_layout(fig, height=340, yformat=",.0f")
        fig.update_yaxes(range=[0, max(montos) / 1e6 * 1.12], ticksuffix=" M", separatethousands=True)
        fig.update_xaxes(tickangle=0)
        fig.update_layout(separators=",.")
        ref = conc[conc["escenario"] == "referencia_docente"].sort_values("orden")
        referencia = html.Div([
            html.Div("Referencia docente (separada del escenario ampliado)", className="sub",
                     style={"marginTop": 8}),
            tabla([("etapa", "Etapa", False), ("filas", "Filas", True), ("monto", "Monto CLP", True)],
                  [{"etapa": REF_ETAPAS.get(e, e), "filas": num(f), "monto": clp(m)}
                   for e, f, m in ref[["etapa", "filas", "monto_clp"]].values]),
        ])
        cm = q.ventas_canal_modalidad(cfg, corte)
        etiquetas = (cm["canal"] + " · " + cm["modalidad_entrega"].str.replace("_", " ")).tolist()
        bar = go.Figure(go.Bar(
            y=etiquetas, x=cm["neto_clp"] / 1e6, orientation="h", cliponaxis=False, marker=dict(color=BLUE, line=dict(color=SURFACE, width=2)),
            text=[mm(v) for v in cm["neto_clp"]], textposition="outside", textfont=dict(color=INK_2),
            customdata=cm["ventas"], hovertemplate="<b>%{y}</b><br>%{text}<br>%{customdata:,} ventas<extra></extra>"))
        base_layout(bar, height=260, yformat=None)
        bar.update_xaxes(tickformat=",.0f", ticksuffix=" M", gridcolor=GRID, showgrid=True,
                         range=[0, cm["neto_clp"].max() / 1e6 * 1.3])
        bar.update_layout(separators=",.")
        bar.update_yaxes(autorange="reversed", showgrid=False)
        canal_tabla = tabla([("seg", "Canal · modalidad", False), ("ventas", "Ventas", True), ("neto", "Neto CLP", True)],
                            [{"seg": e, "ventas": num(v), "neto": clp(n)}
                             for e, v, n in zip(etiquetas, cm["ventas"], cm["neto_clp"])])
        return kpis, fig, referencia, bar, canal_tabla, q.clientes(cfg, corte)

    @app.callback(Output("perfil", "children"), Input("cliente", "value"), Input("corte", "value"))
    def perfil(customer_id, corte):
        if not customer_id:
            return html.Div("Selecciona un cliente.", className="sub")
        p = q.perfil_cliente(cfg, corte, customer_id)
        if p is None:
            return html.Div("Sin datos para ese cliente en el corte.", className="sub")
        campos = [
            ("customer_id", p["customer_id"]), ("Estado de identidad", p["estado_identidad"]),
            ("Comuna", p["comuna"]), ("Alta", f"{p['fecha_alta']} · {p['canal_alta']}"),
            ("Consentimiento publicidad", p["consentimiento_publicidad"]),
            ("Frecuencia (compras confirmadas)", num(p["frecuencia_compras"])),
            ("Recencia", "—" if p["recencia_dias"] is None else f"{int(p['recencia_dias'])} días "
                                                               f"(última {p['fecha_ultima_compra']})"),
            ("Monto neto", clp(p["monto_neto_clp"])), ("Canal preferido", p["canal_preferido"] or "—"),
            ("Órdenes postventa", f"{num(p['n_ordenes_postventa'])} ({num(p['n_ordenes_abiertas'])} abiertas, "
                                  f"{num(p['n_ordenes_garantia'])} en garantía)"),
            ("Días de resolución promedio", "—" if p["dias_resolucion_promedio"] is None
             else f"{p['dias_resolucion_promedio']:.1f}"),
        ]
        return html.Dl([x for k, v in campos for x in (html.Dt(k), html.Dd(v))])

    # ---------------------------------------------------------------- operaciones
    @app.callback(Output("estado-web", "children"), Output("kpis-web", "children"), Output("intentos", "figure"),
                  Output("tasa", "figure"), Output("tabla-web", "children"), Output("estado-stock", "children"),
                  Output("tabla-stock", "children"),
                  Input("tick", "n_intervals"), Input("desde", "value"), Input("hasta", "value"),
                  Input("corte-stock", "value"))
    def operaciones(_n, desde, hasta, corte_stock):
        c = q.checkout(cfg, desde, hasta)
        f = c["filas"]
        ahora = datetime.now(ZoneInfo(cfg.tz)).strftime("%H:%M:%S")
        estados = f.drop_duplicates("minuto_utc")["estado_ventana"].value_counts().to_dict() if len(f) else {}
        estado = [f"Consultado {ahora} · refresco cada {REFRESCO_MS // 1000} s · ventanas: "] + (
            [x for e, n in estados.items() for x in (badge(e), f" {n}   ")] or ["sin datos en el intervalo"])
        kpis = [
            tile("Intentos de checkout", num(c["intentos"]), f"{desde} → {hasta}"),
            tile("Tasa de error 5xx", "—" if c["tasa_error"] is None else f"{c['tasa_error']:.2%}",
                 f"{num(c['errores_5xx'])} errores / {num(c['intentos'])} intentos"),
            tile("Sesiones afectadas", num(c["sesiones_afectadas"]), "COUNT(DISTINCT id_sesion) en el intervalo"),
            tile("Latencia promedio", "—" if c["latencia_promedio_ms"] is None else f"{num(round(c['latencia_promedio_ms']))} ms",
                 "suma ÷ recuento de latencias"),
        ]
        fig_i, fig_t = go.Figure(), go.Figure()
        filas_tabla = []
        if len(f):
            for dev in [d for d in SERIES if d in set(f["dispositivo"])]:
                d = f[f["dispositivo"] == dev]
                fig_i.add_bar(x=d["minuto_local"], y=d["intentos"], name=dev, width=45_000,
                              marker=dict(color=SERIES[dev], line=dict(color=SURFACE, width=2)),
                              customdata=d[["errores_5xx", "estado_ventana"]].values,
                              hovertemplate=f"<b>{dev}</b> · %{{x|%H:%M}}<br>%{{y}} intentos · %{{customdata[0]}} errores"
                                            "<br>%{customdata[1]}<extra></extra>")
            g = f.groupby("minuto_local", as_index=False)[["errores_5xx", "intentos"]].sum()
            g["tasa"] = g["errores_5xx"] / g["intentos"]
            fig_t.add_scatter(x=g["minuto_local"], y=g["tasa"], mode="lines+markers", name="Tasa global",
                              line=dict(color=BLUE, width=2), marker=dict(size=8, color=BLUE,
                                                                          line=dict(color=SURFACE, width=2)),
                              customdata=g[["errores_5xx", "intentos"]].values,
                              hovertemplate="%{x|%H:%M}<br><b>%{y:.2%}</b> · %{customdata[0]}/%{customdata[1]}"
                                            "<extra></extra>")
            for r in f.itertuples():
                filas_tabla.append({"min": r.minuto_local.strftime("%d-%m %H:%M"), "dev": r.dispositivo,
                                    "int": num(r.intentos), "err": num(r.errores_5xx), "tasa": f"{r.tasa_error:.2%}",
                                    "ses": num(r.sesiones_afectadas),
                                    "lat": "—" if not r.latencia_n else num(round(r.latencia_suma_ms / r.latencia_n)),
                                    "estado": badge(r.estado_ventana),
                                    "ver": "—" if r.version != r.version or r.version is None else int(r.version),
                                    "fuente": r.fuente})
        rango = _rango_local(cfg, desde, hasta)
        base_layout(fig_i, height=280)
        fig_i.update_layout(barmode="stack", showlegend=True, legend_traceorder="normal")
        fig_i.update_xaxes(tickformat="%H:%M", range=rango, dtick=60_000)
        base_layout(fig_t, height=280, yformat=".0%")
        fig_t.update_xaxes(tickformat="%H:%M", range=rango, dtick=60_000)
        fig_t.update_yaxes(rangemode="tozero")
        tabla_web = tabla([("min", "Minuto (local)", False), ("dev", "Dispositivo", False), ("int", "Intentos", True),
                           ("err", "Errores 5xx", True), ("tasa", "Tasa", True), ("ses", "Sesiones afect.", True),
                           ("lat", "Latencia ms", True), ("estado", "Ventana", False), ("ver", "Versión", True),
                           ("fuente", "Fuente", False)], filas_tabla)

        try:
            s = q.stock(cfg, corte_stock)
        except ValueError as exc:
            return estado, kpis, fig_i, fig_t, tabla_web, str(exc), None
        filas_stock = []
        for r in s["filas"].to_dict("records") if len(s["filas"]) else []:
            clave = "negativo" if r["incidencia_saldo_negativo"] else ("reposicion" if r["alerta_reposicion"] else "ok")
            filas_stock.append({"sku": r["id_producto"], "ub": r["id_ubicacion"], "ini": num(r["disponible_inicial"]),
                                "mov": f"{int(r['delta_movimientos']):+d}".replace("-", "−") + f" ({int(r['n_movimientos'])})",
                                "saldo": num(r["saldo_disponible"]), "umbral": num(r["umbral_reposicion"]),
                                "estado": badge(clave)})
        estado_stock = [badge(s["estado"]),
                        f"  Corte {corte_stock}" + (f" · versión {s['version']}" if s["version"] else "")
                        + (f" · {len(s['pendientes'])} movimientos sin snapshot" if s["pendientes"] else "")]
        tabla_stock = tabla([("sku", "SKU", False), ("ub", "Ubicación", False), ("ini", "Disponible inicial", True),
                             ("mov", "Movimientos (n)", True), ("saldo", "Saldo disponible", True),
                             ("umbral", "Umbral", True), ("estado", "Estado", False)], filas_stock)
        return estado, kpis, fig_i, fig_t, tabla_web, estado_stock, tabla_stock

    return app


def serve(cfg: Config | None = None, host: str = "127.0.0.1", port: int = 8050) -> None:
    create_app(cfg).run(host=host, port=port, debug=False)
