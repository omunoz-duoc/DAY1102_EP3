"""CRM: normalización de identidad, resolución de fichas y mapa estable id_crm -> customer_id.

Reglas:
  * Fichas con RUT válido se agrupan por RUT.
  * Sin RUT: correo Y teléfono deben apuntar al mismo cliente. Si solo coincide una señal o apuntan a
    clientes distintos, la ficha va a revisión, recibe un customer_id provisional propio y no se fusiona.
  * Consentimiento más restrictivo: no > sin_registro > si.
  * Contacto: se prefiere el correo verificado; luego el más reciente.
"""
from __future__ import annotations

import json
import re
from datetime import datetime

from sqlalchemy import Connection, text

from tnr.common.contracts import QualityAlert, RawRecord, Rejection, SilverRecord
from tnr.common.identidad import normalize_email, normalize_phone, normalize_rut
from tnr.common.tipos import clean_text, is_blank, parse_fecha_negocio
from tnr.ingest._csv import CsvSnapshotSource

RESTRICCION = {"si": 0, "sin_registro": 1, "no": 2}
CANALES = {"online": "Online", "tienda": "Tienda"}


class ClientesSource(CsvSnapshotSource):
    fuente = "clientes"
    version_esquema = "clientes.v1"
    silver_table = "silver_clientes"
    archivo = "clientes_crm.csv"
    key_field = "id_crm"

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        id_crm = (d.get("id_crm") or "").strip().upper()
        if not re.fullmatch(r"CRM-[A-Z]?\d{4,5}", id_crm):
            return Rejection(record.ref, id_crm or None, "ID_CRM_INVALIDO")
        alerts: list[QualityAlert] = []

        rut, rut_err = normalize_rut(d.get("rut"))
        if rut_err and rut_err != "RUT_AUSENTE":
            alerts.append(QualityAlert("rut", rut_err))
        email = normalize_email(d.get("email"))
        if email is None and not is_blank(d.get("email")):
            alerts.append(QualityAlert("email", "EMAIL_INVALIDO"))
        telefono = normalize_phone(d.get("telefono"))
        if telefono is None and not is_blank(d.get("telefono")):
            alerts.append(QualityAlert("telefono", "TELEFONO_INVALIDO"))
        if not (rut or email or telefono):
            return Rejection(record.ref, id_crm, "SIN_SENALES_DE_IDENTIDAD")

        consent = (d.get("consentimiento_publicidad") or "").strip().lower()
        if consent not in RESTRICCION:
            alerts.append(QualityAlert("consentimiento_publicidad", "CONSENTIMIENTO_DESCONOCIDO", consent))
            consent = "sin_registro"

        verificado = (d.get("email_verificado") or "").strip().lower()
        if verificado not in {"true", "false"}:
            alerts.append(QualityAlert("email_verificado", "BOOLEANO_INVALIDO", verificado))
        try:
            actualizado = datetime.fromisoformat((d.get("actualizado_en") or "").strip()).isoformat()
        except ValueError:
            actualizado = None
            alerts.append(QualityAlert("actualizado_en", "TIMESTAMP_INVALIDO", d.get("actualizado_en")))
        fecha_alta = parse_fecha_negocio(d.get("fecha_alta"))
        if fecha_alta is None:
            alerts.append(QualityAlert("fecha_alta", "FECHA_ALTA_INVALIDA", d.get("fecha_alta")))
        canal = CANALES.get((d.get("canal_alta") or "").strip().lower())

        return SilverRecord(ref=record.ref, key=id_crm, alerts=alerts, data={
            "id_crm": id_crm, "rut": rut, "email": email, "telefono": telefono,
            "email_verificado": verificado == "true", "consentimiento": consent,
            "actualizado_en": actualizado or "", "fecha_alta": fecha_alta.isoformat() if fecha_alta else None,
            "canal_alta": canal, "comuna": clean_text(d.get("comuna")),
        })

    # ------------------------------------------------------------------ resolución
    @staticmethod
    def resolve(records: list[SilverRecord]) -> list[dict]:
        """Agrupa fichas en clústeres de cliente. Devuelve [{members, metodo, revision, evidencia}]."""
        clusters: list[dict] = []
        by_rut: dict[str, int] = {}
        email_idx: dict[str, set[int]] = {}
        phone_idx: dict[str, set[int]] = {}

        def index(i: int, rec: SilverRecord) -> None:
            if rec.data["email"]:
                email_idx.setdefault(rec.data["email"], set()).add(i)
            if rec.data["telefono"]:
                phone_idx.setdefault(rec.data["telefono"], set()).add(i)

        ordered = sorted(records, key=lambda r: r.key)
        for rec in (r for r in ordered if r.data["rut"]):
            rut = rec.data["rut"]
            if rut not in by_rut:
                by_rut[rut] = len(clusters)
                clusters.append({"members": [], "metodo": "rut", "revision": False, "evidencia": None})
            clusters[by_rut[rut]]["members"].append(rec)
            index(by_rut[rut], rec)

        for rec in (r for r in ordered if not r.data["rut"]):
            ce = email_idx.get(rec.data["email"] or "", set())
            cp = phone_idx.get(rec.data["telefono"] or "", set())
            if ce and cp and ce == cp and len(ce) == 1:
                i = next(iter(ce))
                clusters[i]["members"].append(rec)
                rec.data["_metodo"] = "correo_y_telefono"
                continue
            if not ce and not cp:
                metodo, revision, evidencia = "nuevo", False, None
            else:
                metodo, revision = "revision", True
                evidencia = {
                    "id_crm": rec.key,
                    "motivo": "SENALES_CONTRADICTORIAS" if ce and cp else "SENAL_UNICA_INSUFICIENTE",
                    "candidatos_por_correo": sorted(m.key for i in ce for m in clusters[i]["members"]),
                    "candidatos_por_telefono": sorted(m.key for i in cp for m in clusters[i]["members"]),
                }
            clusters.append({"members": [rec], "metodo": metodo, "revision": revision, "evidencia": evidencia})
            if not revision:
                index(len(clusters) - 1, rec)
        return clusters

    @staticmethod
    def golden(members: list[SilverRecord]) -> dict:
        recent = sorted(members, key=lambda r: (r.data["actualizado_en"], r.key), reverse=True)
        consent = max((m.data["consentimiento"] for m in members), key=RESTRICCION.__getitem__)
        emails = sorted((m for m in members if m.data["email"]),
                        key=lambda r: (r.data["email_verificado"], r.data["actualizado_en"], r.key), reverse=True)
        altas = sorted((m for m in members if m.data["fecha_alta"]), key=lambda r: (r.data["fecha_alta"], r.key))
        return {
            "consentimiento": consent,
            "email": emails[0].data["email"] if emails else None,
            "email_verificado": emails[0].data["email_verificado"] if emails else None,
            "telefono": next((m.data["telefono"] for m in recent if m.data["telefono"]), None),
            "comuna": next((m.data["comuna"] for m in recent if m.data["comuna"]), None),
            "fecha_alta": altas[0].data["fecha_alta"] if altas else None,
            "canal_alta": altas[0].data["canal_alta"] if altas else None,
        }

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        clusters = self.resolve(records)
        existing = dict(conn.execute(text("SELECT id_crm, customer_id FROM ident_crm_customer")).all())
        seq = max((int(c.split("-")[1]) for c in existing.values()), default=0)

        clusters.sort(key=lambda c: min(m.key for m in c["members"]))
        clientes, contactos, ruts, mapa, revisiones = [], [], [], [], []
        for c in clusters:
            known = sorted({existing[m.key] for m in c["members"] if m.key in existing})
            if known:
                customer_id = known[0]
            else:
                seq += 1
                customer_id = f"CUS-{seq:06d}"
            g = self.golden(c["members"])
            consents = {m.data["consentimiento"] for m in c["members"]}
            if len(consents) > 1:
                for m in c["members"]:
                    if m.data["consentimiento"] != g["consentimiento"]:
                        m.alerts.append(QualityAlert("consentimiento_publicidad", "CONSENTIMIENTO_CONFLICTO_RESTRICTIVO",
                                                     f"{m.data['consentimiento']}->{g['consentimiento']}"))
            if len(known) > 1:
                revisiones.append({"key": customer_id, "motivo": "FUSION_DE_CUSTOMER_IDS", "evidencia": json.dumps(known)})
            if c["revision"]:
                revisiones.append({"key": c["members"][0].key, "motivo": c["evidencia"]["motivo"],
                                   "evidencia": json.dumps(c["evidencia"], ensure_ascii=False)})
            clientes.append({"customer_id": customer_id, "comuna": g["comuna"], "canal_alta": g["canal_alta"],
                             "fecha_alta": g["fecha_alta"], "consentimiento_publicidad": g["consentimiento"],
                             "estado_identidad": "revision" if c["revision"] else "resuelto",
                             "n_fichas_crm": len(c["members"]), "run_id": run_id})
            contactos.append({"customer_id": customer_id, "email": g["email"], "verificado": g["email_verificado"],
                              "telefono": g["telefono"], "run_id": run_id})
            rut_set = {m.data["rut"] for m in c["members"] if m.data["rut"]}
            ruts += [{"rut": r, "customer_id": customer_id, "run_id": run_id} for r in rut_set]
            for m in c["members"]:
                metodo = m.data.get("_metodo") or c["metodo"]
                mapa.append({"id_crm": m.key, "customer_id": customer_id, "metodo": metodo, "run_id": run_id})

        conn.execute(text("DELETE FROM ident_contacto_cliente"))
        conn.execute(text("DELETE FROM ident_rut_customer"))
        conn.execute(text("DELETE FROM silver_clientes"))
        conn.execute(text(
            "INSERT INTO silver_clientes (customer_id, comuna, canal_alta, fecha_alta, consentimiento_publicidad,"
            " estado_identidad, n_fichas_crm, run_id) VALUES (:customer_id, :comuna, :canal_alta, :fecha_alta,"
            " :consentimiento_publicidad, :estado_identidad, :n_fichas_crm, :run_id)"), clientes)
        conn.execute(text(
            "INSERT INTO ident_contacto_cliente (customer_id, email_contacto, email_verificado, telefono, run_id)"
            " VALUES (:customer_id, :email, :verificado, :telefono, :run_id)"), contactos)
        if ruts:
            conn.execute(text("INSERT INTO ident_rut_customer (rut_normalizado, customer_id, run_id)"
                              " VALUES (:rut, :customer_id, :run_id)"), ruts)
        # mapa estable: UPSERT, nunca se borra
        conn.execute(text(
            "INSERT INTO ident_crm_customer (id_crm, customer_id, metodo_vinculo, run_id)"
            " VALUES (:id_crm, :customer_id, :metodo, :run_id)"
            " ON CONFLICT (id_crm) DO UPDATE SET customer_id = excluded.customer_id,"
            " metodo_vinculo = excluded.metodo_vinculo, run_id = excluded.run_id"), mapa)
        if revisiones:
            conn.execute(text("INSERT INTO etl_revision (run_id, fuente, id_registro, motivo, evidencia)"
                              " VALUES (:run_id, 'clientes', :key, :motivo, :evidencia)"),
                         [{**r, "run_id": run_id} for r in revisiones])
        return len(clientes)
