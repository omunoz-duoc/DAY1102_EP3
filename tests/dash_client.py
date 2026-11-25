"""Invoca callbacks de Dash vía el endpoint HTTP real (/_dash-update-component) con el test client de Flask."""
from __future__ import annotations


def call(app, outputs: list[tuple[str, str]], inputs: list[tuple[str, str, object]]):
    out_key = "...".join(f"{i}.{p}" for i, p in outputs)
    payload = {
        "output": f"..{out_key}.." if len(outputs) > 1 else out_key,
        "outputs": [{"id": i, "property": p} for i, p in outputs] if len(outputs) > 1
        else {"id": outputs[0][0], "property": outputs[0][1]},
        "inputs": [{"id": i, "property": p, "value": v} for i, p, v in inputs],
        "changedPropIds": [f"{i}.{p}" for i, p, _ in inputs],
        "state": [],
    }
    r = app.server.test_client().post("/_dash-update-component", json=payload)
    assert r.status_code == 200, r.get_data(as_text=True)[:2000]
    return r.get_json()["response"]
