"""Sirve respuestas de postventa de la simulación TNR en localhost."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class PostventaHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        page = parse_qs(url.query).get('page', ['1'])
        if url.path != '/ordenes' or len(page) != 1 or page[0] not in {'1', '2', '3'}:
            self.send_error(404)
            return

        fixture = Path(__file__).with_name(f'postventa_pagina_{page[0]}.json')
        payload = fixture.read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == '__main__':
    with ThreadingHTTPServer(('127.0.0.1', 8766), PostventaHandler) as server:
        print('Postventa TNR en http://127.0.0.1:8766/ordenes', flush=True)
        server.serve_forever()
