"""
Konfigurasi ASGI untuk proyek PANDU.

File ini adalah entry point utama server ASGI (Daphne).
Mengarahkan traffic:
  - HTTP  → Django view biasa
  - ws:// → Django Channels WebSocket consumer

Alur koneksi WebSocket:
  Browser → ws://host/ws/dashboard/ → AuthMiddlewareStack → URLRouter
  → DashboardConsumer (consumers.py)
"""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pandu.settings')

# Inisialisasi Django terlebih dahulu sebelum import routing app
django_asgi_app = get_asgi_application()

import dashboard.routing  # noqa: E402 (import setelah setup Django)

application = ProtocolTypeRouter({
    # Tangani permintaan HTTP biasa via Django views
    'http': django_asgi_app,

    # Tangani koneksi WebSocket dengan autentikasi sesi Django
    'websocket': AuthMiddlewareStack(
        URLRouter(
            dashboard.routing.websocket_urlpatterns
        )
    ),
})
