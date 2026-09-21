"""
Routing WebSocket untuk app dashboard PANDU.

Mendefinisikan pola URL WebSocket yang akan ditangani
oleh DashboardConsumer.

Pola URL WebSocket:
  ws://localhost:8000/ws/dashboard/  →  DashboardConsumer
"""

from django.urls import re_path
from . import consumers

# Daftar pola URL untuk koneksi WebSocket
websocket_urlpatterns = [
    re_path(
        r'ws/dashboard/$',
        consumers.DashboardConsumer.as_asgi(),
        name='ws-dashboard',
    ),
]
