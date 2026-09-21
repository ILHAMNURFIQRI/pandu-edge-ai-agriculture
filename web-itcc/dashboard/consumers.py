"""
dashboard/consumers.py — WebSocket Consumer untuk proyek PANDU.

Alur kerja:
  1. Browser terhubung ke ws://host/ws/dashboard/
  2. DashboardConsumer menerima koneksi dan bergabung ke group 'dashboard_live'
  3. Script mqtt_listener.py mengirim pesan ke group yang sama via Channel Layer
  4. Consumer meneruskan pesan ke semua browser yang terhubung (broadcast)

Format pesan yang diterima dari Channel Layer (dari mqtt_listener):
  {
    "type"   : "kirim_sensor" | "kirim_prediksi" | "kirim_aktuator" | "kirim_log",
    "payload": { ... data ... }
  }

Format pesan yang dikirim ke browser (JSON via WebSocket):
  {
    "tipe"   : "sensor" | "prediksi" | "aktuator" | "log",
    "data"   : { ... data ... }
  }
"""

import json
import logging
from datetime import datetime, timezone

from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async

logger = logging.getLogger(__name__)

# Nama group Channel Layer — semua consumer dashboard berbagi group ini
GRUP_DASHBOARD = 'dashboard_live'


class DashboardConsumer(AsyncWebsocketConsumer):
    """
    Consumer WebSocket utama PANDU.

    Menangani koneksi browser dan mendistribusikan (broadcast) data
    real-time yang diterima dari MQTT listener ke semua klien aktif.
    """

    # ──────────────────────────────────────────────────────────────
    # Siklus Hidup Koneksi
    # ──────────────────────────────────────────────────────────────

    async def connect(self):
        """
        Dipanggil saat browser membuka koneksi WebSocket baru.
        Bergabung ke group 'dashboard_live' agar menerima broadcast.
        """
        # Bergabung ke group Channel Layer
        await self.channel_layer.group_add(
            GRUP_DASHBOARD,
            self.channel_name,
        )
        await self.accept()

        logger.info(
            "[WS] Klien terhubung: %s (total channel: %s)",
            self.channel_name,
            GRUP_DASHBOARD,
        )

        # Kirim data terbaru dari MongoDB ke klien yang baru terhubung
        # sehingga dashboard langsung terisi tanpa menunggu kiriman MQTT
        await self._kirim_snapshot_awal()

    async def disconnect(self, close_code):
        """
        Dipanggil saat browser menutup koneksi WebSocket.
        Keluar dari group agar tidak menerima broadcast lagi.
        """
        await self.channel_layer.group_discard(
            GRUP_DASHBOARD,
            self.channel_name,
        )
        logger.info(
            "[WS] Klien terputus: %s (kode: %s)",
            self.channel_name,
            close_code,
        )

    async def receive(self, text_data):
        """
        Dipanggil saat browser mengirim pesan ke server via WebSocket.
        Saat ini hanya menangani pesan 'ping' untuk keep-alive.
        """
        try:
            data = json.loads(text_data)
            tipe = data.get('tipe', '')

            if tipe == 'ping':
                # Balas ping dengan pong (keep-alive)
                await self.send(json.dumps({'tipe': 'pong', 'ts': _ts_sekarang()}))
            else:
                logger.debug("[WS] Pesan masuk dari browser: %s", data)

        except json.JSONDecodeError:
            logger.warning("[WS] Pesan tidak valid diterima: %s", text_data[:100])

    # ──────────────────────────────────────────────────────────────
    # Handler Pesan dari Channel Layer (dari mqtt_listener)
    # Penamaan: method name = "type" field dengan titik → garis bawah
    # Contoh: type="kirim_sensor" → method kirim_sensor()
    # ──────────────────────────────────────────────────────────────

    async def kirim_sensor(self, event):
        """
        Handler: meneruskan data sensor ke browser.
        Dipicu oleh mqtt_listener saat ada data baru dari topic sensor.
        """
        await self.send(json.dumps({
            'tipe': 'sensor',
            'data': event['payload'],
        }))

    async def kirim_prediksi(self, event):
        """
        Handler: meneruskan hasil prediksi AI ke browser.
        Dipicu oleh mqtt_listener saat ada inferensi baru dari Coral TPU.
        """
        await self.send(json.dumps({
            'tipe': 'prediksi',
            'data': event['payload'],
        }))

    async def kirim_aktuator(self, event):
        """
        Handler: meneruskan status aktuator ke browser.
        Dipicu oleh mqtt_listener saat pompa/valve berubah status.
        """
        await self.send(json.dumps({
            'tipe': 'aktuator',
            'data': event['payload'],
        }))

    async def kirim_log(self, event):
        """
        Handler: meneruskan entri log sistem baru ke browser.
        Browser akan menambahkan baris baru ke panel #system-log-list.
        """
        await self.send(json.dumps({
            'tipe': 'log',
            'data': event['payload'],
        }))

    async def kirim_sistem(self, event):
        """
        Handler: meneruskan status sistem (CPU, memori, uptime) ke browser.
        Dipicu periodik oleh mqtt_listener setiap beberapa detik.
        """
        await self.send(json.dumps({
            'tipe': 'sistem',
            'data': event['payload'],
        }))

    # ──────────────────────────────────────────────────────────────
    # Helper Privat
    # ──────────────────────────────────────────────────────────────

    async def _kirim_snapshot_awal(self):
        """
        Mengambil data terbaru dari MongoDB dan mengirimnya ke klien
        yang baru terhubung sebagai 'snapshot' awal dashboard.

        Berjalan secara async agar tidak memblokir event loop.
        """
        try:
            # Ambil data dari MongoDB secara sinkron di thread terpisah
            data_sensor = await database_sync_to_async(_ambil_sensor_terbaru)()
            data_prediksi = await database_sync_to_async(_ambil_prediksi_terbaru)()

            if data_sensor:
                await self.send(json.dumps({
                    'tipe': 'sensor',
                    'data': data_sensor,
                }, default=str))  # 'default=str' untuk serialisasi datetime

            if data_prediksi:
                await self.send(json.dumps({
                    'tipe': 'prediksi',
                    'data': data_prediksi,
                }, default=str))

            logger.debug("[WS] Snapshot awal dikirim ke %s", self.channel_name)

        except Exception as e:
            logger.error("[WS] Gagal mengirim snapshot awal: %s", e)


# ──────────────────────────────────────────────────────────────────────────────
# Fungsi Sinkron (dijalankan di thread pool via database_sync_to_async)
# ──────────────────────────────────────────────────────────────────────────────

def _ambil_sensor_terbaru() -> dict | None:
    """Mengambil dokumen sensor terbaru dari MongoDB (fungsi sinkron)."""
    from .models import SensorDAO
    return SensorDAO.ambil_terbaru()


def _ambil_prediksi_terbaru() -> dict | None:
    """Mengambil dokumen prediksi terbaru dari MongoDB (fungsi sinkron)."""
    from .models import PrediksiDAO
    return PrediksiDAO.ambil_terbaru()


def _ts_sekarang() -> str:
    """Mengembalikan timestamp ISO UTC sekarang sebagai string."""
    return datetime.now(timezone.utc).isoformat()
