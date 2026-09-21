"""
dashboard/management/commands/mqtt_listener.py
Management command Django — MQTT Subscriber untuk proyek PANDU.

Cara menjalankan (di terminal/window terpisah dari server Django):
    python manage.py mqtt_listener

Fungsi utama:
  1. Subscribe ke 3 topic MQTT dari ESP32:
       - pandu/sensor/data       → data sensor lingkungan
       - pandu/ai/prediction     → hasil inferensi Coral Edge TPU
       - pandu/actuator/status   → perubahan status pompa/valve
  2. Parse payload JSON dari setiap pesan MQTT
  3. Simpan dokumen ke MongoDB via DAO
  4. Broadcast data ke semua browser yang terhubung via Django Channels
     (group 'dashboard_live')

Alur data lengkap:
  ESP32 → MQTT Broker → [mqtt_listener] → MongoDB
                                        → Channel Layer → DashboardConsumer → Browser

Format payload JSON yang diharapkan dari ESP32:

  Topic: pandu/sensor/data
  {
    "device_id"        : "esp32-pandu-01",
    "kelembapan"       : 72.5,
    "ph"               : 6.8,
    "suhu"             : 29.3,
    "cahaya"           : 54000.0,
    "kelembapan_udara" : 78.0,
    "curah_hujan"      : 0.0,
    "ec"               : 1.8
  }

  Topic: pandu/ai/prediction
  {
    "device_id"        : "esp32-pandu-01",
    "estimasi_panen"   : 46.2,
    "yield_per_ha"     : 15.4,
    "confidence"       : 94.7,
    "status_lahan"     : "Sangat Optimal",
    "proyeksi_tanggal" : "14 Oktober 2026",
    "feature_weights"  : {"kelembapan":32,"ph":24,"suhu":21,"cahaya":15,"ec":8},
    "tren_mingguan"    : [41.3, 42.1, 43.5, 44.2, 44.8, 45.5, 46.2],
    "model_versi"      : "CropYieldV3"
  }

  Topic: pandu/actuator/status
  {
    "device_id" : "esp32-pandu-01",
    "aktuator"  : "pompa",
    "aksi"      : "aktif",
    "trigger"   : "otomatis_ai",
    "pesan"     : "Pompa aktif. Kelembapan 68% -> target 75%"
  }
"""

import json
import logging
import signal
import sys
import asyncio
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from django.core.management.base import BaseCommand
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

# Nama group Channel Layer — harus sama dengan yang di consumers.py
GRUP_DASHBOARD = 'dashboard_live'

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Perintah Django untuk menjalankan MQTT Subscriber PANDU.
    Berjalan sebagai proses blocking (loop selamanya) sampai dihentikan.
    """

    help = 'Jalankan MQTT subscriber yang menjembatani ESP32 ke WebSocket'

    def handle(self, *args, **options):
        """Titik masuk utama command."""
        self.stdout.write('[PANDU-MQTT] Memulai MQTT Listener...')
        self.stdout.write(f'[PANDU-MQTT] Broker : {settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}')
        self.stdout.write(f'[PANDU-MQTT] Client ID: {settings.MQTT_CLIENT_ID}')

        # Inisialisasi MQTT client
        klien = _buat_klien_mqtt()

        # Tangani Ctrl+C dengan graceful shutdown
        def _sinyal_keluar(sig, frame):
            self.stdout.write('\n[PANDU-MQTT] Menghentikan listener...')
            klien.disconnect()
            klien.loop_stop()
            self.stdout.write('[PANDU-MQTT] Listener dihentikan.')
            sys.exit(0)

        signal.signal(signal.SIGINT, _sinyal_keluar)
        signal.signal(signal.SIGTERM, _sinyal_keluar)

        # Mulai loop MQTT (blocking)
        klien.loop_forever()


# ══════════════════════════════════════════════════════════════════════════════
# Fungsi Pembuatan Klien MQTT
# ══════════════════════════════════════════════════════════════════════════════

def _buat_klien_mqtt() -> mqtt.Client:
    """
    Membuat dan mengkonfigurasi instance klien MQTT Paho.

    Returns:
        Objek mqtt.Client yang sudah terhubung dan siap menerima pesan.
    """
    # Buat klien dengan protocol MQTT v3.1.1
    klien = mqtt.Client(
        client_id=settings.MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )

    # Set kredensial jika dikonfigurasi di .env
    if settings.MQTT_USERNAME:
        klien.username_pw_set(settings.MQTT_USERNAME, settings.MQTT_PASSWORD)

    # Daftarkan callback
    klien.on_connect    = _on_connect
    klien.on_disconnect = _on_disconnect
    klien.on_message    = _on_message

    # Konfigurasi reconnect otomatis (tunggu 2–60 detik antar percobaan)
    klien.reconnect_delay_set(min_delay=2, max_delay=60)

    # Hubungkan ke broker
    klien.connect(
        host=settings.MQTT_BROKER_HOST,
        port=settings.MQTT_BROKER_PORT,
        keepalive=60,
    )

    return klien


# ══════════════════════════════════════════════════════════════════════════════
# Callback MQTT
# ══════════════════════════════════════════════════════════════════════════════

def _on_connect(klien, userdata, flags, rc):
    """
    Dipanggil saat klien berhasil/gagal terhubung ke broker MQTT.

    rc = 0 → berhasil
    rc = 1 → versi protokol salah
    rc = 2 → client ID tidak valid
    rc = 3 → broker tidak tersedia
    rc = 4 → username/password salah
    rc = 5 → tidak diizinkan
    """
    if rc == 0:
        logger.info("[PANDU-MQTT] Terhubung ke broker MQTT (rc=0)")
        print(f'[PANDU-MQTT] Terhubung ke broker: {settings.MQTT_BROKER_HOST}')

        # Subscribe ke semua topic yang relevan
        topic_list = [
            (settings.MQTT_TOPIC_SENSOR,     1),  # QoS 1: at least once
            (settings.MQTT_TOPIC_PREDICTION, 1),
            (settings.MQTT_TOPIC_ACTUATOR,   1),
        ]
        klien.subscribe(topic_list)

        for topic, qos in topic_list:
            print(f'[PANDU-MQTT] Subscribed: {topic} (QoS {qos})')

    else:
        kode_error = {
            1: 'Versi protokol salah',
            2: 'Client ID tidak valid',
            3: 'Broker tidak tersedia',
            4: 'Username/password salah',
            5: 'Akses ditolak',
        }
        pesan = kode_error.get(rc, f'Error tidak dikenal (rc={rc})')
        logger.error("[PANDU-MQTT] Gagal terhubung: %s", pesan)
        print(f'[PANDU-MQTT] GAGAL terhubung: {pesan}')


def _on_disconnect(klien, userdata, rc):
    """
    Dipanggil saat koneksi ke broker terputus.
    Jika rc != 0, akan mencoba reconnect secara otomatis.
    """
    if rc == 0:
        logger.info("[PANDU-MQTT] Koneksi ditutup bersih.")
    else:
        logger.warning("[PANDU-MQTT] Koneksi terputus (rc=%s). Mencoba reconnect...", rc)
        print(f'[PANDU-MQTT] Koneksi terputus (rc={rc}). Reconnect otomatis...')


def _on_message(klien, userdata, msg):
    """
    Dipanggil setiap kali ada pesan masuk dari broker MQTT.
    Fungsi ini SINKRON (berjalan di thread MQTT callback).

    Alur:
      1. Decode payload JSON
      2. Tentukan tipe pesan berdasarkan topic
      3. Simpan ke MongoDB
      4. Broadcast ke Channel Layer → browser
    """
    try:
        topic   = msg.topic
        payload = json.loads(msg.payload.decode('utf-8'))
        logger.debug("[PANDU-MQTT] Pesan diterima [%s]: %s", topic, payload)

        # Routing berdasarkan topic MQTT
        if topic == settings.MQTT_TOPIC_SENSOR:
            _proses_sensor(payload)

        elif topic == settings.MQTT_TOPIC_PREDICTION:
            _proses_prediksi(payload)

        elif topic == settings.MQTT_TOPIC_ACTUATOR:
            _proses_aktuator(payload)

        else:
            logger.warning("[PANDU-MQTT] Topic tidak dikenal: %s", topic)

    except json.JSONDecodeError:
        logger.error("[PANDU-MQTT] Payload bukan JSON valid: %s", msg.payload[:100])
    except Exception as e:
        logger.error("[PANDU-MQTT] Error saat memproses pesan: %s", e, exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# Pemrosesan per Tipe Pesan
# ══════════════════════════════════════════════════════════════════════════════

def _proses_sensor(payload: dict):
    """
    Memproses payload data sensor dari ESP32:
      1. Buat objek DataSensor dan hitung status otomatis
      2. Simpan ke koleksi 'data_sensor' MongoDB
      3. Broadcast ke semua browser via WebSocket

    Args:
        payload: Dictionary hasil parse JSON dari MQTT.
    """
    from dashboard.models import DataSensor, SensorDAO

    try:
        # Buat objek DataSensor dari payload (field yang tidak ada diabaikan)
        sensor = DataSensor(
            device_id        = payload.get('device_id', 'esp32-pandu-01'),
            kelembapan       = float(payload.get('kelembapan', 0)),
            ph               = float(payload.get('ph', 7.0)),
            suhu             = float(payload.get('suhu', 25)),
            cahaya           = float(payload.get('cahaya', 0)),
            kelembapan_udara = float(payload.get('kelembapan_udara', 0)),
            curah_hujan      = float(payload.get('curah_hujan', 0)),
            ec               = float(payload.get('ec', 0)),
            timestamp        = datetime.now(timezone.utc),
        )
        sensor.status = sensor.hitung_status()

        # Simpan ke MongoDB
        SensorDAO.simpan(sensor)

        # Siapkan payload untuk dikirim ke browser
        # (hapus datetime agar JSON serializable)
        data_ws = sensor.ke_dict()
        data_ws['timestamp'] = sensor.timestamp.isoformat()

        # Broadcast ke group WebSocket
        _broadcast(tipe_handler='kirim_sensor', payload=data_ws)

        print(
            f'[SENSOR] K:{sensor.kelembapan}% '
            f'pH:{sensor.ph} '
            f'T:{sensor.suhu}C '
            f'L:{sensor.cahaya}Lx '
            f'[{sensor.status.upper()}]'
        )

    except Exception as e:
        logger.error("[PANDU-MQTT] Gagal proses sensor: %s", e)


def _proses_prediksi(payload: dict):
    """
    Memproses payload hasil prediksi AI dari Coral Edge TPU:
      1. Buat objek DataPrediksi
      2. Simpan ke koleksi 'data_prediksi' MongoDB
      3. Broadcast ke semua browser
      4. Tambahkan entri log sistem

    Args:
        payload: Dictionary hasil parse JSON dari MQTT.
    """
    from dashboard.models import DataPrediksi, PrediksiDAO

    try:
        prediksi = DataPrediksi(
            device_id        = payload.get('device_id', 'esp32-pandu-01'),
            estimasi_panen   = float(payload.get('estimasi_panen', 0)),
            yield_per_ha     = float(payload.get('yield_per_ha', 0)),
            confidence       = float(payload.get('confidence', 0)),
            status_lahan     = payload.get('status_lahan', 'Tidak Diketahui'),
            proyeksi_tanggal = payload.get('proyeksi_tanggal', ''),
            feature_weights  = payload.get('feature_weights', {}),
            tren_mingguan    = payload.get('tren_mingguan', [0.0] * 7),
            model_versi      = payload.get('model_versi', 'CropYieldV3'),
            timestamp        = datetime.now(timezone.utc),
        )

        PrediksiDAO.simpan(prediksi)

        data_ws = prediksi.ke_dict()
        data_ws['timestamp'] = prediksi.timestamp.isoformat()

        _broadcast(tipe_handler='kirim_prediksi', payload=data_ws)

        # Tambahkan log sistem ke browser
        jam = datetime.now().strftime('%H:%M')
        _broadcast(tipe_handler='kirim_log', payload={
            'pesan': f'{jam} · AI prediksi diperbarui: {prediksi.estimasi_panen}T '
                     f'(conf: {prediksi.confidence:.1f}%)',
            'warna': 'lime-deep',
        })

        print(f'[PREDIKSI] Panen: {prediksi.estimasi_panen}T | Conf: {prediksi.confidence}%')

    except Exception as e:
        logger.error("[PANDU-MQTT] Gagal proses prediksi: %s", e)


def _proses_aktuator(payload: dict):
    """
    Memproses payload status aktuator (pompa/valve):
      1. Buat objek LogAktuator
      2. Simpan ke koleksi 'log_aktuator' MongoDB
      3. Broadcast status ke browser (untuk update tombol ON/OFF)
      4. Broadcast entri log baru ke panel log

    Args:
        payload: Dictionary hasil parse JSON dari MQTT.
    """
    from dashboard.models import LogAktuator, AktuatorDAO

    try:
        log = LogAktuator(
            device_id = payload.get('device_id', 'esp32-pandu-01'),
            aktuator  = payload.get('aktuator', 'pompa'),
            aksi      = payload.get('aksi', 'nonaktif'),
            trigger   = payload.get('trigger', 'otomatis_ai'),
            pesan     = payload.get('pesan', ''),
            timestamp = datetime.now(timezone.utc),
        )

        AktuatorDAO.simpan(log)

        # Kirim update status aktuator ke browser
        data_ws = log.ke_dict()
        data_ws['timestamp'] = log.timestamp.isoformat()
        _broadcast(tipe_handler='kirim_aktuator', payload=data_ws)

        # Tambahkan ke log sistem di browser
        jam = datetime.now().strftime('%H:%M')
        _broadcast(tipe_handler='kirim_log', payload={
            'pesan': f'{jam} · {log.pesan}',
            'warna': 'forest' if log.aksi == 'aktif' else 'muted',
        })

        print(f'[AKTUATOR] {log.aktuator} -> {log.aksi} ({log.trigger})')

    except Exception as e:
        logger.error("[PANDU-MQTT] Gagal proses aktuator: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# Fungsi Broadcast ke Channel Layer
# ══════════════════════════════════════════════════════════════════════════════

def _broadcast(tipe_handler: str, payload: dict):
    """
    Mengirim pesan ke semua WebSocket consumer yang bergabung di group
    'dashboard_live' melalui Django Channels Layer.

    Fungsi ini SINKRON (menggunakan async_to_sync) karena dipanggil
    dari callback MQTT yang berjalan di thread biasa (bukan async).

    Args:
        tipe_handler : Nama handler di DashboardConsumer
                       (contoh: 'kirim_sensor' → method kirim_sensor())
        payload      : Data yang akan diteruskan ke browser.
    """
    try:
        channel_layer = get_channel_layer()

        # async_to_sync: menjalankan coroutine async dari konteks sinkron
        async_to_sync(channel_layer.group_send)(
            GRUP_DASHBOARD,
            {
                'type'   : tipe_handler,   # Maps ke method di consumer
                'payload': payload,
            }
        )

    except Exception as e:
        logger.error("[PANDU-MQTT] Gagal broadcast ke Channel Layer: %s", e)
