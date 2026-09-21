"""
tools/simulasi_mqtt.py — Simulator ESP32 untuk pengujian PANDU
tanpa perangkat keras nyata.

Cara menjalankan (di terminal KETIGA, setelah server & mqtt_listener aktif):
    python tools/simulasi_mqtt.py

Perilaku:
  - Mengirim data sensor setiap 3 detik dengan variasi acak
  - Mengirim prediksi AI setiap 30 detik
  - Mengirim status aktuator setiap 15 detik (toggle otomatis)
  - Berhenti dengan Ctrl+C

Konfigurasi: ambil dari .env melalui Django settings.
"""

import json
import math
import random
import signal
import sys
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

# Muat .env langsung (tanpa Django setup)
load_dotenv()

# ─── Konfigurasi dari .env ───────────────────────────────────────
BROKER_HOST  = os.getenv('MQTT_BROKER_HOST', 'broker.hivemq.com')
BROKER_PORT  = int(os.getenv('MQTT_BROKER_PORT', 1883))
CLIENT_ID    = 'pandu-simulator-esp32'
TOPIC_SENSOR = os.getenv('MQTT_TOPIC_SENSOR', 'pandu/sensor/data')
TOPIC_PRED   = os.getenv('MQTT_TOPIC_PREDICTION', 'pandu/ai/prediction')
TOPIC_ACT    = os.getenv('MQTT_TOPIC_ACTUATOR', 'pandu/actuator/status')
USERNAME     = os.getenv('MQTT_USERNAME', '')
PASSWORD     = os.getenv('MQTT_PASSWORD', '')


# ─── State internal simulator ────────────────────────────────────
_iterasi     = 0           # Penghitung iterasi
_pompa_aktif = True        # State pompa saat ini
_valve_aktif = False       # State valve saat ini


def buat_data_sensor() -> dict:
    """
    Menghasilkan data sensor dengan variasi sinusoidal + noise acak,
    mensimulasikan pembacaan sensor ESP32 yang realistis.
    """
    global _iterasi
    t = _iterasi * 0.1  # Parameter waktu untuk gelombang sinus

    return {
        'device_id'        : 'esp32-pandu-01',
        # Kelembapan berfluktuasi antara 60–82% dengan pola gelombang
        'kelembapan'       : round(71 + 6 * math.sin(t) + random.uniform(-1, 1), 1),
        # pH berfluktuasi kecil di sekitar 6.8 (stabil)
        'ph'               : round(6.8 + 0.15 * math.sin(t * 0.5) + random.uniform(-0.05, 0.05), 1),
        # Suhu meningkat perlahan lalu turun (pola siang-malam)
        'suhu'             : round(28 + 3 * math.sin(t * 0.3) + random.uniform(-0.5, 0.5), 1),
        # Cahaya bervariasi antara 30k–60k Lux
        'cahaya'           : round(45000 + 12000 * math.sin(t * 0.4) + random.uniform(-1000, 1000)),
        # Kelembapan udara berkebalikan dengan suhu
        'kelembapan_udara' : round(78 - 2 * math.sin(t * 0.3) + random.uniform(-1, 1), 1),
        'curah_hujan'      : round(random.uniform(0, 0.2) if random.random() > 0.9 else 0, 1),
        'ec'               : round(1.8 + 0.1 * math.sin(t * 0.2) + random.uniform(-0.05, 0.05), 1),
    }


def buat_data_prediksi() -> dict:
    """Menghasilkan data prediksi AI yang realistis."""
    estimasi = round(45 + random.uniform(-2, 2), 1)
    return {
        'device_id'        : 'esp32-pandu-01',
        'estimasi_panen'   : estimasi,
        'yield_per_ha'     : round(estimasi / 3.0, 1),
        'confidence'       : round(random.uniform(88, 97), 1),
        'status_lahan'     : random.choice(['Sangat Optimal', 'Optimal', 'Optimal']),
        'proyeksi_tanggal' : '14 Oktober 2026',
        'feature_weights'  : {
            'kelembapan': 32, 'ph': 24,
            'suhu': 21, 'cahaya': 15, 'ec': 8,
        },
        'tren_mingguan'    : [
            round(estimasi - 5 + i * 0.8, 1) for i in range(7)
        ],
        'model_versi'      : 'CropYieldV3',
    }


def buat_status_aktuator(aktuator: str, aktif: bool) -> dict:
    """Menghasilkan payload status aktuator."""
    if aktif:
        if aktuator == 'pompa':
            pesan = 'Pompa aktif. Irigasi zona A-B-C dimulai.'
        else:
            pesan = 'Valve nutrisi aktif. Fertigasi dimulai.'
    else:
        if aktuator == 'pompa':
            pesan = 'Pompa dimatikan. Siklus irigasi selesai.'
        else:
            pesan = 'Valve nutrisi dimatikan. Fertigasi selesai.'

    return {
        'device_id' : 'esp32-pandu-01',
        'aktuator'  : aktuator,
        'aksi'      : 'aktif' if aktif else 'nonaktif',
        'trigger'   : 'otomatis_ai',
        'pesan'     : pesan,
    }


def _on_connect(klien, userdata, flags, rc):
    if rc == 0:
        print(f'[SIM] Terhubung ke broker: {BROKER_HOST}:{BROKER_PORT}')
        print('[SIM] Mulai mengirim data simulasi... (Ctrl+C untuk berhenti)\n')
    else:
        print(f'[SIM] GAGAL terhubung (rc={rc}). Periksa konfigurasi broker.')
        sys.exit(1)


def main():
    global _iterasi, _pompa_aktif, _valve_aktif

    # Buat klien MQTT
    klien = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
    if USERNAME:
        klien.username_pw_set(USERNAME, PASSWORD)
    klien.on_connect = _on_connect

    print(f'[SIM] Menghubungkan ke {BROKER_HOST}:{BROKER_PORT}...')
    klien.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    klien.loop_start()

    # Tunggu koneksi selesai
    time.sleep(2)

    # ── Graceful shutdown ──
    def _keluar(sig, frame):
        print('\n[SIM] Simulasi dihentikan.')
        klien.loop_stop()
        klien.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, _keluar)

    # ── Loop utama simulasi ──
    hitungan_prediksi  = 0
    hitungan_aktuator  = 0

    while True:
        _iterasi += 1

        # ── Kirim data sensor setiap iterasi (3 detik) ──
        data_sensor = buat_data_sensor()
        klien.publish(TOPIC_SENSOR, json.dumps(data_sensor), qos=1)
        jam = datetime.now().strftime('%H:%M:%S')
        print(
            f'[{jam}] SENSOR | '
            f'K:{data_sensor["kelembapan"]}% '
            f'pH:{data_sensor["ph"]} '
            f'T:{data_sensor["suhu"]}C '
            f'L:{data_sensor["cahaya"]:.0f}Lx'
        )

        # ── Kirim prediksi AI setiap 10 iterasi (30 detik) ──
        hitungan_prediksi += 1
        if hitungan_prediksi >= 10:
            data_pred = buat_data_prediksi()
            klien.publish(TOPIC_PRED, json.dumps(data_pred), qos=1)
            print(f'[{jam}] PREDIKSI | Panen:{data_pred["estimasi_panen"]}T Conf:{data_pred["confidence"]}%')
            hitungan_prediksi = 0

        # ── Toggle aktuator setiap 5 iterasi (15 detik) ──
        hitungan_aktuator += 1
        if hitungan_aktuator >= 5:
            # Toggle pompa
            _pompa_aktif = not _pompa_aktif
            data_act = buat_status_aktuator('pompa', _pompa_aktif)
            klien.publish(TOPIC_ACT, json.dumps(data_act), qos=1)
            status_str = 'AKTIF' if _pompa_aktif else 'OFF'
            print(f'[{jam}] AKTUATOR | pompa -> {status_str}')
            hitungan_aktuator = 0

        time.sleep(3)


if __name__ == '__main__':
    main()
