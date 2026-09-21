"""
dashboard/db.py — Modul koneksi dan inisialisasi database MongoDB untuk PANDU.
https://127.0.0.1:54895/static/artifacts/fec692fb-482a-47a0-9056-850ed8b13483/pandu_konfigurasi_test_2_1789922390972.webp?csrf=74aa7285-a7df-44b0-8ea9-0c4abd43e704&t=1789922725669
Menyediakan:
  - Fungsi get_db()      : mengembalikan instance database MongoDB
  - Fungsi get_col()     : shortcut untuk mengambil koleksi tertentu
  - Fungsi init_indexes(): membuat indeks time-series secara otomatis

Skema Koleksi:
  ┌─────────────────────────────────────────────────────────────┐
  │ Koleksi: data_sensor                                        │
  │  Field          Tipe       Keterangan                       │
  │  timestamp      datetime   Waktu pembacaan sensor (index)   │
  │  device_id      str        ID perangkat ESP32               │
  │  kelembapan     float      % VWC kelembapan tanah           │
  │  ph             float      Nilai pH tanah (4.0–9.0)         │
  │  suhu           float      Suhu lingkungan dalam °C         │
  │  cahaya         float      Intensitas cahaya dalam Lux      │
  │  kelembapan_udara float    % kelembapan udara               │
  │  curah_hujan    float      mm/jam curah hujan               │
  │  ec             float      dS/m konduktivitas listrik tanah  │
  │  status         str        "optimal" | "warning" | "kritis" │
  ├─────────────────────────────────────────────────────────────┤
  │ Koleksi: data_prediksi                                      │
  │  Field            Tipe     Keterangan                       │
  │  timestamp        datetime Waktu inferensi model            │
  │  device_id        str      ID perangkat                     │
  │  estimasi_panen   float    Total estimasi panen (Ton)       │
  │  yield_per_ha     float    Ton per hektar                   │
  │  confidence       float    Confidence score model (0–100)   │
  │  status_lahan     str      Deskripsi status lahan           │
  │  proyeksi_tanggal str      Tanggal proyeksi panen           │
  │  feature_weights  dict     Bobot fitur input model          │
  │  tren_mingguan    list     Array 7 nilai tren mingguan      │
  │  model_versi      str      Versi model yang digunakan       │
  ├─────────────────────────────────────────────────────────────┤
  │ Koleksi: log_aktuator                                       │
  │  Field       Tipe     Keterangan                            │
  │  timestamp   datetime Waktu kejadian                        │
  │  aktuator    str      "pompa" | "valve"                     │
  │  aksi        str      "aktif" | "nonaktif"                  │
  │  trigger     str      "manual" | "otomatis_ai"              │
  │  pesan       str      Pesan log lengkap                     │
  └─────────────────────────────────────────────────────────────┘
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Variabel klien global — dibuat sekali, digunakan ulang (connection pooling)
_klien: MongoClient | None = None


def get_client() -> MongoClient:
    """
    Mengembalikan instance MongoClient yang sudah ada (singleton pattern).
    Membuat koneksi baru jika belum ada.
    """
    global _klien
    if _klien is None:
        _klien = MongoClient(
            settings.MONGO_URI,
            # Timeout 5 detik agar tidak hang jika MongoDB tidak bisa diakses
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
        )
        logger.info("[PANDU-DB] Koneksi MongoDB dibuka: %s", settings.MONGO_URI)
    return _klien


def get_db():
    """Mengembalikan instance database PANDU dari MongoDB."""
    return get_client()[settings.MONGO_DB_NAME]


def get_col(nama_koleksi: str) -> Collection:
    """
    Shortcut: mengambil koleksi MongoDB berdasarkan nama.

    Args:
        nama_koleksi: Nama koleksi yang diinginkan.

    Returns:
        Objek Collection PyMongo.

    Contoh:
        col = get_col('data_sensor')
        doc = col.find_one(sort=[('timestamp', -1)])
    """
    return get_db()[nama_koleksi]


def init_indexes():
    """
    Membuat indeks MongoDB yang diperlukan untuk performa optimal.
    Dipanggil oleh management command 'init_db'.

    Indeks yang dibuat:
      - data_sensor.timestamp    : descending (query data terbaru)
      - data_sensor.device_id    : ascending  (filter per perangkat)
      - data_prediksi.timestamp  : descending
      - log_aktuator.timestamp   : descending
    """
    db = get_db()

    # ─── Indeks koleksi data_sensor ─────────────────────────────
    col_sensor = db['data_sensor']
    col_sensor.create_index(
        [('timestamp', DESCENDING)],
        name='idx_sensor_timestamp_desc',
        background=True,
    )
    col_sensor.create_index(
        [('device_id', ASCENDING), ('timestamp', DESCENDING)],
        name='idx_sensor_device_waktu',
        background=True,
    )
    # Indeks TTL: otomatis hapus data sensor yang lebih dari 90 hari
    col_sensor.create_index(
        [('timestamp', ASCENDING)],
        name='idx_sensor_ttl_90hari',
        expireAfterSeconds=90 * 24 * 3600,
        background=True,
    )
    logger.info("[PANDU-DB] Indeks 'data_sensor' berhasil dibuat.")

    # ─── Indeks koleksi data_prediksi ───────────────────────────
    col_pred = db['data_prediksi']
    col_pred.create_index(
        [('timestamp', DESCENDING)],
        name='idx_prediksi_timestamp_desc',
        background=True,
    )
    # Indeks TTL: simpan riwayat prediksi selama 365 hari
    col_pred.create_index(
        [('timestamp', ASCENDING)],
        name='idx_prediksi_ttl_365hari',
        expireAfterSeconds=365 * 24 * 3600,
        background=True,
    )
    logger.info("[PANDU-DB] Indeks 'data_prediksi' berhasil dibuat.")

    # ─── Indeks koleksi log_aktuator ────────────────────────────
    col_log = db['log_aktuator']
    col_log.create_index(
        [('timestamp', DESCENDING)],
        name='idx_log_timestamp_desc',
        background=True,
    )
    logger.info("[PANDU-DB] Indeks 'log_aktuator' berhasil dibuat.")

    print("[PANDU] ✅ Semua indeks MongoDB berhasil dikonfigurasi.")
