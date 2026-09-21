"""
dashboard/models.py — Representasi Dokumen MongoDB untuk PANDU.

Karena menggunakan PyMongo (bukan Django ORM), file ini mendefinisikan:
  - Dataclass Python sebagai "skema" dokumen
  - Class DAO (Data Access Object) untuk operasi CRUD ke MongoDB
  - Fungsi serialisasi/deserialisasi dokumen

Tiga entitas utama:
  1. DataSensor       — Data time-series dari sensor ESP32
  2. DataPrediksi     — Hasil inferensi model Edge AI (Crop Yield)
  3. LogAktuator      — Riwayat kejadian pompa/valve
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from .db import get_col

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# BAGIAN 1: DATACLASS — Representasi Skema Dokumen
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class DataSensor:
    """
    Merepresentasikan satu dokumen di koleksi 'data_sensor'.

    Setiap field sensor sesuai dengan payload JSON dari ESP32
    yang dikirimkan melalui MQTT topic: pandu/sensor/data

    Contoh payload MQTT dari ESP32 (format JSON):
    {
        "device_id": "esp32-pandu-01",
        "kelembapan": 72.5,
        "ph": 6.8,
        "suhu": 29.3,
        "cahaya": 54000,
        "kelembapan_udara": 78.0,
        "curah_hujan": 0.0,
        "ec": 1.8
    }
    """
    # Identitas perangkat
    device_id: str = "esp32-pandu-01"

    # Data sensor utama (4 kartu besar di dashboard)
    kelembapan: float = 0.0        # % VWC (Volumetric Water Content)
    ph: float = 7.0                # Nilai pH tanah
    suhu: float = 25.0             # Suhu lingkungan dalam °C
    cahaya: float = 0.0            # Intensitas cahaya dalam Lux

    # Data sensor sekunder (strip bawah dashboard)
    kelembapan_udara: float = 0.0  # % kelembapan relatif udara
    curah_hujan: float = 0.0       # mm/jam
    ec: float = 0.0                # dS/m (Electrical Conductivity)

    # Metadata yang digenerate server (bukan dari ESP32)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    status: str = "optimal"        # "optimal" | "warning" | "kritis"

    def hitung_status(self) -> str:
        """
        Menentukan status sensor berdasarkan ambang batas agronomi.

        Returns:
            "optimal"  jika semua parameter dalam rentang ideal
            "warning"  jika ada parameter mendekati batas
            "kritis"   jika ada parameter melewati batas kritis
        """
        peringatan = 0

        # Cek kelembapan tanah (ideal: 65–80%)
        if not (60 <= self.kelembapan <= 85):
            peringatan += 2 if (self.kelembapan < 40 or self.kelembapan > 95) else 1

        # Cek pH (ideal: 6.0–7.0)
        if not (5.5 <= self.ph <= 7.5):
            peringatan += 2 if (self.ph < 4.5 or self.ph > 8.5) else 1

        # Cek suhu (ideal: 24–32°C)
        if not (20 <= self.suhu <= 35):
            peringatan += 2 if (self.suhu < 10 or self.suhu > 42) else 1

        if peringatan >= 4:
            return "kritis"
        elif peringatan >= 1:
            return "warning"
        return "optimal"

    def ke_dict(self) -> dict:
        """Konversi dataclass ke dictionary untuk disimpan ke MongoDB."""
        d = asdict(self)
        d['status'] = self.hitung_status()
        return d

    @classmethod
    def dari_dict(cls, data: dict) -> 'DataSensor':
        """
        Buat instance DataSensor dari dictionary (hasil query MongoDB).
        Field '_id' MongoDB diabaikan secara otomatis.
        """
        data.pop('_id', None)  # Hapus field ObjectId dari MongoDB
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DataPrediksi:
    """
    Merepresentasikan satu dokumen di koleksi 'data_prediksi'.

    Dikirim melalui MQTT topic: pandu/ai/prediction
    setelah Google Coral Edge TPU selesai melakukan inferensi.

    Contoh payload MQTT:
    {
        "device_id": "esp32-pandu-01",
        "estimasi_panen": 46.2,
        "yield_per_ha": 15.4,
        "confidence": 94.7,
        "status_lahan": "Sangat Optimal",
        "proyeksi_tanggal": "14 Oktober 2026",
        "feature_weights": {
            "kelembapan": 32, "ph": 24, "suhu": 21,
            "cahaya": 15, "ec": 8
        },
        "tren_mingguan": [41.3, 42.1, 43.5, 44.2, 44.8, 45.5, 46.2],
        "model_versi": "CropYieldV3"
    }
    """
    device_id: str = "esp32-pandu-01"

    # Hasil prediksi utama
    estimasi_panen: float = 0.0     # Total estimasi panen dalam Ton
    yield_per_ha: float = 0.0       # Ton per hektar
    confidence: float = 0.0         # Confidence score model (0–100%)

    # Metadata status
    status_lahan: str = "Tidak Diketahui"
    proyeksi_tanggal: str = ""

    # Detail model
    feature_weights: dict = field(default_factory=lambda: {
        "kelembapan": 32,
        "ph": 24,
        "suhu": 21,
        "cahaya": 15,
        "ec": 8,
    })
    tren_mingguan: list = field(default_factory=lambda: [0.0] * 7)
    model_versi: str = "CropYieldV3"

    # Metadata server
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def ke_dict(self) -> dict:
        """Konversi ke dictionary untuk disimpan ke MongoDB."""
        return asdict(self)

    @classmethod
    def dari_dict(cls, data: dict) -> 'DataPrediksi':
        """Buat instance dari dictionary hasil query MongoDB."""
        data.pop('_id', None)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class LogAktuator:
    """
    Merepresentasikan satu dokumen di koleksi 'log_aktuator'.

    Dikirim melalui MQTT topic: pandu/actuator/status
    setiap kali pompa atau valve berubah status.

    Contoh payload MQTT:
    {
        "device_id": "esp32-pandu-01",
        "aktuator": "pompa",
        "aksi": "aktif",
        "trigger": "otomatis_ai",
        "pesan": "Pompa aktif. Kelembapan 68% → target 75%"
    }
    """
    device_id: str = "esp32-pandu-01"
    aktuator: str = "pompa"        # "pompa" | "valve"
    aksi: str = "aktif"            # "aktif" | "nonaktif"
    trigger: str = "manual"        # "manual" | "otomatis_ai"
    pesan: str = ""

    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def ke_dict(self) -> dict:
        """Konversi ke dictionary untuk disimpan ke MongoDB."""
        return asdict(self)


# ══════════════════════════════════════════════════════════════════════════════
# BAGIAN 2: DAO — Data Access Object (Operasi CRUD)
# ══════════════════════════════════════════════════════════════════════════════

class SensorDAO:
    """
    Data Access Object untuk koleksi 'data_sensor'.
    Menyediakan operasi: simpan, ambil terbaru, ambil riwayat.
    """

    KOLEKSI = 'data_sensor'

    @classmethod
    def simpan(cls, data: DataSensor) -> str:
        """
        Menyimpan satu dokumen sensor ke MongoDB.

        Args:
            data: Objek DataSensor yang akan disimpan.

        Returns:
            String ID dokumen yang baru dibuat (_id MongoDB).
        """
        col = get_col(cls.KOLEKSI)
        hasil = col.insert_one(data.ke_dict())
        logger.debug("[SensorDAO] Dokumen disimpan: %s", hasil.inserted_id)
        return str(hasil.inserted_id)

    @classmethod
    def ambil_terbaru(cls, device_id: Optional[str] = None) -> Optional[dict]:
        """
        Mengambil satu dokumen sensor paling baru.

        Args:
            device_id: Filter berdasarkan ID perangkat (opsional).

        Returns:
            Dictionary dokumen atau None jika koleksi kosong.
        """
        col = get_col(cls.KOLEKSI)
        filter_query = {'device_id': device_id} if device_id else {}
        dokumen = col.find_one(
            filter_query,
            {'_id': 0},                        # Jangan sertakan field _id
            sort=[('timestamp', -1)],           # Ambil yang paling baru
        )
        return dokumen

    @classmethod
    def ambil_riwayat(cls, batas: int = 50, device_id: Optional[str] = None) -> list:
        """
        Mengambil beberapa dokumen sensor terbaru.

        Args:
            batas:     Jumlah maksimum dokumen yang diambil.
            device_id: Filter berdasarkan ID perangkat (opsional).

        Returns:
            List dictionary dokumen, diurutkan dari terbaru ke terlama.
        """
        col = get_col(cls.KOLEKSI)
        filter_query = {'device_id': device_id} if device_id else {}
        return list(
            col.find(filter_query, {'_id': 0})
               .sort('timestamp', -1)
               .limit(batas)
        )


class PrediksiDAO:
    """Data Access Object untuk koleksi 'data_prediksi'."""

    KOLEKSI = 'data_prediksi'

    @classmethod
    def simpan(cls, data: DataPrediksi) -> str:
        """Menyimpan satu hasil prediksi ke MongoDB."""
        col = get_col(cls.KOLEKSI)
        hasil = col.insert_one(data.ke_dict())
        logger.debug("[PrediksiDAO] Prediksi disimpan: %s", hasil.inserted_id)
        return str(hasil.inserted_id)

    @classmethod
    def ambil_terbaru(cls) -> Optional[dict]:
        """Mengambil satu hasil prediksi paling baru."""
        col = get_col(cls.KOLEKSI)
        return col.find_one({}, {'_id': 0}, sort=[('timestamp', -1)])

    @classmethod
    def ambil_riwayat(cls, batas: int = 20) -> list:
        """Mengambil beberapa hasil prediksi terbaru."""
        col = get_col(cls.KOLEKSI)
        return list(
            col.find({}, {'_id': 0})
               .sort('timestamp', -1)
               .limit(batas)
        )


class AktuatorDAO:
    """Data Access Object untuk koleksi 'log_aktuator'."""

    KOLEKSI = 'log_aktuator'

    @classmethod
    def simpan(cls, log: LogAktuator) -> str:
        """Menyimpan satu entri log aktuator ke MongoDB."""
        col = get_col(cls.KOLEKSI)
        hasil = col.insert_one(log.ke_dict())
        return str(hasil.inserted_id)

    @classmethod
    def ambil_log_terbaru(cls, batas: int = 20) -> list:
        """
        Mengambil entri log aktuator terbaru.

        Returns:
            List dictionary log, diurutkan dari terbaru ke terlama.
        """
        col = get_col(cls.KOLEKSI)
        return list(
            col.find({}, {'_id': 0})
               .sort('timestamp', -1)
               .limit(batas)
        )

@dataclass
class KonfigurasiLahan:
    """
    Merepresentasikan pengaturan profil lahan dari input manual pengguna.
    """
    luas_hektar: float = 0.0
    komoditas: str = ""
    tanggal_tanam: str = ""
    populasi_bibit: int = 0
    
    # Metadata server
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def ke_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def dari_dict(cls, data: dict) -> 'KonfigurasiLahan':
        data.pop('_id', None)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class KonfigurasiDAO:
    """Data Access Object untuk koleksi 'konfigurasi_lahan'."""

    KOLEKSI = 'konfigurasi_lahan'

    @classmethod
    def simpan(cls, data: KonfigurasiLahan) -> str:
        """Menyimpan satu entri konfigurasi lahan ke MongoDB."""
        col = get_col(cls.KOLEKSI)
        hasil = col.insert_one(data.ke_dict())
        return str(hasil.inserted_id)

    @classmethod
    def ambil_terbaru(cls) -> Optional[dict]:
        """Mengambil konfigurasi lahan yang paling terakhir diinputkan."""
        col = get_col(cls.KOLEKSI)
        return col.find_one({}, {'_id': 0}, sort=[('timestamp', -1)])
