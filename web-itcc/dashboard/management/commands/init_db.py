"""
management/commands/init_db.py
Management command Django untuk menginisialisasi database MongoDB PANDU.

Cara menjalankan:
    python manage.py init_db

Yang dilakukan:
  1. Menguji koneksi ke MongoDB
  2. Membuat koleksi jika belum ada
  3. Membuat semua indeks (timestamp, device_id, TTL)
  4. Menyisipkan satu dokumen contoh (opsional, dengan flag --seed)

Contoh penggunaan:
    python manage.py init_db           # Hanya buat indeks
    python manage.py init_db --seed    # Buat indeks + insert data contoh
"""

from django.core.management.base import BaseCommand
from django.conf import settings
from datetime import datetime, timezone, timedelta
import random


class Command(BaseCommand):
    """Inisialisasi database MongoDB dan indeks untuk proyek PANDU."""

    help = 'Inisialisasi koleksi dan indeks MongoDB untuk PANDU'

    def add_arguments(self, parser):
        """Tambahkan argumen opsional --seed untuk data contoh."""
        parser.add_argument(
            '--seed',
            action='store_true',
            help='Sisipkan data sensor dan prediksi contoh ke MongoDB',
        )

    def handle(self, *args, **options):
        """Titik masuk utama command."""
        from dashboard.db import get_db, init_indexes
        from dashboard.models import DataSensor, DataPrediksi, LogAktuator, SensorDAO, PrediksiDAO, AktuatorDAO

        self.stdout.write(self.style.HTTP_INFO(
            '\n=================================================='
            '\n   PANDU - Inisialisasi Database MongoDB'
            '\n==================================================\n'
        ))

        # ─── Langkah 1: Uji Koneksi ──────────────────────────────
        self.stdout.write('▶ Menguji koneksi ke MongoDB...')
        try:
            db = get_db()
            # Ping server MongoDB untuk memastikan koneksi berhasil
            db.command('ping')
            self.stdout.write(self.style.SUCCESS(
                f'  ✅ Terhubung ke MongoDB: {settings.MONGO_URI}'
            ))
            self.stdout.write(self.style.SUCCESS(
                f'  ✅ Database: {settings.MONGO_DB_NAME}'
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'  ❌ Gagal terhubung ke MongoDB: {e}'
            ))
            self.stdout.write(self.style.WARNING(
                '  Pastikan MongoDB berjalan dan MONGO_URI di .env sudah benar.'
            ))
            return

        # ─── Langkah 2: Buat Indeks ──────────────────────────────
        self.stdout.write('\n▶ Membuat indeks koleksi...')
        try:
            init_indexes()
            self.stdout.write(self.style.SUCCESS(
                '  ✅ Indeks data_sensor    (timestamp ↓, device+waktu, TTL 90hr)'
            ))
            self.stdout.write(self.style.SUCCESS(
                '  ✅ Indeks data_prediksi  (timestamp ↓, TTL 365hr)'
            ))
            self.stdout.write(self.style.SUCCESS(
                '  ✅ Indeks log_aktuator   (timestamp ↓)'
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  ❌ Gagal membuat indeks: {e}'))
            return

        # ─── Langkah 3: Seed Data Contoh (opsional) ──────────────
        if options['seed']:
            self.stdout.write('\n▶ Menyisipkan data contoh...')
            self._seed_data(SensorDAO, PrediksiDAO, AktuatorDAO)
        else:
            self.stdout.write(self.style.WARNING(
                '\n  ℹ Lewati seed data. Jalankan dengan --seed untuk data contoh.'
            ))

        self.stdout.write(self.style.SUCCESS(
            '\n✅ Inisialisasi database PANDU selesai!\n'
        ))

    def _seed_data(self, SensorDAO, PrediksiDAO, AktuatorDAO):
        """
        Menyisipkan data contoh untuk pengujian dashboard.
        Membuat 10 dokumen sensor dan 3 prediksi historis.
        """
        from dashboard.models import DataSensor, DataPrediksi, LogAktuator

        # Hapus data lama (opsional, aman untuk dev)
        from dashboard.db import get_col
        get_col('data_sensor').delete_many({'device_id': 'esp32-demo'})
        get_col('data_prediksi').delete_many({'device_id': 'esp32-demo'})
        get_col('log_aktuator').delete_many({'device_id': 'esp32-demo'})

        # ── Seed: Data Sensor (10 titik data terakhir) ──
        now = datetime.now(timezone.utc)
        for i in range(10):
            waktu = now - timedelta(minutes=i * 10)
            sensor = DataSensor(
                device_id='esp32-demo',
                kelembapan=round(random.uniform(65, 80), 1),
                ph=round(random.uniform(6.0, 7.0), 1),
                suhu=round(random.uniform(25, 32), 1),
                cahaya=round(random.uniform(30000, 60000), 0),
                kelembapan_udara=round(random.uniform(70, 85), 1),
                curah_hujan=0.0,
                ec=round(random.uniform(1.5, 2.1), 1),
                timestamp=waktu,
            )
            SensorDAO.simpan(sensor)

        self.stdout.write(self.style.SUCCESS('  ✅ 10 dokumen data_sensor disisipkan'))

        # ── Seed: Data Prediksi (3 riwayat) ──
        tren_dasar = [41.3, 42.1, 43.5, 44.2, 44.8, 45.5, 46.2]
        for i in range(3):
            waktu = now - timedelta(hours=i * 8)
            prediksi = DataPrediksi(
                device_id='esp32-demo',
                estimasi_panen=round(46.2 - i * 1.5, 1),
                yield_per_ha=round((46.2 - i * 1.5) / 3.0, 1),
                confidence=round(random.uniform(88, 96), 1),
                status_lahan='Sangat Optimal' if i == 0 else 'Optimal',
                proyeksi_tanggal='14 Oktober 2026',
                feature_weights={
                    'kelembapan': 32, 'ph': 24,
                    'suhu': 21, 'cahaya': 15, 'ec': 8,
                },
                tren_mingguan=[round(v - i * 0.5, 1) for v in tren_dasar],
                model_versi='CropYieldV3',
                timestamp=waktu,
            )
            PrediksiDAO.simpan(prediksi)

        self.stdout.write(self.style.SUCCESS('  ✅ 3 dokumen data_prediksi disisipkan'))

        # ── Seed: Log Aktuator ──
        log_contoh = [
            LogAktuator(
                device_id='esp32-demo',
                aktuator='pompa',
                aksi='aktif',
                trigger='otomatis_ai',
                pesan='Pompa aktif. Kelembapan 68% → target 75%',
                timestamp=now - timedelta(minutes=9),
            ),
            LogAktuator(
                device_id='esp32-demo',
                aktuator='valve',
                aksi='nonaktif',
                trigger='otomatis_ai',
                pesan='Valve nutrisi selesai. Durasi 20 menit.',
                timestamp=now - timedelta(minutes=20),
            ),
        ]
        for log in log_contoh:
            AktuatorDAO.simpan(log)

        self.stdout.write(self.style.SUCCESS('  ✅ 2 dokumen log_aktuator disisipkan'))
