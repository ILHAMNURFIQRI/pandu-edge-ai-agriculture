"""
dashboard/views.py — Views (Controller) untuk app dashboard PANDU.

Menangani:
  1. Render halaman utama dashboard (index) dengan data inisial MongoDB
  2. API endpoint JSON untuk riwayat sensor (GET /api/sensor/history/)
  3. API endpoint JSON untuk riwayat prediksi AI (GET /api/prediction/history/)
"""

import json
import logging
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta

from .models import SensorDAO, PrediksiDAO, KonfigurasiLahan, KonfigurasiDAO
from .db import get_db

logger = logging.getLogger(__name__)


def _serialisasi_datetime(obj):
    """
    Helper untuk JSON serializer: mengubah objek datetime menjadi string ISO.
    Dipakai sebagai 'default' parameter di json.dumps().
    """
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Tipe tidak dapat diserialisasi: {type(obj)}")


def index(request):
    """
    Tampilkan halaman utama dashboard PANDU.

    Context:
        data_sensor_awal : JSON string data sensor terbaru dari MongoDB
                           (digunakan untuk render awal sebelum WebSocket aktif)
        judul_halaman    : Judul halaman HTML
    """
    try:
        # Ambil data sensor terbaru dari MongoDB untuk render pertama halaman
        data_terakhir = SensorDAO.ambil_terbaru()
    except Exception as e:
        logger.warning("[PANDU-View] Gagal ambil data MongoDB: %s", e)
        data_terakhir = None

    konteks = {
        'data_sensor_awal': json.dumps(data_terakhir or {}, default=_serialisasi_datetime),
        'judul_halaman': 'PANDU — Pengawas Agrikultur Terpadu',
    }
    return render(request, 'dashboard/index.html', konteks)


@require_GET
def sensor_history_api(request):
    """
    API Endpoint: Kembalikan riwayat data sensor dalam format JSON.

    Method:  GET
    URL:     /api/sensor/history/?limit=50
    Params:
        limit (int): Jumlah maksimum dokumen. Default 50, maks 500.

    Returns:
        JSON: {"status": "ok", "jumlah": N, "data": [...]}
    """
    try:
        batas = min(int(request.GET.get('limit', 50)), 500)
        data = SensorDAO.ambil_riwayat(batas=batas)
        return JsonResponse({
            'status': 'ok',
            'jumlah': len(data),
            'data': data,
        }, json_dumps_params={'default': _serialisasi_datetime})
    except Exception as e:
        logger.error("[PANDU-API] Gagal ambil riwayat sensor: %s", e)
        return JsonResponse({'status': 'error', 'pesan': str(e)}, status=500)


@require_GET
def prediction_history_api(request):
    """
    API Endpoint: Kembalikan riwayat prediksi AI dalam format JSON.

    Method:  GET
    URL:     /api/prediction/history/?limit=20
    Params:
        limit (int): Jumlah maksimum dokumen. Default 20, maks 100.

    Returns:
        JSON: {"status": "ok", "jumlah": N, "data": [...]}
    """
    try:
        batas = min(int(request.GET.get('limit', 20)), 100)
        data = PrediksiDAO.ambil_riwayat(batas=batas)
        return JsonResponse({
            'status': 'ok',
            'jumlah': len(data),
            'data': data,
        }, json_dumps_params={'default': _serialisasi_datetime})
    except Exception as e:
        logger.error("[PANDU-API] Gagal ambil riwayat prediksi: %s", e)
        return JsonResponse({'status': 'error', 'pesan': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST", "GET"])
def konfigurasi_lahan_api(request):
    """
    API Endpoint: Simpan dan kalkulasi konfigurasi lahan.
    
    POST: Menerima JSON payload dari modal frontend, mensimulasikan
          Yield per Ha dan Tonase berdasarkan komoditas, lalu menyimpannya.
    GET:  Mengambil konfigurasi terbaru.
    """
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            luas_hektar = float(body.get('luas_hektar', 0))
            komoditas = body.get('komoditas', '')
            tanggal_tanam = body.get('tanggal_tanam', '')
            populasi_bibit = int(body.get('populasi_bibit', 0))

            # --- Simulasi Kalkulasi Sederhana Berdasarkan Komoditas ---
            konstanta_yield = 12.0 # Default ton/ha
            estimasi_hari_panen = 90
            
            if "Granola" in komoditas:
                konstanta_yield = 22.5
                estimasi_hari_panen = 100
            elif "Atlantik" in komoditas:
                konstanta_yield = 18.2
                estimasi_hari_panen = 110
            elif "Cabai" in komoditas:
                konstanta_yield = 14.5
                estimasi_hari_panen = 85

            # Hitung proyeksi tanggal panen
            try:
                tgl_obj = datetime.strptime(tanggal_tanam, "%Y-%m-%d")
                tgl_panen = tgl_obj + timedelta(days=estimasi_hari_panen)
                proyeksi_tanggal = tgl_panen.strftime("%d %B %Y")
            except ValueError:
                proyeksi_tanggal = "Format Tanggal Salah"

            yield_per_ha = konstanta_yield
            estimasi_panen = round(luas_hektar * yield_per_ha, 1)

            # Simpan konfigurasi dasar ke database
            konfigurasi = KonfigurasiLahan(
                luas_hektar=luas_hektar,
                komoditas=komoditas,
                tanggal_tanam=tanggal_tanam,
                populasi_bibit=populasi_bibit
            )
            KonfigurasiDAO.simpan(konfigurasi)

            # Kembalikan response JSON berisi hasil kalkulasi
            return JsonResponse({
                'status': 'ok',
                'pesan': 'Konfigurasi berhasil disimpan.',
                'data': {
                    'luas_hektar': f"{luas_hektar} Hektar",
                    'yield_per_ha': f"{yield_per_ha} Ton/Ha",
                    'estimasi_panen': estimasi_panen,
                    'proyeksi_tanggal': proyeksi_tanggal,
                    'komoditas': komoditas
                }
            })

        except Exception as e:
            logger.error(f"[PANDU-API] Gagal menyimpan konfigurasi: {e}")
            return JsonResponse({'status': 'error', 'pesan': str(e)}, status=400)
            
    elif request.method == 'GET':
        data_terbaru = KonfigurasiDAO.ambil_terbaru()
        return JsonResponse({
            'status': 'ok',
            'data': data_terbaru or {}
        }, json_dumps_params={'default': _serialisasi_datetime})
