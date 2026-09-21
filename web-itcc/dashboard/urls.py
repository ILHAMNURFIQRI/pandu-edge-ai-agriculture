"""URL routing untuk app dashboard PANDU."""

from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    # Halaman utama dashboard PANDU
    path('', views.index, name='index'),
    # Endpoint API untuk mengambil riwayat data sensor (JSON)
    path('api/sensor/history/', views.sensor_history_api, name='sensor-history'),
    # Endpoint API untuk mengambil riwayat prediksi AI (JSON)
    path('api/prediction/history/', views.prediction_history_api, name='prediction-history'),
    # Endpoint API konfigurasi lahan
    path('api/konfigurasi/', views.konfigurasi_lahan_api, name='konfigurasi-api'),
]
