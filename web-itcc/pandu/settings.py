"""
Konfigurasi utama Django untuk proyek PANDU
(Pengawas Agrikultur Terpadu).

Mendukung:
- Django Channels (WebSocket via Daphne/ASGI)
- MongoDB (via PyMongo langsung)
- MQTT (konfigurasi disimpan di sini, dipakai oleh mqtt_listener)
- Redis (sebagai Channel Layer untuk Channels)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Muat variabel lingkungan dari file .env
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# ─── Keamanan ─────────────────────────────────────────────────────────────────
SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-pandu-ganti-sebelum-deploy!')
DEBUG = os.getenv('DEBUG', 'True') == 'True'
ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', 'localhost,127.0.0.1').split(',')

# ─── Aplikasi Terpasang ───────────────────────────────────────────────────────
INSTALLED_APPS = [
    'daphne',                       # ASGI server (HARUS urutan pertama)
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'channels',                     # Django Channels untuk WebSocket
    'dashboard',                    # App utama PANDU
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'pandu.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        # Folder 'templates' di root proyek untuk template global
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# ─── ASGI: Menggantikan WSGI agar WebSocket dapat berjalan ───────────────────
ASGI_APPLICATION = 'pandu.asgi.application'

# ─── Database ─────────────────────────────────────────────────────────────────
# SQLite hanya dipakai untuk Django Admin (user, session, dll.)
# Data IoT (sensor, prediksi) disimpan langsung ke MongoDB via PyMongo
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# ─── Konfigurasi MongoDB (via PyMongo) ────────────────────────────────────────
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pandu_db')

# ─── Django Channels Layer ────────────────────────────────────────────────────
# Menggunakan InMemoryChannelLayer untuk pengembangan lokal (tanpa Redis).
# CATATAN: Untuk produksi atau multi-worker, ganti ke RedisChannelLayer.
# Cara ganti ke Redis:
#   pip install channels-redis
#   Ubah BACKEND ke 'channels_redis.core.RedisChannelLayer'
#   Tambahkan 'CONFIG': {'hosts': [os.getenv('REDIS_URL', 'redis://127.0.0.1:6379')]}
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels.layers.InMemoryChannelLayer',
    },
}

# ─── Konfigurasi MQTT ─────────────────────────────────────────────────────────
MQTT_BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'broker.hivemq.com')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'pandu-django-server')

# Topic MQTT yang di-subscribe dari ESP32
MQTT_TOPIC_SENSOR = os.getenv('MQTT_TOPIC_SENSOR', 'pandu/sensor/data')
MQTT_TOPIC_PREDICTION = os.getenv('MQTT_TOPIC_PREDICTION', 'pandu/ai/prediction')
MQTT_TOPIC_ACTUATOR = os.getenv('MQTT_TOPIC_ACTUATOR', 'pandu/actuator/status')

# ─── File Statis ──────────────────────────────────────────────────────────────
STATIC_URL = '/static/'
STATICFILES_DIRS = [BASE_DIR / 'static']

# ─── Lokalisasi ───────────────────────────────────────────────────────────────
LANGUAGE_CODE = 'id-id'
TIME_ZONE = 'Asia/Jakarta'
USE_I18N = True
USE_TZ = True

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
