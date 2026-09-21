"""Konfigurasi URL utama proyek PANDU."""

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # Panel admin Django bawaan
    path('admin/', admin.site.urls),
    # Delegasikan semua URL ke app dashboard
    path('', include('dashboard.urls')),
]
