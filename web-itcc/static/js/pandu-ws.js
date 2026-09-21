/**
 * pandu-ws.js — WebSocket Client & DOM Wiring untuk PANDU Dashboard
 *
 * Tanggung jawab:
 *   1. Membuka dan mempertahankan koneksi WebSocket ke server Django
 *   2. Menangkap 5 tipe pesan: sensor, prediksi, aktuator, log, sistem
 *   3. Memperbarui elemen DOM secara real-time tanpa page reload
 *   4. Menampilkan status koneksi di navbar
 *   5. Reconnect otomatis dengan exponential backoff
 *
 * Peta ID elemen yang diperbarui:
 *   Sensor     → s-{nama}-{nilai|tren|bar|status}
 *   Prediksi   → ai-{nama}
 *   Aktuator   → act-{aktuator}-{metrik}
 *   Chart      → chart-bar-{hari}
 *   Log        → #system-log-list
 *   Koneksi    → #ws-status-badge
 *
 * Variabel global yang dibutuhkan (ditetapkan di dashboard/index.html):
 *   WS_URL       : URL WebSocket (ws:// atau wss://)
 *   PANDU_INIT_DATA : Data sensor awal dari Django view (JSON)
 */

'use strict';

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 1 — KONFIGURASI & STATE
   ════════════════════════════════════════════════════════════════════════════ */

/** Konfigurasi reconnect: jeda awal, faktor pengali, batas maksimum */
const RECONNECT_INIT_MS  = 1000;   // Jeda pertama: 1 detik
const RECONNECT_FAKTOR   = 2;      // Setiap gagal, jeda x2
const RECONNECT_MAKS_MS  = 30000;  // Batas atas jeda: 30 detik
const PING_INTERVAL_MS   = 25000;  // Kirim ping setiap 25 detik (keep-alive)

/** State WebSocket */
let _ws               = null;   // Instance WebSocket aktif
let _jeda_reconnect   = RECONNECT_INIT_MS;
let _timer_reconnect  = null;
let _timer_ping       = null;
let _terhubung        = false;

/** Ketinggian maksimum chart bar mingguan dalam px */
const CHART_TINGGI_MAKS = 90;

/** Mapping hari ke ID elemen chart bar */
const CHART_HARI = ['sen', 'sel', 'rab', 'kam', 'jum', 'sab', 'mng'];

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 2 — MANAJEMEN KONEKSI WEBSOCKET
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Membuka koneksi WebSocket baru ke server Django (Daphne).
 * Dipanggil saat halaman pertama kali dimuat dan saat reconnect.
 */
function panduWsConnect() {
  if (!window.WS_URL) {
    console.error('[PANDU-WS] WS_URL tidak terdefinisi.');
    return;
  }

  _setStatusKoneksi('reconnecting');
  console.log('[PANDU-WS] Menghubungkan ke:', WS_URL);

  _ws = new WebSocket(WS_URL);

  /* ── Event: Koneksi berhasil dibuka ── */
  _ws.addEventListener('open', () => {
    console.log('[PANDU-WS] Terhubung!');
    _terhubung       = true;
    _jeda_reconnect  = RECONNECT_INIT_MS; // Reset jeda reconnect

    _setStatusKoneksi('connected');

    // Mulai kiriman ping periodik untuk mencegah timeout
    _mulaiPing();

    // Terapkan data inisial dari Django view (jika ada)
    if (window.PANDU_INIT_DATA && Object.keys(PANDU_INIT_DATA).length > 0) {
      _updateSensor(PANDU_INIT_DATA);
      console.log('[PANDU-WS] Data inisial diterapkan dari server.');
    }
  });

  /* ── Event: Pesan diterima dari server ── */
  _ws.addEventListener('message', (event) => {
    try {
      const pesan = JSON.parse(event.data);
      _routekanPesan(pesan);
    } catch (e) {
      console.warn('[PANDU-WS] Pesan tidak valid:', event.data);
    }
  });

  /* ── Event: Koneksi terputus ── */
  _ws.addEventListener('close', (event) => {
    _terhubung = false;
    _hentikanPing();
    _setStatusKoneksi('disconnected');
    console.warn(`[PANDU-WS] Koneksi terputus (kode: ${event.code}). Reconnect dalam ${_jeda_reconnect / 1000}s...`);

    // Jadwalkan reconnect dengan exponential backoff
    _timer_reconnect = setTimeout(() => {
      _jeda_reconnect = Math.min(_jeda_reconnect * RECONNECT_FAKTOR, RECONNECT_MAKS_MS);
      panduWsConnect();
    }, _jeda_reconnect);
  });

  /* ── Event: Error koneksi ── */
  _ws.addEventListener('error', (error) => {
    console.error('[PANDU-WS] Error WebSocket:', error);
    _setStatusKoneksi('disconnected');
  });
}

/**
 * Mengirim pesan ping ke server setiap PING_INTERVAL_MS detik.
 * Mencegah koneksi idle ter-timeout oleh proxy atau server.
 */
function _mulaiPing() {
  _hentikanPing();
  _timer_ping = setInterval(() => {
    if (_ws && _ws.readyState === WebSocket.OPEN) {
      _ws.send(JSON.stringify({ tipe: 'ping' }));
    }
  }, PING_INTERVAL_MS);
}

function _hentikanPing() {
  if (_timer_ping) { clearInterval(_timer_ping); _timer_ping = null; }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 3 — ROUTING PESAN
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Meneruskan pesan yang diterima dari WebSocket ke handler yang sesuai.
 * @param {Object} pesan - Objek {tipe, data} dari server
 */
function _routekanPesan(pesan) {
  const { tipe, data } = pesan;

  switch (tipe) {
    case 'sensor':    _updateSensor(data);    break;
    case 'prediksi':  _updatePrediksi(data);  break;
    case 'aktuator':  _updateAktuator(data);  break;
    case 'log':       _tambahLog(data);        break;
    case 'sistem':    _updateSistem(data);     break;
    case 'pong':      /* keep-alive, abaikan */ break;
    default:
      console.log('[PANDU-WS] Tipe pesan tidak dikenal:', tipe);
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 4 — UPDATE DOM: SENSOR
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Memperbarui semua elemen DOM yang terkait data sensor.
 * @param {Object} d - Dokumen sensor dari MongoDB/WebSocket
 */
function _updateSensor(d) {
  if (!d || typeof d !== 'object') return;

  /* ── 4.1 Kelembapan Tanah ── */
  if (d.kelembapan !== undefined) {
    const val = parseFloat(d.kelembapan);
    const pct = Math.min(Math.max(val, 0), 100);

    _setText('s-kelembapan-nilai', Math.round(val));
    _setBar('s-kelembapan-bar', pct);

    // Tentukan status dan warna tren
    const { label, kelas } = _statusKelembapan(val);
    _setStatusChip('s-kelembapan-status', label);
    _setText('s-kelembapan-tren', _trendLabel(d.kelembapan, 65, 80));
  }

  /* ── 4.2 pH Tanah ── */
  if (d.ph !== undefined) {
    const val = parseFloat(d.ph);
    _setText('s-ph-nilai', val.toFixed(1));

    // Posisi marker pH (skala 4.0–9.0 → 0–100%)
    const posisi = ((val - 4.0) / 5.0) * 100;
    _setGaya('s-ph-marker', 'left', `${Math.min(Math.max(posisi, 0), 98)}%`);

    const { label } = _statusPH(val);
    _setStatusChip('s-ph-status', label);
    _setText('s-ph-tren', val >= 6.0 && val <= 7.0 ? 'Stabil' : val < 6.0 ? 'Terlalu Asam' : 'Terlalu Basa');
  }

  /* ── 4.3 Suhu Lingkungan ── */
  if (d.suhu !== undefined) {
    const val = parseFloat(d.suhu);
    // Skala 15°C–45°C → 0–100%
    const pct = Math.min(Math.max(((val - 15) / 30) * 100, 0), 100);

    _setText('s-suhu-nilai', Math.round(val));
    _setBar('s-suhu-bar', pct);

    const { label } = _statusSuhu(val);
    _setStatusChip('s-suhu-status', label);
    _setText('s-suhu-tren', _trendLabel(val, 24, 32, '°C'));
  }

  /* ── 4.4 Intensitas Cahaya ── */
  if (d.cahaya !== undefined) {
    const val = parseFloat(d.cahaya);
    // Skala 0–100k Lux → 0–100%
    const pct = Math.min((val / 100000) * 100, 100);
    const tampil = val >= 1000 ? Math.round(val / 1000) : val;

    _setText('s-cahaya-nilai', tampil);
    _setText('s-cahaya-satuan-suffix', val >= 1000 ? 'k' : '');
    _setBar('s-cahaya-bar', pct);

    const { label } = _statusCahaya(val);
    _setStatusChip('s-cahaya-status', label);
    _setText('s-cahaya-tren', val > 60000 ? 'Di atas target' : val < 30000 ? 'Di bawah target' : 'Puncak siang');
  }

  /* ── 4.5 Sensor Sekunder (strip bawah) ── */
  if (d.kelembapan_udara !== undefined) {
    _setText('s-hum-udara', parseFloat(d.kelembapan_udara).toFixed(1) + '%');
  }
  if (d.curah_hujan !== undefined) {
    _setText('s-curah-hujan', parseFloat(d.curah_hujan).toFixed(1) + ' mm/h');
  }
  if (d.ec !== undefined) {
    _setText('s-ec', parseFloat(d.ec).toFixed(1) + ' dS/m');
  }

  /* ── 4.6 Perbarui timestamp live ── */
  const elWaktu = document.getElementById('live-time');
  if (elWaktu) {
    elWaktu.textContent = new Date().toLocaleTimeString('id-ID', { hour12: false });
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 5 — UPDATE DOM: PREDIKSI AI
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Memperbarui semua elemen DOM terkait hasil prediksi Coral Edge TPU.
 * @param {Object} d - Dokumen prediksi dari MongoDB/WebSocket
 */
function _updatePrediksi(d) {
  if (!d || typeof d !== 'object') return;

  /* ── 5.1 Estimasi Panen ── */
  if (d.estimasi_panen !== undefined) {
    _animateAILoading('ai-yield-nilai', parseFloat(d.estimasi_panen).toFixed(1));
  }
  if (d.yield_per_ha !== undefined) {
    _setText('ai-yield-per-ha', parseFloat(d.yield_per_ha).toFixed(1) + ' Ton/Ha');
  }

  /* ── 5.2 Status Lahan ── */
  if (d.status_lahan !== undefined) {
    _setText('ai-status-lahan', d.status_lahan);
  }
  if (d.status_lahan !== undefined) {
    const deskMap = {
      'Sangat Optimal': 'Semua parameter dalam batas ideal. Tidak ada intervensi diperlukan.',
      'Optimal'       : 'Parameter sebagian besar dalam batas. Monitor berkala dianjurkan.',
      'Perlu Perhatian': 'Beberapa parameter mendekati batas kritis. Tindakan preventif disarankan.',
      'Kritis'        : 'Parameter di luar batas aman. Intervensi segera diperlukan!',
    };
    _setText('ai-status-desc', deskMap[d.status_lahan] || '');
  }

  /* ── 5.3 Confidence Score ── */
  if (d.confidence !== undefined) {
    const conf = parseFloat(d.confidence);
    _setText('ai-confidence-nilai', conf.toFixed(1));
    _setBar('ai-confidence-bar', conf);
  }

  /* ── 5.4 Bobot Fitur (Feature Importance) ── */
  if (d.feature_weights && typeof d.feature_weights === 'object') {
    const fw = d.feature_weights;
    const petaId = {
      kelembapan : 'fi-kelembapan',
      ph         : 'fi-ph',
      suhu       : 'fi-suhu',
      cahaya     : 'fi-cahaya',
      ec         : 'fi-ec',
    };
    for (const [kunci, idEl] of Object.entries(petaId)) {
      if (fw[kunci] !== undefined) {
        _setText(idEl, fw[kunci] + '%');
      }
    }
  }

  /* ── 5.5 Tren Mingguan (Bar Chart) ── */
  if (Array.isArray(d.tren_mingguan) && d.tren_mingguan.length === 7) {
    const tren = d.tren_mingguan.map(Number);
    const maks = Math.max(...tren);
    const min  = Math.min(...tren);

    CHART_HARI.forEach((hari, i) => {
      const el = document.getElementById(`chart-bar-${hari}`);
      if (!el) return;

      // Hitung tinggi proporsional terhadap nilai maksimum
      const tinggiPx = maks > 0
        ? Math.round((tren[i] / maks) * CHART_TINGGI_MAKS)
        : 0;

      // Animasikan dengan CSS custom property
      el.style.setProperty('--h', `${tinggiPx}px`);
      el.style.height = '0px'; // Reset dulu
      el.style.animationPlayState = 'paused';

      // Reflow → trigger ulang animasi
      void el.offsetWidth;
      el.style.animationPlayState = 'running';

      // Hari terakhir (Minggu) beri warna penuh lime
      if (i === 6) {
        el.classList.add('bg-lime');
        el.classList.remove('bg-lime/40');
      } else {
        el.classList.add('bg-lime/40');
        el.classList.remove('bg-lime');
      }
    });

    _setText('ai-trend-min', min.toFixed(1) + 'T');
    _setText('ai-trend-maks', maks.toFixed(1) + 'T');
  }

  /* ── 5.6 Proyeksi Tanggal Panen ── */
  if (d.proyeksi_tanggal) {
    _setText('ai-proyeksi-tanggal', d.proyeksi_tanggal);
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 6 — UPDATE DOM: AKTUATOR
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Memperbarui tampilan tombol dan indikator aktuator (pompa/valve).
 * Dipanggil saat status aktuator berubah dari MQTT.
 * @param {Object} d - Dokumen log aktuator dari WebSocket
 */
function _updateAktuator(d) {
  if (!d || !d.aktuator || !d.aksi) return;

  const aktif       = d.aksi === 'aktif';
  const isPompa     = d.aktuator === 'pompa';

  if (isPompa) {
    _setTampilAktuator({
      btnId        : 'pump-toggle',
      statusId     : 'pump-status',
      indicatorId  : 'pump-indicator',
      boxId        : 'pump-box',
      aktif,
      pesanAktif   : 'Sistem Irigasi Berjalan',
      pesanNonAktif: 'Sistem Irigasi Standby',
    });

    /* Update metrik pompa jika ada */
    if (d.lpm !== undefined) {
      _setText('act-pump-lpm', parseFloat(d.lpm).toFixed(1));
      const pctFlow = Math.min((d.lpm / 4.0) * 100, 100);
      _setBar('act-pump-bar', pctFlow);
      _setText('act-pump-flow-label', `${d.lpm} / 4.0 L/min`);
    }
    if (d.psi !== undefined) _setText('act-pump-psi', d.psi);
    if (d.kw  !== undefined) _setText('act-pump-kw', parseFloat(d.kw).toFixed(1));

  } else {
    /* Solenoid Valve */
    _setTampilAktuator({
      btnId        : 'valve-toggle',
      statusId     : 'valve-status',
      indicatorId  : 'valve-indicator',
      boxId        : 'valve-box',
      aktif,
      pesanAktif   : 'Sistem Fertigasi Berjalan',
      pesanNonAktif: 'Sistem Fertigasi Standby',
    });

    if (d.lpm !== undefined) _setText('act-valve-lpm', parseFloat(d.lpm).toFixed(1));
    if (d.kw  !== undefined) _setText('act-valve-kw',  parseFloat(d.kw).toFixed(1));
  }
}

/**
 * Helper: mengubah tampilan satu aktuator (tombol + indikator + box).
 * @param {Object} opts - Opsi konfigurasi tampilan
 */
function _setTampilAktuator({ btnId, statusId, indicatorId, boxId, aktif, pesanAktif, pesanNonAktif }) {
  const btn       = document.getElementById(btnId);
  const status    = document.getElementById(statusId);
  const indicator = document.getElementById(indicatorId);
  const box       = document.getElementById(boxId);

  if (!btn) return;

  if (aktif) {
    btn.classList.replace('actuator-off', 'actuator-on');
    btn.textContent = 'AKTIF';
    if (indicator) {
      indicator.classList.remove('bg-muted');
      indicator.classList.add('bg-forest', 'pulse-active');
    }
    if (status) {
      status.classList.replace('text-muted', 'text-forest');
      status.textContent = pesanAktif;
    }
    if (box) {
      box.style.borderColor = '#1A3626';
      box.style.background  = '#D9F2C4';
    }
  } else {
    btn.classList.replace('actuator-on', 'actuator-off');
    btn.textContent = 'NON-AKTIF';
    if (indicator) {
      indicator.classList.remove('bg-forest', 'pulse-active');
      indicator.classList.add('bg-muted');
    }
    if (status) {
      status.classList.replace('text-forest', 'text-muted');
      status.textContent = pesanNonAktif;
    }
    if (box) {
      box.style.borderColor = '#1A362633';
      box.style.background  = '#ffffff';
    }
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 7 — UPDATE DOM: LOG SISTEM
   ════════════════════════════════════════════════════════════════════════════ */

/** Jumlah maksimum entri log yang ditampilkan */
const LOG_MAKS_ENTRI = 20;

/**
 * Menambahkan satu baris baru ke panel Log Sistem.
 * Baris baru muncul di atas, entri lama terdorong ke bawah.
 * Jika log melebihi LOG_MAKS_ENTRI, entri terlama dihapus.
 * @param {Object} d - {pesan: string, warna: string}
 */
function _tambahLog(d) {
  const kontainer = document.getElementById('system-log-list');
  if (!kontainer || !d || !d.pesan) return;

  // Tentukan warna garis kiri berdasarkan tipe log
  const petaWarna = {
    'forest'   : 'border-forest',
    'lime-deep': 'border-lime-deep',
    'muted'    : 'border-forest/30',
  };
  const kelasWarna = petaWarna[d.warna] || 'border-forest/30';

  // Buat elemen log baru
  const item = document.createElement('div');
  item.className = `border-l-2 ${kelasWarna} pl-2 opacity-0`;
  item.innerHTML = `<p class="text-[9px] font-inter text-muted">${_escape(d.pesan)}</p>`;

  // Sisipkan di bagian paling atas
  kontainer.insertBefore(item, kontainer.firstChild);

  // Animasikan kemunculan (fade-in)
  requestAnimationFrame(() => {
    item.style.transition = 'opacity 0.4s ease';
    item.style.opacity = '1';
  });

  // Hapus entri paling bawah jika melebihi batas
  const semua = kontainer.querySelectorAll('div');
  if (semua.length > LOG_MAKS_ENTRI) {
    kontainer.removeChild(semua[semua.length - 1]);
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 8 — UPDATE DOM: STATUS SISTEM (CPU, RAM, Uptime)
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Memperbarui elemen status sistem di panel Hero.
 * @param {Object} d - {cpu_load, memori, uptime_detik, latensi_ms}
 */
function _updateSistem(d) {
  if (!d) return;

  if (d.cpu_load !== undefined) {
    const cpu = Math.round(d.cpu_load);
    _setText('cpu-val', cpu + '%');
    _setBar('cpu-bar', cpu);
  }
  if (d.memori !== undefined) {
    const mem = Math.round(d.memori);
    _setText('mem-val', mem + '%');
    _setBar('mem-bar', mem);
  }
  if (d.uptime_detik !== undefined) {
    _setText('uptime-val', _formatUptime(d.uptime_detik));
  }
  if (d.latensi_ms !== undefined) {
    _setText('latency-val', d.latensi_ms + ' ms');
  }
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 9 — UPDATE DOM: STATUS KONEKSI WEBSOCKET
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Memperbarui indikator status WebSocket di navbar.
 * @param {'connected'|'disconnected'|'reconnecting'} status
 */
function _setStatusKoneksi(status) {
  const el = document.getElementById('ws-status-badge');
  if (!el) return;

  const petaStatus = {
    connected    : { teks: '⬤ WS Live',        kelas: 'ws-connected'    },
    disconnected : { teks: '⬤ WS Terputus',     kelas: 'ws-disconnected' },
    reconnecting : { teks: '⬤ WS Menghubungi…', kelas: 'ws-reconnecting' },
  };

  const { teks, kelas } = petaStatus[status] || petaStatus.disconnected;

  el.textContent = teks;
  el.className   = `text-[9px] font-grotesk tracking-widest uppercase hidden md:block ${kelas}`;
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 10 — LOGIKA STATUS SENSOR
   ════════════════════════════════════════════════════════════════════════════ */

/** Mengembalikan {label, kelas} berdasarkan nilai kelembapan */
function _statusKelembapan(val) {
  if (val >= 65 && val <= 80) return { label: 'Optimal',         kelas: 'bg-lime' };
  if (val >= 50 && val  < 65) return { label: 'Di Bawah Target', kelas: 'bg-yellow-100' };
  if (val  > 80 && val <= 90) return { label: 'Di Atas Target',  kelas: 'bg-yellow-100' };
  return { label: 'Kritis', kelas: 'bg-red-100' };
}

function _statusPH(val) {
  if (val >= 6.0 && val <= 7.0) return { label: 'Optimal' };
  if (val >= 5.5 && val  < 6.0) return { label: 'Sedikit Asam' };
  if (val  > 7.0 && val <= 7.5) return { label: 'Sedikit Basa' };
  return { label: 'Di Luar Batas' };
}

function _statusSuhu(val) {
  if (val >= 24 && val <= 32) return { label: 'Optimal' };
  if (val >= 20 && val  < 24) return { label: 'Sejuk' };
  if (val  > 32 && val <= 36) return { label: 'Hangat' };
  return { label: 'Ekstrem' };
}

function _statusCahaya(val) {
  if (val >= 30000 && val <= 60000) return { label: 'Optimal' };
  if (val < 30000)                  return { label: 'Redup' };
  return { label: 'Terlalu Terang' };
}

/**
 * Menghasilkan label tren dengan perubahan nilai (contoh: "▲ +2 dari kemarin").
 * Membandingkan nilai saat ini terhadap rentang target ideal.
 */
function _trendLabel(nilai, min, maks, satuan = '') {
  const n = parseFloat(nilai);
  if (n >= min && n <= maks) return `Optimal ${satuan}`.trim();
  if (n < min)               return `▼ Di bawah target ${satuan}`.trim();
  return `▲ Di atas target ${satuan}`.trim();
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 11 — FUNGSI HELPER DOM
   ════════════════════════════════════════════════════════════════════════════ */

/** Mengatur teks sebuah elemen berdasarkan ID */
function _setText(id, teks) {
  const el = document.getElementById(id);
  if (el) el.textContent = teks;
}

/** Mengatur teks dengan animasi count-up singkat (flash efek) */
function _setTextAnimasi(id, teks) {
  const el = document.getElementById(id);
  if (!el) return;

  el.style.transition = 'opacity 0.15s ease';
  el.style.opacity    = '0.3';
  setTimeout(() => {
    el.textContent  = teks;
    el.style.opacity = '1';
  }, 150);
}

/**
 * Menganimasikan angka acak (loading state) sebelum menampilkan hasil final prediksi AI.
 * Berjalan selama 1200ms sebelum mengunci ke nilai final.
 */
function _animateAILoading(id, finalTeks) {
  const el = document.getElementById(id);
  if (!el) return;

  // Hapus efek warna/opacity dari animasi sebelumnya
  el.style.transition = 'none';
  el.style.opacity = '0.5';
  
  let iterasi = 0;
  const maxIterasi = 20; // 20 kali ganti angka
  const intervalMs = 60; // 60ms per pergantian (~1.2 detik total)

  // Animasi acak angka
  const interval = setInterval(() => {
    iterasi++;
    // Hasil acak format 00.0
    const acak = (Math.random() * 99).toFixed(1);
    el.textContent = acak;

    if (iterasi >= maxIterasi) {
      clearInterval(interval);
      // Tampilkan hasil final dengan transisi warna
      el.textContent = finalTeks;
      el.style.transition = 'opacity 0.3s ease';
      el.style.opacity = '1';
      
      // Jika tersedia Toast global, kita bisa beri notifikasi 
      // (tapi lebih baik tidak dipanggil per elemen, jadi skip di sini)
    }
  }, intervalMs);
}

/**
 * Mengatur lebar/tinggi progress bar berdasarkan persentase.
 * Bar menggunakan CSS custom property --fill atau inline style width.
 * @param {string} id  - ID elemen bar
 * @param {number} pct - Persentase (0–100)
 */
function _setBar(id, pct) {
  const el = document.getElementById(id);
  if (!el) return;

  const nilai = Math.min(Math.max(pct, 0), 100);

  // Untuk elemen dengan animasi bar-fill (menggunakan --fill CSS var)
  if (el.style.getPropertyValue('--fill') !== undefined) {
    el.style.setProperty('--fill', `${nilai}%`);
  }
  // Selalu set width juga sebagai fallback
  el.style.width = `${nilai}%`;
}

/** Mengatur satu properti CSS (style) elemen */
function _setGaya(id, properti, nilai) {
  const el = document.getElementById(id);
  if (el) el.style[properti] = nilai;
}

/**
 * Memperbarui chip status sensor (Optimal / Warning / Kritis).
 * @param {string} id    - ID elemen <span> chip
 * @param {string} label - Teks label status
 */
function _setStatusChip(id, label) {
  const el = document.getElementById(id);
  if (!el) return;

  el.textContent = label;

  // Hapus kelas status lama
  el.classList.remove('text-forest', 'text-yellow-700', 'text-red-700');

  // Terapkan warna sesuai status
  if (label === 'Optimal')           el.classList.add('text-forest');
  else if (label === 'Kritis' ||
           label === 'Di Luar Batas' ||
           label === 'Ekstrem')      el.classList.add('text-red-700');
  else                               el.classList.add('text-yellow-700');
}

/** Mengkonversi detik ke format "X j Y m" */
function _formatUptime(detik) {
  const jam   = Math.floor(detik / 3600);
  const menit = Math.floor((detik % 3600) / 60);
  return `${jam} j ${menit} m`;
}

/** Escape karakter HTML untuk mencegah XSS pada log */
function _escape(teks) {
  return String(teks)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 12 — DIGITAL TWIN PLACEHOLDER ANIMASI
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Menambahkan animasi floating/pulse pada Digital Twin Placeholder
 * agar terlihat "hidup" sambil menunggu build WebGL Unity.
 */
function _animasiDigitalTwin() {
  const placeholder = document.getElementById('digital-twin-placeholder');
  if (!placeholder) return;

  // Ambil elemen cincin di dalam placeholder
  const ring = placeholder.querySelector('.w-16');
  if (!ring) return;

  let sudut = 0;

  setInterval(() => {
    sudut = (sudut + 1) % 360;

    // Rotasi lambat cincin luar
    ring.style.transform = `rotate(${sudut}deg)`;
    ring.style.transition = 'transform 0.05s linear';

    // Pulse opacity cincin dalam tiap 90 derajat
    const opacity = 0.3 + 0.7 * Math.abs(Math.sin((sudut * Math.PI) / 180));
    const innerBox = ring.querySelector('.w-4');
    if (innerBox) {
      innerBox.style.opacity = opacity;
    }
  }, 50); // ~20fps untuk hemat CPU

  // Pulse outline secara periodik
  setInterval(() => {
    placeholder.style.transition = 'outline-color 1s ease';
    placeholder.style.outlineColor = '#D9F2C480';
    setTimeout(() => {
      placeholder.style.outlineColor = '#D9F2C420';
    }, 1000);
  }, 2000);
}

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 13 — INISIALISASI
   ════════════════════════════════════════════════════════════════════════════ */

/**
 * Titik masuk utama — dijalankan saat DOM selesai dimuat.
 * Menginisialisasi WebSocket dan animasi Digital Twin.
 */
document.addEventListener('DOMContentLoaded', () => {
  console.log('[PANDU-WS] Inisialisasi...');

  // Mulai koneksi WebSocket
  panduWsConnect();

  // Aktifkan animasi Digital Twin Placeholder
  _animasiDigitalTwin();

  // Inisialisasi Modal Konfigurasi Lahan
  _initModalKonfigurasi();

  console.log('[PANDU-WS] Siap. Menunggu data real-time...');
});

/* ════════════════════════════════════════════════════════════════════════════
   BAGIAN 14 — MODAL KONFIGURASI LAHAN
   ════════════════════════════════════════════════════════════════════════════ */

function _initModalKonfigurasi() {
  const modal = document.getElementById('modal-konfigurasi');
  const btnBuka = document.getElementById('btn-buka-konfigurasi');
  const btnTutup = document.getElementById('btn-tutup-konfigurasi');
  const btnBatal = document.getElementById('btn-batal-konfigurasi');
  const overlay = document.getElementById('modal-overlay');
  const form = document.getElementById('form-konfigurasi');
  const feedback = document.getElementById('konfigurasi-feedback');

  if (!modal || !btnBuka || !form) return;

  // Fungsi utilitas buka/tutup
  const bukaModal = () => {
    modal.classList.remove('hidden');
    feedback.classList.add('hidden'); // Sembunyikan feedback lama
  };
  const tutupModal = () => modal.classList.add('hidden');

  // Event Listeners buka/tutup
  btnBuka.addEventListener('click', bukaModal);
  btnTutup.addEventListener('click', tutupModal);
  btnBatal.addEventListener('click', tutupModal);
  overlay.addEventListener('click', tutupModal);

  // Event Listener submit form
  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    // Tampilkan state loading
    const btnSimpan = document.getElementById('btn-simpan-konfigurasi');
    const teksAsli = btnSimpan.textContent;
    btnSimpan.textContent = 'Menyimpan...';
    btnSimpan.disabled = true;

    // Ambil data form
    const formData = new FormData(form);
    const dataJSON = Object.fromEntries(formData.entries());

    try {
      // Kirim POST request ke endpoint Django menggunakan Fetch API
      const response = await fetch('/api/konfigurasi/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // Note: Jika CSRF error, ambil token dari cookies
        },
        body: JSON.stringify(dataJSON)
      });

      const result = await response.json();

      if (result.status === 'ok') {
        // Tampilkan feedback sukses
        feedback.textContent = 'Berhasil! Data lahan dan kalkulasi panen diperbarui.';
        feedback.className = 'block text-[10px] font-inter text-forest mt-2 p-2 border border-lime bg-lime';

        // Animasikan update ke DOM Dashboard
        _setTextAnimasi('ai-luas-lahan', result.data.luas_hektar);
        _setTextAnimasi('ai-yield-per-ha', result.data.yield_per_ha);
        _animateAILoading('ai-yield-nilai', result.data.estimasi_panen.toFixed(1));
        _setTextAnimasi('ai-proyeksi-tanggal', result.data.proyeksi_tanggal);

        // Tutup modal otomatis setelah 1.5 detik
        setTimeout(() => {
          tutupModal();
          form.reset(); // Kosongkan form
        }, 1500);
      } else {
        throw new Error(result.pesan || 'Terjadi kesalahan');
      }
    } catch (error) {
      // Tampilkan pesan error di form
      feedback.textContent = 'Gagal menyimpan: ' + error.message;
      feedback.className = 'block text-[10px] font-inter text-red-700 mt-2 p-2 border border-red-300 bg-red-100';
    } finally {
      // Kembalikan tombol ke semula
      btnSimpan.textContent = teksAsli;
      btnSimpan.disabled = false;
    }
  });
}
