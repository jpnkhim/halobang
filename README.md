# NovaEX AI Telegram Bot

Bot Telegram untuk auto-register akun NovaEX AI (mail.tm + Google Authenticator + multi-thread + rotating proxies). Siap deploy ke **Koyeb Free Web Service**.

## Fitur

- 🤖 Tombol menu inline (Bahasa Indonesia)
- ⚙️ Pengaturan lengkap via tombol: jumlah akun, threads, invite mode (random/fixed), invite code, skip GA, retries, max proxy swaps
- 📤 Upload `proxies.txt` (auto-rename apapun nama filenya)
- 🚀 Live progress + tombol **⛔ Cancel** — jika ditekan, akun yang sudah berhasil otomatis disimpan ke CSV dan dikirim
- 📥 Tombol Download CSV kapanpun
- 📊 Tombol Total Akun
- 💾 Penyimpanan in-memory (cocok untuk filesystem ephemeral Koyeb)
- 🌐 Long-polling mode (sederhana, tanpa setup webhook)

## Struktur

```
telegram_bot/
├── bot.py            # Telegram handlers (menu, callback, document, text)
├── main.py           # FastAPI + PTB Application (webhook / polling)
├── novaku_core.py    # Refactored core: cancel_event + on_account callback
├── requirements.txt
├── Dockerfile
├── Procfile
├── .env.example
└── README.md
```

## Menjalankan Lokal

```bash
cd telegram_bot
cp .env.example .env
# isi BOT_TOKEN
pip install -r requirements.txt
python main.py
```

Bot akan jalan dengan long-polling. Kirim `/start` ke bot Anda.

## Deploy ke Koyeb (Free Web Service)

Koyeb free tier butuh service yang listen di `$PORT`. Bot ini menjalankan FastAPI (untuk `/health`) berdampingan dengan long-polling Telegram, jadi memenuhi syarat tanpa perlu setup webhook.

### Langkah-langkah

1. **Buat Bot di Telegram**
   - Chat `@BotFather` → `/newbot` → simpan token.

2. **Push Folder Ini ke GitHub**
   ```bash
   cd telegram_bot
   git init
   git add .
   git commit -m "telegram bot"
   git branch -M main
   git remote add origin git@github.com:<username>/<repo>.git
   git push -u origin main
   ```

   *Pastikan `.env` masuk `.gitignore` dan **tidak** di-push.*

3. **Buat Service di Koyeb**
   - Login ke https://app.koyeb.com → **Create Service** → **GitHub**
   - Pilih repo Anda → branch `main`
   - **Builder**: pilih **Dockerfile** (otomatis terdeteksi)
   - **Instance**: pilih **Free** (Nano)
   - **Region**: pilih yang terdekat (mis. Frankfurt / Singapore)
   - **Ports**: `8000` (HTTP)
   - **Environment Variables**:
     | Key | Value |
     |---|---|
     | `BOT_TOKEN` | token dari BotFather |
     | `PORT` | `8000` |
   - **Health check**: `GET /health` (port 8000)
   - Klik **Deploy**.

4. **Tes**
   - Buka chat bot → kirim `/start`. Tombol menu akan muncul.

> Catatan: **hanya jalankan satu instance bot dengan token yang sama**. Jika
> Anda menjalankan bot di lokal sambil instance Koyeb juga aktif, Telegram
> akan mengembalikan `Conflict 409` karena dua poller bersaing untuk update
> yang sama.

## Catatan

- **Filesystem ephemeral**: file CSV & proxies akan hilang saat Koyeb me-redeploy/sleep instance. Karena itu CSV disimpan di memori dan langsung dikirim ke Telegram saat selesai/cancel.
- **Free tier sleep**: Koyeb free instance bisa idle-sleep. Webhook tetap diterima tapi update pertama setelah sleep mungkin sedikit lambat.
- **Concurrent users**: state per-user dipisah via `context.user_data`. Aman untuk multi user, tapi karena in-memory, restart = kehilangan data.

## Lisensi

Sama dengan source `novaku.py` asli.
