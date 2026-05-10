"""
Telegram bot for NovaEX AI auto-register.

Features
--------
- /start menu with inline buttons (Indonesian language)
- Settings menu via buttons (count, threads, invite, invite-mode, GA toggle,
  retries, max-proxy-swaps, reset)
- Upload proxies.txt -> auto-renamed and stored per user
- Start registration with live progress message + Cancel button
- Cancel saves accumulated accounts to CSV automatically
- Download CSV button
- Total accounts counter
- All in-memory (Koyeb free tier ephemeral filesystem friendly)
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import threading
import time

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from novaku_core import (
    DEFAULT_SETTINGS,
    MAX_THREADS,
    rows_to_csv_bytes,
    run_registration,
)


log = logging.getLogger("bot")

PROXY_DIR = "/tmp/nova_bot_proxies"
os.makedirs(PROXY_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Per-user state
# ---------------------------------------------------------------------------
def _get_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Per-user state stored in context.user_data."""
    ud = context.user_data
    if "settings" not in ud:
        ud["settings"] = dict(DEFAULT_SETTINGS)
    if "accounts" not in ud:
        ud["accounts"] = []        # list of row dicts
    if "is_running" not in ud:
        ud["is_running"] = False
    if "cancel_event" not in ud:
        ud["cancel_event"] = None
    if "proxy_path" not in ud:
        ud["proxy_path"] = None
    if "awaiting" not in ud:
        ud["awaiting"] = None      # name of the setting awaiting a text reply
    if "progress_message_id" not in ud:
        ud["progress_message_id"] = None
    return ud


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Mulai Registrasi", callback_data="start_reg")],
        [InlineKeyboardButton("⚙️ Pengaturan", callback_data="settings")],
        [
            InlineKeyboardButton("📊 Total Akun", callback_data="total"),
            InlineKeyboardButton("📥 Download CSV", callback_data="download"),
        ],
        [InlineKeyboardButton("📤 Upload Proxies", callback_data="upload_proxies")],
        [InlineKeyboardButton("❓ Bantuan", callback_data="help")],
    ])


def settings_kb(settings: dict) -> InlineKeyboardMarkup:
    ga_label = "Skip GA: ✅ YA" if settings["no_ga"] else "Skip GA: ❌ TIDAK"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Jumlah Akun: {settings['count']}", callback_data="set_count")],
        [InlineKeyboardButton(f"Threads: {settings['threads']}/{MAX_THREADS}", callback_data="set_threads")],
        [InlineKeyboardButton(f"Invite Mode: {settings['invite_mode']}", callback_data="toggle_invite_mode")],
        [InlineKeyboardButton(f"Invite Code: {settings['invite']}", callback_data="set_invite")],
        [InlineKeyboardButton(ga_label, callback_data="toggle_ga")],
        [InlineKeyboardButton(f"Retries: {settings['retries']}", callback_data="set_retries")],
        [InlineKeyboardButton(f"Max Proxy Swaps: {settings['max_proxy_swaps']}", callback_data="set_swaps")],
        [InlineKeyboardButton("♻️ Reset Default", callback_data="reset_settings")],
        [InlineKeyboardButton("⬅️ Kembali", callback_data="main_menu")],
    ])


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data="cancel_run")]])


def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Kembali", callback_data="main_menu")]])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
WELCOME_TEXT = (
    "🤖 *NovaEX AI Auto Register Bot*\n\n"
    "Bot untuk auto-register akun NovaEX AI lengkap dengan email mail.tm + "
    "Google Authenticator + proxy rotating + multi-thread.\n\n"
    "Gunakan tombol di bawah untuk mengontrol bot."
)

HELP_TEXT = (
    "❓ *Cara Pakai*\n\n"
    "1. Tap *📤 Upload Proxies* lalu kirim file `proxies.txt` "
    "(format `user:pass@host:port` atau `host:port`, satu per baris).\n"
    "2. Tap *⚙️ Pengaturan* untuk atur jumlah akun, threads, invite code, GA, dll.\n"
    "3. Tap *🚀 Mulai Registrasi* untuk menjalankan.\n"
    "4. Saat berjalan, tombol *⛔ Cancel* akan muncul. Jika ditekan, akun yang "
    "sudah berhasil sampai saat itu akan otomatis disimpan ke CSV dan dikirim.\n"
    "5. Tap *📥 Download CSV* untuk mengunduh akun yang sudah dikumpulkan.\n"
    "6. Tap *📊 Total Akun* untuk melihat jumlah akun terkumpul.\n\n"
    "_File akun disimpan di memori. Akan hilang jika bot di-restart._"
)


async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.message.reply_text(
            WELCOME_TEXT, reply_markup=main_menu_kb(), parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            WELCOME_TEXT, reply_markup=main_menu_kb(), parse_mode="Markdown"
        )


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _get_state(context)
    await send_main_menu(update, context)


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _get_state(context)
    await send_main_menu(update, context)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Callback query router
# ---------------------------------------------------------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    state = _get_state(context)
    data = query.data

    if data == "main_menu":
        await query.message.reply_text(WELCOME_TEXT, reply_markup=main_menu_kb(), parse_mode="Markdown")
        return

    if data == "help":
        await query.message.reply_text(HELP_TEXT, parse_mode="Markdown", reply_markup=back_kb())
        return

    if data == "total":
        n = len(state["accounts"])
        await query.message.reply_text(
            f"📊 *Total akun terkumpul:* `{n}`",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    if data == "download":
        rows = state["accounts"]
        if not rows:
            await query.message.reply_text(
                "Belum ada akun terkumpul. Jalankan registrasi dulu.",
                reply_markup=back_kb(),
            )
            return
        csv_bytes = rows_to_csv_bytes(rows)
        bio = io.BytesIO(csv_bytes)
        bio.name = f"accounts_{int(time.time())}.csv"
        await query.message.reply_document(
            document=InputFile(bio, filename=bio.name),
            caption=f"📥 {len(rows)} akun",
        )
        return

    if data == "upload_proxies":
        state["awaiting"] = "proxies_upload"
        await query.message.reply_text(
            "📤 Kirim file `proxies.txt` Anda sekarang sebagai *Document*.\n"
            "Format: `user:pass@host:port` atau `host:port` (satu per baris).\n"
            "Nama file apa pun akan otomatis di-rename.",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    if data == "settings":
        await query.message.reply_text(
            "⚙️ *Pengaturan*\nTap tombol untuk mengubah nilai.",
            reply_markup=settings_kb(state["settings"]),
            parse_mode="Markdown",
        )
        return

    if data == "toggle_invite_mode":
        s = state["settings"]
        s["invite_mode"] = "fixed" if s["invite_mode"] == "random" else "random"
        await query.edit_message_reply_markup(reply_markup=settings_kb(s))
        return

    if data == "toggle_ga":
        s = state["settings"]
        s["no_ga"] = not s["no_ga"]
        await query.edit_message_reply_markup(reply_markup=settings_kb(s))
        return

    if data == "reset_settings":
        state["settings"] = dict(DEFAULT_SETTINGS)
        await query.edit_message_reply_markup(reply_markup=settings_kb(state["settings"]))
        await query.message.reply_text("♻️ Pengaturan di-reset ke default.")
        return

    # Set-prompts
    prompts = {
        "set_count":   ("count",           "Kirim jumlah akun yang ingin dibuat (angka >= 1):"),
        "set_threads": ("threads",         f"Kirim jumlah threads (1-{MAX_THREADS}):"),
        "set_invite":  ("invite",          "Kirim invite code (string):"),
        "set_retries": ("retries",         "Kirim jumlah retries per akun (1-10):"),
        "set_swaps":   ("max_proxy_swaps", "Kirim max proxy swaps per akun (0-20):"),
    }
    if data in prompts:
        key, msg = prompts[data]
        state["awaiting"] = key
        await query.message.reply_text(msg, reply_markup=back_kb())
        return

    if data == "start_reg":
        await start_registration(update, context)
        return

    if data == "cancel_run":
        ev: threading.Event | None = state.get("cancel_event")
        if ev and not ev.is_set():
            ev.set()
            try:
                await query.edit_message_text(
                    "⛔ *Cancel diminta* — worker akan berhenti setelah akun "
                    "yang sedang berjalan selesai. CSV akan dikirim otomatis.",
                    parse_mode="Markdown",
                )
            except Exception:
                await query.message.reply_text(
                    "⛔ Cancel diminta — menunggu worker selesai..."
                )
        else:
            await query.message.reply_text("Tidak ada proses yang sedang berjalan.")
        return


# ---------------------------------------------------------------------------
# Free-text handler (settings input + ignore)
# ---------------------------------------------------------------------------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = _get_state(context)
    awaiting = state.get("awaiting")
    if not awaiting:
        await update.message.reply_text(
            "Gunakan /menu untuk membuka menu.",
            reply_markup=main_menu_kb(),
        )
        return

    text = (update.message.text or "").strip()
    s = state["settings"]

    def _ack():
        return update.message.reply_text(
            "✅ Tersimpan.",
            reply_markup=settings_kb(s),
        )

    try:
        if awaiting == "count":
            v = max(1, int(text))
            s["count"] = v
        elif awaiting == "threads":
            v = max(1, min(MAX_THREADS, int(text)))
            s["threads"] = v
        elif awaiting == "invite":
            if not text:
                raise ValueError("kosong")
            s["invite"] = text
        elif awaiting == "retries":
            v = max(1, min(10, int(text)))
            s["retries"] = v
        elif awaiting == "max_proxy_swaps":
            v = max(0, min(20, int(text)))
            s["max_proxy_swaps"] = v
        else:
            await update.message.reply_text("Input tidak dikenal.")
            state["awaiting"] = None
            return
    except ValueError:
        await update.message.reply_text("⚠️ Nilai tidak valid. Coba lagi.")
        return

    state["awaiting"] = None
    await _ack()


# ---------------------------------------------------------------------------
# Document upload (proxies.txt)
# ---------------------------------------------------------------------------
async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = _get_state(context)
    if state.get("awaiting") != "proxies_upload":
        # Allow upload anytime if user is not in a different awaiting state
        if state.get("awaiting"):
            await update.message.reply_text(
                "Anda sedang mengisi pengaturan lain. Selesaikan dulu atau /menu untuk reset.",
            )
            return

    doc = update.message.document
    if not doc:
        return

    user_id = update.effective_user.id
    dest_path = os.path.join(PROXY_DIR, f"proxies_{user_id}.txt")

    file = await doc.get_file()
    await file.download_to_drive(dest_path)

    # Validate (count non-empty / non-comment lines)
    try:
        with open(dest_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln for ln in (line.strip() for line in f) if ln and not ln.startswith("#")]
    except Exception as e:
        await update.message.reply_text(f"⚠️ Gagal membaca file: {e}")
        return

    if not lines:
        await update.message.reply_text("⚠️ File proxy kosong. Upload ulang dengan baris berisi proxy.")
        return

    state["proxy_path"] = dest_path
    state["awaiting"] = None
    await update.message.reply_text(
        f"✅ Proxies tersimpan ({len(lines)} baris). File otomatis di-rename ke `proxies_{user_id}.txt`.",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(),
    )


# ---------------------------------------------------------------------------
# Registration runner
# ---------------------------------------------------------------------------
async def start_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = _get_state(context)
    chat_id = update.effective_chat.id

    if state["is_running"]:
        await context.bot.send_message(chat_id, "⏳ Sudah ada proses yang berjalan. Tekan Cancel dulu.")
        return

    if not state["proxy_path"] or not os.path.exists(state["proxy_path"]):
        await context.bot.send_message(
            chat_id,
            "⚠️ Belum ada file proxies. Tap *📤 Upload Proxies* dulu.",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(),
        )
        return

    s = state["settings"]
    cancel_event = threading.Event()
    state["cancel_event"] = cancel_event
    state["is_running"] = True

    msg = await context.bot.send_message(
        chat_id,
        f"🚀 *Mulai registrasi*\n"
        f"Target: `{s['count']}`  Threads: `{s['threads']}`\n"
        f"Invite: `{s['invite']}` ({s['invite_mode']})  GA: "
        f"`{'skip' if s['no_ga'] else 'on'}`\n\n"
        f"Progress: 0/{s['count']}\n\n"
        f"_Anda tetap bisa pakai menu lain selama proses berjalan._",
        parse_mode="Markdown",
        reply_markup=cancel_kb(),
    )
    state["progress_message_id"] = msg.message_id

    settings_snapshot = dict(s)
    proxy_path = state["proxy_path"]
    pre_invites = [r.get("user_code", "") for r in state["accounts"] if r.get("user_code")]

    # Fire-and-forget so this handler returns immediately and the bot stays
    # responsive to all other buttons (cancel, settings, download, ...).
    asyncio.create_task(
        _run_registration_task(
            context=context,
            chat_id=chat_id,
            state=state,
            settings_snapshot=settings_snapshot,
            proxy_path=proxy_path,
            pre_invites=pre_invites,
            cancel_event=cancel_event,
        )
    )


async def _run_registration_task(
    *, context, chat_id, state, settings_snapshot, proxy_path, pre_invites, cancel_event,
):
    """Background task: runs registration in executor, edits progress, sends CSV."""
    loop = asyncio.get_running_loop()
    accounts_lock = threading.Lock()
    started_count = len(state["accounts"])

    def on_account(row: dict):
        with accounts_lock:
            state["accounts"].append(row)

    fut = loop.run_in_executor(
        None,
        lambda: run_registration(
            settings=settings_snapshot,
            proxy_file=proxy_path,
            on_account=on_account,
            cancel_event=cancel_event,
            pre_invite_codes=pre_invites,
        ),
    )

    last_text = None
    while not fut.done():
        await asyncio.sleep(2.5)
        with accounts_lock:
            done = len(state["accounts"]) - started_count
        text = (
            f"🚀 *Registrasi berjalan*\n"
            f"Target: `{settings_snapshot['count']}`  "
            f"Threads: `{settings_snapshot['threads']}`\n"
            f"Berhasil sesi ini: `{done}/{settings_snapshot['count']}`\n"
            f"Total akun di memori: `{len(state['accounts'])}`\n\n"
            f"_Anda tetap bisa pakai menu lain selama proses berjalan._"
        )
        if cancel_event.is_set():
            text += "\n\n⛔ _Cancel diminta — menunggu worker selesai..._"
        if text != last_text:
            try:
                await context.bot.edit_message_text(
                    text,
                    chat_id=chat_id,
                    message_id=state["progress_message_id"],
                    parse_mode="Markdown",
                    reply_markup=cancel_kb() if not cancel_event.is_set() else None,
                )
                last_text = text
            except Exception:
                pass

    result = await fut

    state["is_running"] = False
    state["cancel_event"] = None

    cancelled = result.get("error") == "cancelled"
    err = result.get("error") if not cancelled else None
    success_run = result.get("success", 0)
    total_run = result.get("total", 0)

    summary = (
        f"{'⛔ *Dibatalkan*' if cancelled else '✅ *Selesai*'}\n"
        f"Berhasil sesi ini: `{success_run}/{total_run}`\n"
        f"Total akun di memori: `{len(state['accounts'])}`"
    )
    if err:
        summary += f"\n⚠️ Error: `{err}`"
    try:
        await context.bot.edit_message_text(
            summary,
            chat_id=chat_id,
            message_id=state["progress_message_id"],
            parse_mode="Markdown",
        )
    except Exception:
        await context.bot.send_message(chat_id, summary, parse_mode="Markdown")

    if state["accounts"]:
        csv_bytes = rows_to_csv_bytes(state["accounts"])
        bio = io.BytesIO(csv_bytes)
        bio.name = f"accounts_{int(time.time())}.csv"
        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(bio, filename=bio.name),
                caption=("⛔ Auto-save (cancel): " if cancelled else "📥 ")
                        + f"{len(state['accounts'])} akun",
                reply_markup=main_menu_kb(),
            )
        except Exception as e:
            await context.bot.send_message(chat_id, f"Gagal mengirim CSV: {e}")
    else:
        await context.bot.send_message(
            chat_id, "Tidak ada akun yang berhasil dibuat.", reply_markup=main_menu_kb()
        )


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------
def build_application(token: str) -> Application:
    application = (
        Application.builder()
        .token(token)
        .concurrent_updates(True)
        .build()
    )
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, on_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    return application
  
