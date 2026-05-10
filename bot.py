import os
from telegram.ext import Application, CommandHandler

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

async def start(update, context):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Unauthorized")
        return

    await update.message.reply_text(
        "✅ NovaEX Telegram Bot Running on Koyeb"
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    print("Bot running...")
    app.run_polling()

if __name__ == "__main__":
    main()