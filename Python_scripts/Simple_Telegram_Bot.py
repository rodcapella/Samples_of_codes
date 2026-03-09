# Um bot que responde a comandos básicos, como /start, /help e /echo, usando Python e a biblioteca python-telegram-bot.
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import os

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")  # Coloque seu token como variável de ambiente

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Olá! Eu sou um bot de exemplo.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("/start - Inicia o bot\n/help - Lista comandos\n/echo <mensagem> - Repete a mensagem")

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = ' '.join(context.args)
    if text:
        await update.message.reply_text(text)
    else:
        await update.message.reply_text("Use: /echo <mensagem>")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("echo", echo))

    print("Bot iniciado...")
    app.run_polling()
