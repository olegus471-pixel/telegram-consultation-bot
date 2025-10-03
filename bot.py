import os
import json
import base64
from oauth2client.service_account import ServiceAccountCredentials
import gspread

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =======================
# Переменные окружения
# =======================
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
PORT = int(os.environ.get("PORT", 10000))

# =======================
# Подключение к Google Sheets
# =======================
creds_json = base64.b64decode(os.environ["GOOGLE_CREDS"])
creds_dict = json.loads(creds_json)

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Убедитесь, что в Google Sheets есть таблица "Расписание" и лист "График"
sheet = client.open("Расписание").worksheet("График")

# =======================
# Главное меню
# =======================
main_menu = [["📅 Записаться на консультацию", "ℹ️ Инфо"]]

# =======================
# Хэндлеры
# =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для записи на консультацию.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "📅 Записаться на консультацию":
        await update.message.reply_text("Введите ваше имя:")
        context.user_data["step"] = "name"
        return

    if context.user_data.get("step") == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "choose_slot"

        all_slots = sheet.get_all_values()[1:]
        free_slots = [row[0].strip() for row in all_slots if row[1].strip() == ""]

        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.")
            context.user_data.clear()
            return

        slot_buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup(slot_buttons, resize_keyboard=True)
        )
        return

    if context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text

        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.")
            return

        if sheet.cell(cell.row, 2).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте снова.")
            return

        sheet.update_cell(cell.row, 2, name)
        sheet.update_cell(cell.row, 3, "Консультация")

        await update.message.reply_text(
            f"✅ Запись принята!\nИмя: {name}\nУслуга: Консультация\nКогда: {slot}",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        await context.bot.send_message(
            ADMIN_ID,
            f"📌 Новая запись:\nИмя: {name}\nУслуга: Консультация\nКогда: {slot}"
        )

        context.user_data.clear()
        return

    if text == "ℹ️ Инфо":
        await update.message.reply_text("ℹ️ Консультации проходят онлайн. Длительность: 1 час.")
        return

    await update.message.reply_text("Не понял 🤔. Попробуйте снова.")

# =======================
# Запуск бота через Webhook
# =======================
if __name__ == "__main__":
    WEBHOOK_URL = "https://telegram-consultation-bot.onrender.com/webhook"

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен на Webhook!")

    # Установка webhook для Telegram
    import asyncio
    async def set_webhook():
        await app.bot.set_webhook(WEBHOOK_URL)

    asyncio.run(set_webhook())

    # Запуск webhook сервера
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_path="/webhook",
        webhook_url=WEBHOOK_URL
    )
