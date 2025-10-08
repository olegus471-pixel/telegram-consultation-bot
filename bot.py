# -*- coding: utf-8 -*-
import os
import json
import base64
import asyncio
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import re
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ============= НАСТРОЙКИ =============
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://telegram-consultation-bot.onrender.com/webhook")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=6)

# ============= Google Sheets =============
sheets_creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDS"])
sheets_creds_dict = json.loads(sheets_creds_json)
sheets_scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(sheets_creds_dict, sheets_scope)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# ============= Google Calendar (Meet) =============
calendar_creds_json = base64.b64decode(os.environ["GOOGLE_CALENDAR_CREDS"])
calendar_creds_dict = json.loads(calendar_creds_json)
calendar_scopes = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.events"
]
calendar_credentials = Credentials.from_service_account_info(
    calendar_creds_dict,
    scopes=calendar_scopes,
    subject="ops@migrall.com"
)
calendar_service = build("calendar", "v3", credentials=calendar_credentials)
CALENDAR_ID = "ops@migrall.com"

# ============= МЕНЮ =============
main_menu = [
    ["📅 Записаться", "📖 Моя запись"],
    ["🔁 Перенос", "❌ Отмена"],
    ["📎 Получить ссылку", "ℹ️ Инфо"]
]

# ============= УТИЛИТЫ =============
async def run_in_thread(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

def parse_slot_datetime(slot_text: str):
    try:
        return datetime.datetime.strptime(slot_text, "%d.%m.%Y, %H:%M")
    except Exception:
        return None

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def find_user_booking_sync(user_id: int):
    all_rows = sheet.get_all_values()
    now = datetime.datetime.now()
    for i, row in enumerate(all_rows[1:], start=2):
        if len(row) >= 6:
            uid = row[5].strip()
            slot_text = row[1].strip()
            if uid == str(user_id):
                slot_dt = parse_slot_datetime(slot_text)
                if slot_dt and slot_dt > now:
                    return i, row, slot_text
    return None, None, None

async def find_user_booking(user_id: int):
    return await run_in_thread(find_user_booking_sync, user_id)

# ============= /start =============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

# ============= ОБРАБОТКА СООБЩЕНИЙ =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user = update.message.from_user
    user_id = user.id

    # Отладка
    print(f"[DEBUG] Получено сообщение от {user_id}: '{text}'")

    if not text:
        await update.message.reply_text("⚠️ Сообщение пустое.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # Нормализуем текст
    text_lower = text.lower()

    # === Команды ===
    if text_lower in ("/start", "старт"):
        await start(update, context)
        return

    elif "запис" in text_lower:
        # 📅 Записаться
        await update.message.reply_text("✏️ Вы выбрали запись на консультацию. Введите ваше имя и фамилию:",
                                        reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
        context.user_data["step"] = "ask_name"
        return

    elif "моя запись" in text_lower:
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[10] if len(row) > 10 else ""
            msg = f"📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}"
            if meet_link:
                msg += f"\n🔗 Ссылка: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        else:
            await update.message.reply_text("ℹ️ У вас нет активных записей.",
                                            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    elif "отмена" in text_lower:
        await update.message.reply_text("❌ Действие отменено.",
                                        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        context.user_data.clear()
        return

    elif "инфо" in text_lower:
        await update.message.reply_text(
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\nДлительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    elif "ссылка" in text_lower:
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ У вас нет активной записи.",
                                            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        else:
            meet_link = row[10] if len(row) > 10 else ""
            if meet_link:
                await update.message.reply_text(f"🔗 Ваша ссылка: {meet_link}",
                                                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            else:
                await update.message.reply_text("🔗 Ссылка пока не создана. Она появится после подтверждения.",
                                                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    elif "перенос" in text_lower:
        await update.message.reply_text("🔁 Функция переноса пока не активна.",
                                        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Обработка этапов ===
    if context.user_data.get("step") == "ask_name":
        full_name = text
        context.user_data["full_name"] = full_name
        await update.message.reply_text(f"✅ Имя получено: {full_name}\n(дальше идёт логика выбора времени...)",
                                        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        context.user_data.clear()
        return

    # === Неизвестная команда ===
    await update.message.reply_text("Не понял команду — попробуйте ещё раз.",
                                    reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("🚀 Бот запущен (webhook)")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
