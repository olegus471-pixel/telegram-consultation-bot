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
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
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

# ============= Google Calendar =============
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

# ============= ПЕРЕВОДЫ =============
LANG = {
    "ru": {
        "choose_lang": "🇷🇺 Пожалуйста, выберите язык / Please choose a language:",
        "start": "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        "language_set": "✅ Язык установлен: Русский 🇷🇺",
        "menu": [
            ["📅 Записаться", "📖 Моя запись"],
            ["🔁 Перенос", "❌ Отмена"],
            ["📎 Получить ссылку", "ℹ️ Инфо"]
        ],
        "cancel": "Отмена",
        "no_booking": "ℹ️ У вас нет активных записей.",
        "enter_name": "✏️ Введите ваше имя и фамилию:",
        "choose_time": "Выберите удобное время:",
        "already_booked": "❌ У вас уже есть активная запись на {slot}.",
        "request_sent": "📨 Запрос отправлен! Ожидайте подтверждения администратора.",
        "info": (
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\nДлительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться."
        ),
        "language_choice_buttons": [["Русский 🇷🇺", "English 🇬🇧"]],
    },
    "en": {
        "choose_lang": "🇷🇺 Пожалуйста, выберите язык / Please choose a language:",
        "start": "👋 Hi! I’m the Migrall consultation booking bot.\nPlease choose an action:",
        "language_set": "✅ Language set: English 🇬🇧",
        "menu": [
            ["📅 Book", "📖 My Booking"],
            ["🔁 Reschedule", "❌ Cancel"],
            ["📎 Get Link", "ℹ️ Info"]
        ],
        "cancel": "Cancel",
        "no_booking": "ℹ️ You have no active bookings.",
        "enter_name": "✏️ Please enter your full name:",
        "choose_time": "Select a suitable time:",
        "already_booked": "❌ You already have an active booking on {slot}.",
        "request_sent": "📨 Request sent! Please wait for administrator confirmation.",
        "info": (
            "ℹ️ Consultation on legalization in Portugal 🇵🇹 and Spain 🇪🇸\n\n"
            "Price: 120 € (VAT 23% may apply)\nDuration: 1 hour\n\n"
            "To book — choose 📅 Book."
        ),
        "language_choice_buttons": [["Русский 🇷🇺", "English 🇬🇧"]],
    }
}

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
        if len(row) >= 6 and row[5].strip() == str(user_id):
            slot_text = row[1].strip()
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
        LANG["ru"]["choose_lang"],
        reply_markup=ReplyKeyboardMarkup(LANG["ru"]["language_choice_buttons"], resize_keyboard=True)
    )

# ============= ОСНОВНАЯ ЛОГИКА =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user_id = update.message.from_user.id
    lang = context.user_data.get("lang", None)

    # === Выбор языка ===
    if text in ("Русский 🇷🇺", "English 🇬🇧"):
        lang = "ru" if "Рус" in text else "en"
        context.user_data["lang"] = lang
        await update.message.reply_text(
            LANG[lang]["language_set"],
            reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True)
        )
        return

    # Если язык ещё не выбран
    if not lang:
        await start(update, context)
        return

    # === Старт заново ===
    if text.lower() == "/start":
        await start(update, context)
        return

    # === Моя запись ===
    if text in ("📖 Моя запись", "📖 My Booking"):
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[9] if len(row) > 9 else ""
            msg = f"📋 {'Your booking' if lang == 'en' else 'Ваша запись'}:\n\n🗓 {slot}\n"
            msg += f"{'Status' if lang == 'en' else 'Статус'}: {status}"
            if meet_link:
                msg += f"\n🔗 {'Link' if lang == 'en' else 'Ссылка'}: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
        else:
            await update.message.reply_text(LANG[lang]["no_booking"], reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
        return

    # === Инфо ===
    if text in ("ℹ️ Инфо", "ℹ️ Info"):
        await update.message.reply_text(LANG[lang]["info"], reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
        return

    # === Записаться ===
    if text in ("📅 Записаться", "📅 Book"):
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            await update.message.reply_text(LANG[lang]["already_booked"].format(slot=slot), reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
            return
        context.user_data["step"] = "ask_name"
        await update.message.reply_text(LANG[lang]["enter_name"], reply_markup=ReplyKeyboardMarkup([[LANG[lang]["cancel"]]], resize_keyboard=True))
        return

    # === Имя ===
    if context.user_data.get("step") == "ask_name":
        context.user_data["full_name"] = text
        all_rows = await run_in_thread(sheet.get_all_values)
        now = datetime.datetime.now()
        free_slots = []
        for r in all_rows[1:]:
            if len(r) >= 3 and r[2].strip() == "":
                dt = parse_slot_datetime(r[1].strip())
                if dt and dt > now:
                    free_slots.append(r[1].strip())
        if not free_slots:
            await update.message.reply_text("❌ Нет доступных слотов.", reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
            context.user_data.clear()
            return
        await update.message.reply_text(LANG[lang]["choose_time"], reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, slot)
        except Exception:
            await update.message.reply_text("❌ Слот не найден.", reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
            context.user_data.clear()
            return
        current_status = (await run_in_thread(sheet.cell, cell.row, 3)).value or ""
        if current_status.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.", reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
            context.user_data.clear()
            return
        full_name = context.user_data.get("full_name", "Без имени")
        username_val = f"@{update.message.from_user.username}" if update.message.from_user.username else ""
        def write_request():
            sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            if not sheet.cell(cell.row, 8).value:
                sheet.update_cell(cell.row, 8, "0")
        await run_in_thread(write_request)
        await update.message.reply_text(LANG[lang]["request_sent"], reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
        context.user_data.clear()
        return

    # === Отмена ===
    if text == LANG[lang]["cancel"]:
        context.user_data.clear()
        await update.message.reply_text("❌ Отменено.", reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))
        return

    # === Если ничего не подошло ===
    await update.message.reply_text("❓ Команда не распознана.", reply_markup=ReplyKeyboardMarkup(LANG[lang]["menu"], resize_keyboard=True))

# ============= ЗАПУСК =============
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("🚀 Бот запущен с поддержкой двух языков (RU/EN)")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
