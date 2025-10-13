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

# ============= Google Calendar (Meet) =============
calendar_creds_json = base64.b64decode(os.environ["GOOGLE_CALENDAR_CREDS"])
calendar_creds_dict = json.loads(calendar_creds_json)
calendar_scopes = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.events"
]
calendar_credentials = Credentials.from_service_account_info(
    calendar_creds_dict, scopes=calendar_scopes, subject="ops@migrall.com"
)
calendar_service = build("calendar", "v3", credentials=calendar_credentials)
CALENDAR_ID = "ops@migrall.com"

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
    """Ищет будущую запись пользователя. Возвращает (row_index (1-based), row_values, slot_str) или (None, None, None)."""
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

# ============= МЕНЮ И ТЕКСТЫ =============
actions = {
    "book": {"ru": "📅 Записаться", "en": "📅 Book"},
    "my_booking": {"ru": "📖 Моя запись", "en": "📖 My Booking"},
    "reschedule": {"ru": "🔁 Перенос", "en": "🔁 Reschedule"},
    "cancel": {"ru": "❌ Отмена", "en": "❌ Cancel"},
    "get_link": {"ru": "📎 Получить ссылку", "en": "📎 Get Link"},
    "info": {"ru": "ℹ️ Инфо", "en": "ℹ️ Info"},
    "get_now": {"ru": "🔗 Получить сейчас", "en": "🔗 Get now"},
    "get_later": {"ru": "⏰ За 15 минут до встречи", "en": "⏰ 15 minutes before"},
    "cancel_action": {"ru": "Отмена", "en": "Cancel"},
}

def get_main_menu(lang):
    return [
        [actions["book"][lang], actions["my_booking"][lang]],
        [actions["reschedule"][lang], actions["cancel"][lang]],
        [actions["get_link"][lang], actions["info"][lang]]
    ]

# ============= /start =============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    lang_keyboard = [["Русский", "English"]]
    await update.message.reply_text(
        "Please choose your language / Пожалуйста, выберите язык:",
        reply_markup=ReplyKeyboardMarkup(lang_keyboard, resize_keyboard=True)
    )
    context.user_data['step'] = 'choose_lang'

# ============= ОСНОВНАЯ ЛОГИКА =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user = update.message.from_user
    user_id = user.id
    username = f"@{user.username}" if user.username else f"{user.first_name or ''} {user.last_name or ''}".strip()

    # /start
    if text.lower() == "/start":
        await start(update, context)
        return

    # Выбор языка
    if context.user_data.get('step') == 'choose_lang':
        if text == "Русский":
            lang = 'ru'
        elif text == "English":
            lang = 'en'
        else:
            await update.message.reply_text("Please choose from the buttons / Пожалуйста, выберите из кнопок.")
            return
        context.user_data['lang'] = lang
        welcome = (
            "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:"
            if lang == 'ru' else
            "👋 Hello! I am a bot for booking a Migrall consultation.\nChoose an action:"
        )
        await update.message.reply_text(
            welcome,
            reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True)
        )
        del context.user_data['step']
        return

    if 'lang' not in context.user_data:
        await start(update, context)
        return

    lang = context.user_data['lang']

    # === Универсальная отмена шага ===
    if text == actions["cancel_action"][lang]:
        context.user_data.clear()
        msg = "❌ Действие отменено." if lang == 'ru' else "❌ Action canceled."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        return

    # === Моя запись ===
    if text == actions["my_booking"][lang]:
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[10] if len(row) > 10 else ""
            msg = (
                f"📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}"
                if lang == 'ru' else
                f"📋 Your booking:\n\n🗓 {slot}\nStatus: {status}"
            )
            if meet_link:
                msg += f"\n🔗 Ссылка: {meet_link}" if lang == 'ru' else f"\n🔗 Link: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        else:
            msg = "ℹ️ У вас нет активных записей." if lang == 'ru' else "ℹ️ You have no active bookings."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        return

    # === Получить ссылку (меню) ===
    if text == actions["get_link"][lang]:
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ У вас нет активной записи." if lang == 'ru' else "❌ You have no active booking."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        meet_link = row[10] if len(row) > 10 else ""
        if meet_link:
            msg = f"🔗 Ваша ссылка: {meet_link}" if lang == 'ru' else f"🔗 Your link: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        context.user_data["await_meet_creation"] = {"row": row_idx, "slot": slot, "user_id": str(user_id), "full_name": row[3] if len(row) > 3 else ""}
        ask_msg = (
            "Хотите, чтобы ссылка на Google Meet была выслана прямо сейчас или перед встречей?"
            if lang == 'ru' else
            "Do you want the Google Meet link sent right now or 15 minutes before the meeting?"
        )
        meet_buttons = [[actions["get_now"][lang], actions["get_later"][lang]]]
        await update.message.reply_text(
            ask_msg,
            reply_markup=ReplyKeyboardMarkup(meet_buttons, resize_keyboard=True)
        )
        return

    # === Отмена записи ===
    if text == actions["cancel"][lang]:
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ У вас нет записи для отмены." if lang == 'ru' else "❌ You have no booking to cancel."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        def clear_row():
            for c in range(3, 13):
                sheet.update_cell(row_idx, c, "")
        await run_in_thread(clear_row)
        msg = f"✅ Ваша запись на {slot} отменена." if lang == 'ru' else f"✅ Your booking for {slot} is canceled."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        try:
            await context.bot.send_message(ADMIN_ID, f"❌ Отмена: {row[3]} ({row[4]}) — {slot}")
        except Exception as e:
            logger.error(f"Ошибка уведомления админу при отмене: {e}")
        return

    # === Перенос ===
    if text == actions["reschedule"][lang]:
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ У вас нет записи для переноса." if lang == 'ru' else "❌ You have no booking to reschedule."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        all_rows = await run_in_thread(sheet.get_all_values)
        now = datetime.datetime.now()
        free_slots = []
        for r in all_rows[1:]:
            if len(r) >= 3 and r[2].strip() == "":
                dt = parse_slot_datetime(r[1].strip())
                if dt and dt > now:
                    free_slots.append(r[1].strip())
        if not free_slots:
            msg = "❌ Нет доступных слотов для переноса." if lang == 'ru' else "❌ No available slots for reschedule."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        context.user_data["step"] = "transfer_choose"
        context.user_data["transfer_from_row"] = row_idx
        ask_msg = "Выберите новый слот для переноса:" if lang == 'ru' else "Choose a new slot for reschedule:"
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        return

    if context.user_data.get("step") == "transfer_choose":
        new_slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, new_slot)
        except Exception:
            msg = "❌ Слот не найден. Попробуйте снова." if lang == 'ru' else "❌ Slot not found. Try again."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        if (await run_in_thread(sheet.cell, cell.row, 3)).value.strip() != "":
            msg = "❌ Этот слот уже занят." if lang == 'ru' else "❌ This slot is already taken."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        from_row = context.user_data.get("transfer_from_row")
        if not from_row:
            msg = "❌ Внутренняя ошибка. Повторите попытку." if lang == 'ru' else "❌ Internal error. Try again."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        old_row_values = await run_in_thread(lambda: sheet.row_values(from_row))
        def do_transfer():
            for c in range(3, 13):
                sheet.update_cell(from_row, c, "")
            sheet.update_cell(cell.row, 3, "Подтверждено (перенос)")
            if len(old_row_values) >= 6:
                sheet.update_cell(cell.row, 4, old_row_values[3] if len(old_row_values) > 3 else "")
                sheet.update_cell(cell.row, 5, old_row_values[4] if len(old_row_values) > 4 else "")
                sheet.update_cell(cell.row, 6, old_row_values[5] if len(old_row_values) > 5 else "")
            transfers = int(old_row_values[7]) + 1 if len(old_row_values) > 7 and old_row_values[7].isdigit() else 1
            sheet.update_cell(cell.row, 8, str(transfers))
            sheet.update_cell(cell.row, 9, "0")
            sheet.update_cell(cell.row, 12, old_row_values[11] if len(old_row_values) > 11 else "ru")
        await run_in_thread(do_transfer)
        msg = f"✅ Запись перенесена на {new_slot}." if lang == 'ru' else f"✅ Booking rescheduled to {new_slot}."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        try:
            await context.bot.send_message(ADMIN_ID, f"🔁 Перенос: {old_row_values[3]} ({old_row_values[4]}) — {old_row_values[1]} → {new_slot}")
        except Exception as e:
            logger.error(f"Ошибка уведомления админу о переносе: {e}")
        context.user_data.clear()
        return

    # === Инфо ===
    if text == actions["info"][lang]:
        msg = (
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\nДлительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться."
            if lang == 'ru' else
            "ℹ️ Consultation on legalization in Portugal 🇵🇹 and Spain 🇪🇸\n\n"
            "Cost: 120 € (possible VAT 23%)\nDuration: 1 hour\n\n"
            "To book — choose 📅 Book."
        )
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        return

    # === Записаться (начало) ===
    if text == actions["book"][lang]:
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            msg = f"❌ У вас уже есть активная запись на {slot}." if lang == 'ru' else f"❌ You already have an active booking for {slot}."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        context.user_data["step"] = "ask_name"
        ask_msg = "✏️ Введите ваше имя и фамилию:" if lang == 'ru' else "✏️ Enter your first and last name:"
        cancel_button = [[actions["cancel_action"][lang]]]
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
        return

    # === Имя для записи ===
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
            msg = "❌ Нет доступных слотов на будущее." if lang == 'ru' else "❌ No available slots in the future."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        ask_msg = "Выберите удобное время:" if lang == 'ru' else "Choose a convenient time:"
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, slot)
        except Exception:
            msg = "❌ Слот не найден. Попробуйте снова." if lang == 'ru' else "❌ Slot not found. Try again."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        current_status = (await run_in_thread(sheet.cell, cell.row, 3)).value or ""
        if current_status.strip() != "":
            msg = "❌ Этот слот уже занят." if lang == 'ru' else "❌ This slot is already taken."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        full_name = context.user_data.get("full_name", "Без имени")
        username_val = f"@{user.username}" if user.username else ""
        def write_request():
            sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            if not sheet.cell(cell.row, 8).value:
                sheet.update_cell(cell.row, 8, "0")
            sheet.update_cell(cell.row, 9, "0")
            sheet.update_cell(cell.row, 12, lang)
        await run_in_thread(write_request)
        msg = "📨 Запрос отправлен! Ожидайте подтверждения администратора." if lang == 'ru' else "📨 Request sent! Wait for administrator confirmation."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        admin_msg = f"📩 Новый запрос:\n👤 {full_name}\n💬 {username_val}\n🕒 {slot}"
        try:
            await context.bot.send_message(ADMIN_ID, admin_msg, reply_markup=ReplyKeyboardMarkup(
                [[f"✅ Подтвердить|{username_val}|{cell.row}", f"❌ Отказать|{username_val}|{cell.row}"]],
                resize_keyboard=True
            ))
            logger.info("Уведомление админу отправлено")
        except Exception as e:
            logger.error(f"Ошибка отправки админу: {e}")
        context.user_data.clear()
        return

    # === Подтвердить (админ) ===
    if text.startswith("✅ Подтвердить|"):
        try:
            _, uname, row_str = text.split("|")
            row = int(row_str)
            await run_in_thread(sheet.update_cell, row, 3, "Подтверждено")
            slot_time = (await run_in_thread(sheet.cell, row, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row, 6)).value
            user_lang = (await run_in_thread(sheet.cell, row, 12)).value or 'ru'
            confirmed_msg = (
                f"✅ Ваша запись на {slot_time} подтверждена!\nХотите, чтобы ссылка на Google Meet была выслана прямо сейчас или перед встречей?"
                if user_lang == 'ru' else
                f"✅ Your booking for {slot_time} is confirmed!\nDo you want the Google Meet link sent right now or 15 minutes before the meeting?"
            )
            meet_buttons = [[actions["get_now"][user_lang], actions["get_later"][user_lang]]]
            if user_id_cell:
                try:
                    await context.bot.send_message(
                        int(user_id_cell),
                        confirmed_msg,
                        reply_markup=ReplyKeyboardMarkup(meet_buttons, resize_keyboard=True)
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки пользователю (подтверждение): {e}")
            await update.message.reply_text(f"✅ Подтверждено: {uname} — {slot_time}", reply_markup=ReplyKeyboardMarkup(get_main_menu('ru'), resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"⚠️ Ошибка подтверждения: {e}", reply_markup=ReplyKeyboardMarkup(get_main_menu('ru'), resize_keyboard=True))
        return

    # === Отказать (админ) ===
    if text.startswith("❌ Отказать|"):
        try:
            _, uname, row_str = text.split("|")
            row = int(row_str)
            slot_time = (await run_in_thread(sheet.cell, row, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row, 6)).value
            user_lang = (await run_in_thread(sheet.cell, row, 12)).value or 'ru'
            refused_msg = (
                f"❌ Ваша запись на {slot_time} не подтверждена."
                if user_lang == 'ru' else
                f"❌ Your booking for {slot_time} is not confirmed."
            )
            def clear_row():
                for c in range(3, 13):
                    sheet.update_cell(row, c, "")
            await run_in_thread(clear_row)
            if user_id_cell:
                try:
                    await context.bot.send_message(int(user_id_cell), refused_msg)
                except Exception as e:
                    logger.error(f"Ошибка уведомления пользователю об отказе: {e}")
            await update.message.reply_text(f"❌ Отказано: {uname} — {slot_time}", reply_markup=ReplyKeyboardMarkup(get_main_menu('ru'), resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"⚠️ Ошибка отказа: {e}", reply_markup=ReplyKeyboardMarkup(get_main_menu('ru'), resize_keyboard=True))
        return

    # === Пользователь: выбрал сейчас / за 15 минут ===
    if text in (actions["get_now"][lang], actions["get_later"][lang]):
        context.user_data["meet_choice"] = "now" if text == actions["get_now"][lang] else "later"
        ask_msg = "Введите, пожалуйста, ваш email для отправки приглашения:" if lang == 'ru' else "Please enter your email to send the invitation:"
        cancel_button = [[actions["cancel_action"][lang]]]
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
        return

    # === Пользователь ввёл email (для now или later) ===
    if "meet_choice" in context.user_data and context.user_data["meet_choice"] in ("now", "later"):
        email = text.strip()
        if not EMAIL_RE.match(email):
            msg = "❌ Неверный формат email. Попробуйте снова:" if lang == 'ru' else "❌ Invalid email format. Try again:"
            cancel_button = [[actions["cancel_action"][lang]]]
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
            return
        choice = context.user_data.pop("meet_choice")
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ Не найдена подтверждённая запись. Свяжитесь с администратором." if lang == 'ru' else "❌ Confirmed booking not found. Contact the administrator."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            context.user_data.clear()
            return
        full_name = row[3] if len(row) > 3 else ""
        if choice == "now":
            event_start = parse_slot_datetime(slot)
            if not event_start:
                msg = "⚠️ Неверный формат времени слота. Обратитесь к администратору." if lang == 'ru' else "⚠️ Invalid slot time format. Contact the administrator."
                await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
                return
            event_end = event_start + datetime.timedelta(hours=1)
            request_id = f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}"
            summary = "Консультация Migrall" if lang == 'ru' else "Migrall Consultation"
            description = "Консультация по переезду." if lang == 'ru' else "Relocation consultation."
            event_body = {
                "summary": summary,
                "description": description,
                "start": {"dateTime": event_start.isoformat(), "timeZone": "Europe/Lisbon"},
                "end": {"dateTime": event_end.isoformat(), "timeZone": "Europe/Lisbon"},
                "attendees": [{"email": email}],
                "conferenceData": {
                    "createRequest": {
                        "requestId": request_id,
                        "conferenceSolutionKey": {"type": "hangoutsMeet"}
                    }
                }
            }
            try:
                event = await run_in_thread(lambda: calendar_service.events().insert(
                    calendarId=CALENDAR_ID, body=event_body, conferenceDataVersion=1
                ).execute())
                meet_link = event.get("hangoutLink") or ""
                await run_in_thread(sheet.update_cell, row_idx, 10, email)
                await run_in_thread(sheet.update_cell, row_idx, 11, meet_link)
                send_msg = (
                    f"✅ Ссылка на Google Meet выслана на {email}:\n{meet_link}\n\nЗа 24 часа до встречи вы получите сообщение с напоминанием."
                    if lang == 'ru' else
                    f"✅ Google Meet link sent to {email}:\n{meet_link}\n\nYou will receive a reminder message 24 hours before the meeting."
                )
                await context.bot.send_message(user_id, send_msg)
                reply_msg = "✅ Ссылка создана и отправлена в чат." if lang == 'ru' else "✅ Link created and sent to chat."
                await update.message.reply_text(reply_msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            except Exception as e:
                logger.error(f"Ошибка создания события: {e}")
                msg = f"⚠️ Ошибка создания события: {e}" if lang == 'ru' else f"⚠️ Error creating event: {e}"
                await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        else:  # later
            try:
                await run_in_thread(sheet.update_cell, row_idx, 10, email)
                await run_in_thread(sheet.update_cell, row_idx, 11, "pending")
                msg = (
                    "✅ Email сохранён. Ссылка будет отправлена за 15 минут до встречи в чат."
                    if lang == 'ru' else
                    "✅ Email saved. The link will be sent 15 minutes before the meeting to the chat."
                )
                await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            except Exception as e:
                logger.error(f"Ошибка записи pending: {e}")
                msg = f"⚠️ Ошибка: {e}" if lang == 'ru' else f"⚠️ Error: {e}"
                await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return

    # === Если не распознали команду ===
    msg = "Не понял команду — попробуйте ещё раз." if lang == 'ru' else "Didn't understand the command — try again."
    await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))

# ============= ФОНОВАЯ ЗАДАЧА (создание Meet за 15 минут и напоминания) =============
async def background_jobs(app: Application):
    try:
        all_rows = await run_in_thread(sheet.get_all_values)
    except Exception as e:
        logger.error(f"Ошибка чтения Google Sheets в background_jobs: {e}")
        return
    now = datetime.datetime.now()
    for i, row in enumerate(all_rows[1:], start=2):
        status = row[2].strip() if len(row) > 2 else ""
        remind_flag = row[8].strip() if len(row) > 8 else "0"
        email = row[9].strip() if len(row) > 9 else ""
        link = row[10].strip() if len(row) > 10 else ""
        slot_text = row[1].strip() if len(row) > 1 else ""
        user_id = row[5].strip() if len(row) > 5 else ""
        user_lang = row[11].strip() if len(row) > 11 else "ru"
        if status == "Подтверждено" and user_id:
            slot_dt = parse_slot_datetime(slot_text)
            if not slot_dt:
                continue
            seconds_to = (slot_dt - now).total_seconds()
            # Напоминание за 24 часа
            if remind_flag == "0" and 0 < seconds_to <= 86400:
                try:
                    reminder_msg = (
                        f"⏰ Напоминаем! У вас консультация {slot_text}."
                        if user_lang == 'ru' else
                        f"⏰ Reminder! You have a consultation {slot_text}."
                    )
                    await app.bot.send_message(int(user_id), reminder_msg)
                    await run_in_thread(sheet.update_cell, i, 9, "1")
                except Exception as e:
                    logger.error(f"Ошибка отправки напоминания для row {i}: {e}")
            # Отправка Meet за 15 минут до встречи
            if email and link == "pending" and 0 < seconds_to <= 900:
                request_id = f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}"
                summary = "Консультация Migrall" if user_lang == 'ru' else "Migrall Consultation"
                description = "Консультация по переезду." if user_lang == 'ru' else "Relocation consultation."
                event_body = {
                    "summary": summary,
                    "description": description,
                    "start": {"dateTime": slot_dt.isoformat(), "timeZone": "Europe/Lisbon"},
                    "end": {"dateTime": (slot_dt + datetime.timedelta(hours=1)).isoformat(), "timeZone": "Europe/Lisbon"},
                    "attendees": [{"email": email}],
                    "conferenceData": {
                        "createRequest": {
                            "requestId": request_id,
                            "conferenceSolutionKey": {"type": "hangoutsMeet"}
                        }
                    }
                }
                try:
                    event = await run_in_thread(lambda: calendar_service.events().insert(
                        calendarId=CALENDAR_ID, body=event_body, conferenceDataVersion=1
                    ).execute())
                    meet_link = event.get("hangoutLink") or ""
                    await run_in_thread(sheet.update_cell, i, 11, meet_link)
                    send_msg = (
                        f"🔗 Автоматическая отправка — ваша ссылка на Google Meet:\n{meet_link}"
                        if user_lang == 'ru' else
                        f"🔗 Automatic sending — your Google Meet link:\n{meet_link}"
                    )
                    await app.bot.send_message(int(user_id), send_msg)
                except Exception as e:
                    logger.error(f"Ошибка создания события в background для row {i}: {e}")

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    try:
        app.job_queue.run_repeating(lambda ctx: asyncio.create_task(background_jobs(app)), interval=60, first=10)
    except Exception as e:
        logger.error(f"JobQueue не запущен: {e}. Если возникнут проблемы с отложенной отправкой, установите python-telegram-bot[job-queue].")
    logger.info("🚀 Бот запущен (webhook)")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
