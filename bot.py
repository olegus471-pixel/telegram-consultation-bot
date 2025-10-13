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
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
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

# ============= ЛОКАЛИЗАЦИЯ =============
LANGUAGES = {
    "en": {
        "welcome": "👋 Hello! I am Migrall consultation bot.\nPlease choose your language:",
        "main_menu_prompt": "Please select an action:",
        "book_appointment": "📅 Book Appointment",
        "my_appointment": "📖 My Appointment",
        "reschedule": "🔁 Reschedule",
        "cancel_appointment": "❌ Cancel",
        "get_link": "📎 Get Link",
        "info": "ℹ️ Info",
        "appointment_details": "📋 Your Appointment:\n\n🗓 {slot}\nStatus: {status}",
        "link_available": "\n🔗 Link: {link}",
        "no_active_appointment": "ℹ️ You have no active appointments.",
        "no_appointment_for_link": "❌ You have no active appointment to get a link.",
        "your_link": "🔗 Your link: {link}",
        "ask_meet_link_timing": "Do you want the Google Meet link to be sent now or before the meeting?",
        "get_now": "🔗 Get Now",
        "15_min_before": "⏰ 15 Minutes Before",
        "cancel_action": "Cancel",
        "action_cancelled": "❌ Action cancelled.",
        "appointment_cancelled": "✅ Your appointment for {slot} has been cancelled.",
        "admin_cancellation_notification": "❌ Cancellation: {name} ({username}) — {slot}",
        "no_appointment_to_reschedule": "❌ You have no appointment to reschedule.",
        "no_free_slots_for_reschedule": "❌ No available slots for rescheduling.",
        "choose_new_slot": "Choose a new slot for rescheduling:",
        "slot_not_found": "❌ Slot not found. Please try again.",
        "slot_already_taken": "❌ This slot is already taken.",
        "internal_error": "❌ Internal error. Please try again.",
        "appointment_rescheduled": "✅ Appointment rescheduled to {new_slot}.",
        "admin_reschedule_notification": "🔁 Reschedule: {name} ({username}) — {old_slot} → {new_slot}",
        "info_text": (
            "ℹ️ Consultation on legalization in Portugal 🇵🇹 and Spain 🇪🇸\n\n"
            "Cost: 120 € (23% VAT may apply)\nDuration: 1 hour\n\n"
            "To book an appointment — select 📅 Book Appointment."
        ),
        "already_have_appointment": "❌ You already have an active appointment for {slot}.",
        "enter_name": "✏️ Enter your full name:",
        "no_free_slots": "❌ No available slots in the future.",
        "choose_time": "Choose a convenient time:",
        "request_sent": "📨 Request sent! Please await administrator confirmation.",
        "admin_new_request_notification": "📩 New request:\n👤 {full_name}\n💬 {username_val}\n🕒 {slot}",
        "confirm": "✅ Confirm",
        "decline": "❌ Decline",
        "admin_confirmed_notification": "✅ Confirmed: {username} — {slot_time}",
        "admin_confirmation_error": "⚠️ Confirmation error: {error}",
        "user_confirmed_message": "✅ Your appointment for {slot_time} has been confirmed!\nDo you want the Google Meet link to be sent now or before the meeting?",
        "admin_declined_notification": "❌ Declined: {username} — {slot_time}",
        "admin_declined_error": "⚠️ Decline error: {error}",
        "user_declined_message": "❌ Your appointment for {slot_time} has not been confirmed.",
        "enter_email": "Please enter your email for the invitation:",
        "invalid_email": "❌ Invalid email format. Please try again:",
        "no_confirmed_appointment": "❌ No confirmed appointment found. Please contact the administrator.",
        "invalid_slot_time": "⚠️ Invalid slot time format. Please contact the administrator.",
        "meet_link_sent_now": "✅ Google Meet link sent to {email}:\n{link}\n\n"
                               "You will receive a reminder message 24 hours before the meeting.",
        "link_created_in_chat": "✅ Link created and sent to chat.",
        "meet_creation_error": "⚠️ Event creation error: {error}",
        "email_saved_for_later": "✅ Email saved. The link will be sent to the chat 15 minutes before the meeting.",
        "email_save_error": "⚠️ Error: {error}",
        "unknown_command": "I didn't understand the command — please try again.",
        "reminder_message": "⏰ Reminder! You have a consultation {slot}.",
        "auto_meet_link_message": "🔗 Automatic dispatch — your Google Meet link:\n{link}",
    },
    "ru": {
        "welcome": "👋 Привет! Я бот для записи на консультацию Migrall.\nПожалуйста, выберите ваш язык:",
        "main_menu_prompt": "Выберите действие:",
        "book_appointment": "📅 Записаться",
        "my_appointment": "📖 Моя запись",
        "reschedule": "🔁 Перенос",
        "cancel_appointment": "❌ Отмена",
        "get_link": "📎 Получить ссылку",
        "info": "ℹ️ Инфо",
        "appointment_details": "📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}",
        "link_available": "\n🔗 Ссылка: {link}",
        "no_active_appointment": "ℹ️ У вас нет активных записей.",
        "no_appointment_for_link": "❌ У вас нет активной записи, чтобы получить ссылку.",
        "your_link": "🔗 Ваша ссылка: {link}",
        "ask_meet_link_timing": "Хотите, чтобы ссылка на Google Meet была выслана прямо сейчас или перед встречей?",
        "get_now": "🔗 Получить сейчас",
        "15_min_before": "⏰ За 15 минут до встречи",
        "cancel_action": "Отмена",
        "action_cancelled": "❌ Действие отменено.",
        "appointment_cancelled": "✅ Ваша запись на {slot} отменена.",
        "admin_cancellation_notification": "❌ Отмена: {name} ({username}) — {slot}",
        "no_appointment_to_reschedule": "❌ У вас нет записи для переноса.",
        "no_free_slots_for_reschedule": "❌ Нет доступных слотов для переноса.",
        "choose_new_slot": "Выберите новый слот для переноса:",
        "slot_not_found": "❌ Слот не найден. Попробуйте снова.",
        "slot_already_taken": "❌ Этот слот уже занят.",
        "internal_error": "❌ Внутренняя ошибка. Повторите попытку.",
        "appointment_rescheduled": "✅ Запись перенесена на {new_slot}.",
        "admin_reschedule_notification": "🔁 Перенос: {name} ({username}) — {old_slot} → {new_slot}",
        "info_text": (
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\nДлительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться."
        ),
        "already_have_appointment": "❌ У вас уже есть активная запись на {slot}.",
        "enter_name": "✏️ Введите ваше имя и фамилию:",
        "no_free_slots": "❌ Нет доступных слотов на будущее.",
        "choose_time": "Выберите удобное время:",
        "request_sent": "📨 Запрос отправлен! Ожидайте подтверждения администратора.",
        "admin_new_request_notification": "📩 Новый запрос:\n👤 {full_name}\n💬 {username_val}\n🕒 {slot}",
        "confirm": "✅ Подтвердить",
        "decline": "❌ Отказать",
        "admin_confirmed_notification": "✅ Подтверждено: {username} — {slot_time}",
        "admin_confirmation_error": "⚠️ Ошибка подтверждения: {error}",
        "user_confirmed_message": "✅ Ваша запись на {slot_time} подтверждена!\nХотите, чтобы ссылка на Google Meet была выслана прямо сейчас или перед встречей?",
        "admin_declined_notification": "❌ Отказано: {username} — {slot_time}",
        "admin_declined_error": "⚠️ Ошибка отказа: {error}",
        "user_declined_message": "❌ Ваша запись на {slot_time} не подтверждена.",
        "enter_email": "Введите, пожалуйста, ваш email для отправки приглашения:",
        "invalid_email": "❌ Неверный формат email. Попробуйте снова:",
        "no_confirmed_appointment": "❌ Не найдена подтверждённая запись. Свяжитесь с администратором.",
        "invalid_slot_time": "⚠️ Неверный формат времени слота. Обратитесь к администратору.",
        "meet_link_sent_now": "✅ Ссылка на Google Meet выслана на {email}:\n{link}\n\n"
                               "За 24 часа до встречи вы получите сообщение с напоминанием.",
        "link_created_in_chat": "✅ Ссылка создана и отправлена в чат.",
        "meet_creation_error": "⚠️ Ошибка создания события: {error}",
        "email_saved_for_later": "✅ Email сохранён. Ссылка будет отправлена за 15 минут до встречи в чат.",
        "email_save_error": "⚠️ Ошибка: {error}",
        "unknown_command": "Не понял команду — попробуйте ещё раз.",
        "reminder_message": "⏰ Напоминаем! У вас консультация {slot}.",
        "auto_meet_link_message": "🔗 Автоматическая отправка — ваша ссылка на Google Meet:\n{link}",
    },
}

def get_text(lang_code, key):
    return LANGUAGES.get(lang_code, LANGUAGES["en"]).get(key, key)

def get_main_menu_keyboard(lang_code):
    _ = lambda key: get_text(lang_code, key)
    return ReplyKeyboardMarkup(
        [
            [_( "book_appointment"), _("my_appointment")],
            [_( "reschedule"), _("cancel_appointment")],
            [_( "get_link"), _("info")]
        ],
        resize_keyboard=True
    )

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

# ============= /start =============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("English", callback_data='set_lang_en')],
        [InlineKeyboardButton("Русский", callback_data='set_lang_ru')]
    ])
    await update.message.reply_text(get_text("ru", "welcome"), reply_markup=keyboard) # Используем русский для приветствия и выбора языка

async def set_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang_code = query.data.split('_')[2]
    context.user_data["language"] = lang_code
    _ = lambda key: get_text(lang_code, key)

    await query.edit_message_text(
        _("main_menu_prompt"),
        reply_markup=get_main_menu_keyboard(lang_code)
    )

# ============= ОСНОВНАЯ ЛОГИКА =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang_code = context.user_data.get("language", "en") # По умолчанию английский
    _ = lambda key: get_text(lang_code, key)

    text = (update.message.text or "").strip()
    user = update.message.from_user
    user_id = user.id
    username = f"@{user.username}" if user.username else f"{user.first_name or ''} {user.last_name or ''}".strip()

    # /start
    if text.lower() == "/start":
        await start(update, context)
        return

    # === Моя запись ===
    if text == _("my_appointment"):
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[9] if len(row) > 9 else ""
            msg = _("appointment_details").format(slot=slot, status=status)
            if meet_link:
                msg += _("link_available").format(link=meet_link)
            await update.message.reply_text(msg, reply_markup=get_main_menu_keyboard(lang_code))
        else:
            await update.message.reply_text(_("no_active_appointment"), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Получить ссылку (меню) ===
    if text == _("get_link"):
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text(_("no_appointment_for_link"), reply_markup=get_main_menu_keyboard(lang_code))
            return
        meet_link = row[9] if len(row) > 9 else ""
        if meet_link and meet_link != "pending":
            await update.message.reply_text(_("your_link").format(link=meet_link), reply_markup=get_main_menu_keyboard(lang_code))
            return

        context.user_data["await_meet_creation"] = {"row": row_idx, "slot": slot, "user_id": str(user_id), "full_name": row[3] if len(row) > 3 else ""}
        await update.message.reply_text(
            _("ask_meet_link_timing"),
            reply_markup=ReplyKeyboardMarkup([[_("get_now"), _("15_min_before")]], resize_keyboard=True)
        )
        return

    # === Универсальная отмена шага ===
    if text == _("cancel_action"):
        context.user_data.clear()
        await update.message.reply_text(_("action_cancelled"), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Отмена записи ===
    if text == _("cancel_appointment"):
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text(_("no_appointment_to_reschedule"), reply_markup=get_main_menu_keyboard(lang_code))
            return
        def clear_row_sync():
            for c in range(3, 11):
                sheet.update_cell(row_idx, c, "")
        await run_in_thread(clear_row_sync)
        await update.message.reply_text(_("appointment_cancelled").format(slot=slot), reply_markup=get_main_menu_keyboard(lang_code))
        try:
            await context.bot.send_message(ADMIN_ID, _("admin_cancellation_notification").format(name=row[3], username=row[4], slot=slot))
        except Exception as e:
            logger.error(f"Error notifying admin about cancellation: {e}")
        return

    # === Перенос ===
    if text == _("reschedule"):
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text(_("no_appointment_to_reschedule"), reply_markup=get_main_menu_keyboard(lang_code))
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
            await update.message.reply_text(_("no_free_slots_for_reschedule"), reply_markup=get_main_menu_keyboard(lang_code))
            return
        context.user_data["step"] = "transfer_choose"
        context.user_data["transfer_from_row"] = row_idx
        await update.message.reply_text(_("choose_new_slot"), reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        return

    if context.user_data.get("step") == "transfer_choose":
        new_slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, new_slot)
        except Exception:
            await update.message.reply_text(_("slot_not_found"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return
        if (await run_in_thread(sheet.cell, cell.row, 3)).value.strip() != "":
            await update.message.reply_text(_("slot_already_taken"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return

        from_row = context.user_data.get("transfer_from_row")
        if not from_row:
            await update.message.reply_text(_("internal_error"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return
        old_row_values = await run_in_thread(lambda: sheet.row_values(from_row))

        def do_transfer_sync():
            for c in range(3, 11):
                sheet.update_cell(from_row, c, "")
            sheet.update_cell(cell.row, 3, "Confirmed (reschedule)") # Status in English for sheet
            if len(old_row_values) >= 6:
                sheet.update_cell(cell.row, 4, old_row_values[3] if len(old_row_values) > 3 else "")
                sheet.update_cell(cell.row, 5, old_row_values[4] if len(old_row_values) > 4 else "")
                sheet.update_cell(cell.row, 6, old_row_values[5] if len(old_row_values) > 5 else "")
            transfers = int(old_row_values[7]) + 1 if len(old_row_values) > 7 and old_row_values[7].isdigit() else 1
            sheet.update_cell(cell.row, 8, str(transfers))

        await run_in_thread(do_transfer_sync)
        await update.message.reply_text(_("appointment_rescheduled").format(new_slot=new_slot), reply_markup=get_main_menu_keyboard(lang_code))
        try:
            await context.bot.send_message(
                ADMIN_ID,
                _("admin_reschedule_notification").format(
                    name=old_row_values[3],
                    username=old_row_values[4],
                    old_slot=old_row_values[1],
                    new_slot=new_slot
                )
            )
        except Exception as e:
            logger.error(f"Error notifying admin about reschedule: {e}")
        context.user_data.clear()
        return

    # === Инфо ===
    if text == _("info"):
        await update.message.reply_text(_("info_text"), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Записаться (начало) ===
    if text == _("book_appointment"):
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            await update.message.reply_text(_("already_have_appointment").format(slot=slot), reply_markup=get_main_menu_keyboard(lang_code))
            return
        context.user_data["step"] = "ask_name"
        await update.message.reply_text(_("enter_name"), reply_markup=ReplyKeyboardMarkup([[_("cancel_action")]], resize_keyboard=True))
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
            await update.message.reply_text(_("no_free_slots"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return
        await update.message.reply_text(_("choose_time"), reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, slot)
        except Exception:
            await update.message.reply_text(_("slot_not_found"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return
        current_status = (await run_in_thread(sheet.cell, cell.row, 3)).value or ""
        if current_status.strip() != "":
            await update.message.reply_text(_("slot_already_taken"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return

        full_name = context.user_data.get("full_name", "No name")
        username_val = f"@{user.username}" if user.username else ""

        def write_request_sync():
            sheet.update_cell(cell.row, 3, "Pending Confirmation") # Status in English for sheet
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            if not sheet.cell(cell.row, 8).value:
                sheet.update_cell(cell.row, 8, "0") # transfers count
            if not sheet.cell(cell.row, 9).value:
                sheet.update_cell(cell.row, 9, "0") # remind_flag

        await run_in_thread(write_request_sync)
        await update.message.reply_text(_("request_sent"), reply_markup=get_main_menu_keyboard(lang_code))
        admin_msg = _("admin_new_request_notification").format(full_name=full_name, username_val=username_val, slot=slot)
        try:
            await context.bot.send_message(
                ADMIN_ID,
                admin_msg,
                reply_markup=ReplyKeyboardMarkup(
                    [[f"{_('confirm')}|{username_val}|{cell.row}", f"{_('decline')}|{username_val}|{cell.row}"]],
                    resize_keyboard=True
                )
            )
            logger.info("Admin notification sent")
        except Exception as e:
            logger.error(f"Error sending admin notification: {e}")
        context.user_data.clear()
        return

    # === Подтвердить (админ) ===
    if text.startswith(f"{_('confirm')}|"):
        try:
            _, uname, row_str = text.split("|")
            row_idx = int(row_str)
            await run_in_thread(sheet.update_cell, row_idx, 3, "Confirmed") # Status in English for sheet
            slot_time = (await run_in_thread(sheet.cell, row_idx, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row_idx, 6)).value
            if user_id_cell:
                try:
                    # Determine user's language preference if possible, otherwise default to English
                    # For simplicity, we'll assume the user has a language set in user_data or default to 'en'
                    user_lang_code = context.application.user_data.get(int(user_id_cell), {}).get("language", "en")
                    _user = lambda key: get_text(user_lang_code, key)

                    await context.bot.send_message(
                        int(user_id_cell),
                        _user("user_confirmed_message").format(slot_time=slot_time),
                        reply_markup=ReplyKeyboardMarkup([[_user("get_now"), _user("15_min_before")]], resize_keyboard=True)
                    )
                except Exception as e:
                    logger.error(f"Error sending user confirmation (admin): {e}")
            await update.message.reply_text(_("admin_confirmed_notification").format(username=uname, slot_time=slot_time), reply_markup=get_main_menu_keyboard(lang_code))
        except Exception as e:
            await update.message.reply_text(_("admin_confirmation_error").format(error=e), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Отказать (админ) ===
    if text.startswith(f"{_('decline')}|"):
        try:
            _, uname, row_str = text.split("|")
            row_idx = int(row_str)
            slot_time = (await run_in_thread(sheet.cell, row_idx, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row_idx, 6)).value

            def clear_row_sync_admin():
                for c in range(3, 11):
                    sheet.update_cell(row_idx, c, "")
            await run_in_thread(clear_row_sync_admin)

            if user_id_cell:
                try:
                    user_lang_code = context.application.user_data.get(int(user_id_cell), {}).get("language", "en")
                    _user = lambda key: get_text(user_lang_code, key)
                    await context.bot.send_message(int(user_id_cell), _user("user_declined_message").format(slot_time=slot_time))
                except Exception as e:
                    logger.error(f"Error notifying user about decline: {e}")
            await update.message.reply_text(_("admin_declined_notification").format(username=uname, slot_time=slot_time), reply_markup=get_main_menu_keyboard(lang_code))
        except Exception as e:
            await update.message.reply_text(_("admin_declined_error").format(error=e), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Пользователь: выбрал сейчас / за 15 минут ===
    if text in (_("get_now"), _("15_min_before")):
        context.user_data["meet_choice"] = "now" if text == _("get_now") else "later"
        await update.message.reply_text(_("enter_email"), reply_markup=ReplyKeyboardMarkup([[_("cancel_action")]], resize_keyboard=True))
        return

    # === Пользователь ввёл email (для now или later) ===
    if "meet_choice" in context.user_data and context.user_data["meet_choice"] in ("now", "later"):
        email = text.strip()
        if not EMAIL_RE.match(email):
            await update.message.reply_text(_("invalid_email"), reply_markup=ReplyKeyboardMarkup([[_("cancel_action")]], resize_keyboard=True))
            return

        choice = context.user_data.pop("meet_choice")
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text(_("no_confirmed_appointment"), reply_markup=get_main_menu_keyboard(lang_code))
            context.user_data.clear()
            return

        full_name = row[3] if len(row) > 3 else ""
        await run_in_thread(sheet.update_cell, row_idx, 7, email) # Update email in sheet

        if choice == "now":
            event_start = parse_slot_datetime(slot)
            if not event_start:
                await update.message.reply_text(_("invalid_slot_time"), reply_markup=get_main_menu_keyboard(lang_code))
                return
            event_end = event_start + datetime.timedelta(hours=1)
            request_id = f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}"
            event_body = {
"summary": "Migrall Consultation", # Summary in English for Google Calendar
            "description": "Consultation on relocation.", # Description in English for Google Calendar
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
                calendarId=CALENDAR_ID,
                body=event_body,
                conferenceDataVersion=1
            ).execute())
            meet_link = event.get("hangoutLink") or ""
            await run_in_thread(sheet.update_cell, row_idx, 10, meet_link) # Col 10 for Meet link
            # We already updated email in col 7, no need to update col 9 (remind_flag) here
            # col 9 is for remind_flag, col 7 is for email, col 10 is for meet_link
            # Let's adjust based on the original code's sheet usage
            # Original code: sheet.update_cell(row_idx, 9, email) - this was wrong, col 9 is remind_flag
            # Let's assume col 7 is for email, col 10 for link, col 9 for remind_flag
            await context.bot.send_message(
                user_id,
                _("meet_link_sent_now").format(email=email, link=meet_link)
            )
            await update.message.reply_text(_("link_created_in_chat"), reply_markup=get_main_menu_keyboard(lang_code))
        except Exception as e:
            logger.error(f"Error creating event: {e}")
            await update.message.reply_text(_("meet_creation_error").format(error=e), reply_markup=get_main_menu_keyboard(lang_code))
        return
    else:  # choice == "later"
        try:
            await run_in_thread(sheet.update_cell, row_idx, 7, email)  # Save email in col 7
            await run_in_thread(sheet.update_cell, row_idx, 10, "pending") # Mark link as pending in col 10
            await update.message.reply_text(
                _("email_saved_for_later"),
                reply_markup=get_main_menu_keyboard(lang_code)
            )
        except Exception as e:
            logger.error(f"Error saving pending email: {e}")
            await update.message.reply_text(_("email_save_error").format(error=e), reply_markup=get_main_menu_keyboard(lang_code))
        return

    # === Если не распознали команду ===
    await update.message.reply_text(_("unknown_command"), reply_markup=get_main_menu_keyboard(lang_code))

# ============= ФОНОВАЯ ЗАДАЧА (создание Meet за 15 минут и напоминания) =============
async def background_jobs(app: Application):
    try:
        all_rows = await run_in_thread(sheet.get_all_values)
    except Exception as e:
        logger.error(f"Error reading Google Sheets in background_jobs: {e}")
        return

    now = datetime.datetime.now()
    for i, row in enumerate(all_rows[1:], start=2):
        status = row[2].strip() if len(row) > 2 else ""
        email = row[6].strip() if len(row) > 6 else "" # Email is in column G (index 6)
        remind_flag = row[8].strip() if len(row) > 8 else "0" # Remind flag is in column I (index 8)
        meet_link_status = row[9].strip() if len(row) > 9 else "" # Meet link is in column J (index 9)
        slot_text = row[1].strip() if len(row) > 1 else ""
        user_id_str = row[5].strip() if len(row) > 5 else ""

        if status == "Confirmed" and user_id_str: # Status is in English for sheet
            try:
                user_id = int(user_id_str)
            except ValueError:
                logger.warning(f"Invalid user_id in row {i}: {user_id_str}")
                continue

            # Retrieve user's language for reminders
            user_lang_code = app.user_data.get(user_id, {}).get("language", "en")
            _user = lambda key: get_text(user_lang_code, key)

            slot_dt = parse_slot_datetime(slot_text)
            if not slot_dt:
                continue
            seconds_to = (slot_dt - now).total_seconds()

            # Напоминание за 24 часа
            if remind_flag == "0" and 0 < seconds_to <= 86400:
                try:
                    await app.bot.send_message(user_id, _user("reminder_message").format(slot=slot_text))
                    await run_in_thread(sheet.update_cell, i, 9, "1") # Update remind_flag to '1' (reminded)
                except Exception as e:
                    logger.error(f"Error sending reminder for row {i} (user {user_id}): {e}")

            # Отправка Meet за 15 минут до встречи
            if email and meet_link_status == "pending" and 0 < seconds_to <= 900:
                request_id = f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}"
                event_body = {
                    "summary": "Migrall Consultation", # Summary in English for Google Calendar
                    "description": "Consultation on relocation.", # Description in English for Google Calendar
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
                        calendarId=CALENDAR_ID,
                        body=event_body,
                        conferenceDataVersion=1
                    ).execute())
                    meet_link = event.get("hangoutLink") or ""
                    await run_in_thread(sheet.update_cell, i, 10, meet_link) # Update meet link in col 10
                    await app.bot.send_message(user_id, _user("auto_meet_link_message").format(link=meet_link))
                except Exception as e:
                    logger.error(f"Error creating event in background for row {i} (user {user_id}): {e}")
    return # End of background_jobs

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()

    # Store user data globally for background jobs to access language
    app.user_data = {} # Initialize a dictionary to store user_data

    # Custom middleware to store user_data for background jobs
    async def store_user_data_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user:
            user_id = update.effective_user.id
            if user_id not in app.user_data:
                app.user_data[user_id] = {}
            # Update the user's data in the global storage
            app.user_data[user_id].update(context.user_data)
        await context.application.next_update(update)

    # Add the middleware *before* other handlers
    app.add_handler(CallbackQueryHandler(set_language, pattern='^set_lang_'))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    app.add_handler(store_user_data_middleware, group=-1) # Add middleware at a low priority group


    try:
        app.job_queue.run_repeating(lambda ctx: asyncio.create_task(background_jobs(app)), interval=60, first=10)
    except Exception as e:
        logger.error(f"JobQueue not started: {e}. If delayed sending problems occur, install python-telegram-bot[job-queue].")

    logger.info("🚀 Bot launched (webhook)")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
