import os
import json
import base64
import asyncio
import datetime
import logging
import re
from concurrent.futures import ThreadPoolExecutor

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ========== Настройки и логирование ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor(max_workers=6)

TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://telegram-consultation-bot.onrender.com/webhook")

# ========== Google Sheets (gspread) ==========
sheets_creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDS"])
sheets_creds_dict = json.loads(sheets_creds_json)
sheets_scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(sheets_creds_dict, sheets_scope)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# ========== Google Calendar ==========
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

# ========== Константы / регулярки ==========
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DATE_FORMAT = "%d.%m.%Y, %H:%M"  # формат слота в таблице

# Структура колонок в Google Sheets (1-based индексы) — документируем для ясности:
# 1: (A) индекс / прочее
# 2: (B) slot_text (например "13.10.2025, 15:00")
# 3: (C) status ("" / "Ожидает подтверждения" / "Подтверждено" / ...)
# 4: (D) full_name
# 5: (E) username
# 6: (F) user_id
# 7: (G) -- (unused)
# 8: (H) transfers (число)
# 9: (I) remind24_flag ("0" или "1")
# 10:(J) email_for_meet
# 11:(K) meet_link (или "pending")
# 12:(L) lang ("ru" или "en")

# ========== Утилиты ==========
def parse_slot_datetime(slot_text: str):
    try:
        return datetime.datetime.strptime(slot_text, DATE_FORMAT)
    except Exception:
        return None

async def run_in_thread(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

# Синхронная функция поиска будущей подтверждённой (или любой) записи пользователя
def find_user_booking_sync(user_id: int):
    all_rows = sheet.get_all_values()
    now = datetime.datetime.now()
    for idx, row in enumerate(all_rows[1:], start=2):
        # безопасно читаем поля
        status = row[2].strip() if len(row) > 2 else ""
        slot_text = row[1].strip() if len(row) > 1 else ""
        uid = row[5].strip() if len(row) > 5 else ""
        if uid == str(user_id):
            slot_dt = parse_slot_datetime(slot_text)
            if slot_dt and slot_dt > now:
                return idx, row, slot_text
    return None, None, None

async def find_user_booking(user_id: int):
    return await run_in_thread(find_user_booking_sync, user_id)

def get_main_menu(lang: str):
    ru = {
        "book": "📅 Записаться",
        "my_booking": "📖 Моя запись",
        "reschedule": "🔁 Перенос",
        "cancel": "❌ Отмена",
        "get_link": "📎 Получить ссылку",
        "info": "ℹ️ Инфо",
    }
    en = {
        "book": "📅 Book",
        "my_booking": "📖 My Booking",
        "reschedule": "🔁 Reschedule",
        "cancel": "❌ Cancel",
        "get_link": "📎 Get Link",
        "info": "ℹ️ Info",
    }
    map_used = ru if lang == "ru" else en
    return [
        [map_used["book"], map_used["my_booking"]],
        [map_used["reschedule"], map_used["cancel"]],
        [map_used["get_link"], map_used["info"]],
    ]

# ========== Хэндлеры ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Начало: выбираем язык
    context.user_data.clear()
    lang_keyboard = [["Русский", "English"]]
    await update.message.reply_text(
        "Please choose your language / Пожалуйста, выберите язык:",
        reply_markup=ReplyKeyboardMarkup(lang_keyboard, resize_keyboard=True)
    )
    context.user_data["step"] = "choose_lang"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user = update.message.from_user
    user_id = user.id

    # /start
    if text.lower() == "/start":
        await start(update, context)
        return

    # Выбор языка (один раз)
    if context.user_data.get("step") == "choose_lang":
        if text == "Русский":
            context.user_data["lang"] = "ru"
        elif text == "English":
            context.user_data["lang"] = "en"
        else:
            await update.message.reply_text("Please choose from the buttons / Пожалуйста, выберите из кнопок.")
            return
        lang = context.user_data["lang"]
        welcome = (
            "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:"
            if lang == "ru" else
            "👋 Hello! I am a bot for booking a Migrall consultation.\nChoose an action:"
        )
        await update.message.reply_text(
            welcome,
            reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True)
        )
        context.user_data.pop("step", None)
        return

    # Если язык не выбран — перенаправляем на start
    if "lang" not in context.user_data:
        await start(update, context)
        return

    lang = context.user_data["lang"]
    # Универсальная отмена
    if text.lower() in ("отмена", "cancel"):
        context.user_data.clear()
        msg = "❌ Действие отменено." if lang == "ru" else "❌ Action canceled."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        return

    # === Моя запись ===
    if text in (get_main_menu(lang)[0][1], "📖 Моя запись", "📖 My Booking"):
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[10] if len(row) > 10 else ""
            msg = (
                f"📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}"
                if lang == "ru" else
                f"📋 Your booking:\n\n🗓 {slot}\nStatus: {status}"
            )
            if meet_link:
                msg += f"\n🔗 Ссылка: {meet_link}" if lang == 'ru' else f"\n🔗 Link: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        else:
            msg = "ℹ️ У вас нет активных записей." if lang == 'ru' else "ℹ️ You have no active bookings."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
        return

    # === Инфо ===
    if text in (get_main_menu(lang)[2][1], "ℹ️ Инфо", "ℹ️ Info"):
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
    if text in (get_main_menu(lang)[0][0], "📅 Записаться", "📅 Book"):
        # проверим, есть ли уже запись
        r_idx, r_row, r_slot = await find_user_booking(user_id)
        if r_idx:
            msg = f"❌ У вас уже есть активная запись на {r_slot}." if lang == 'ru' else f"❌ You already have an active booking for {r_slot}."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        context.user_data["step"] = "ask_name"
        ask_msg = "✏️ Введите ваше имя и фамилию:" if lang == 'ru' else "✏️ Enter your first and last name:"
        cancel_button = [[ "Отмена" if lang=="ru" else "Cancel" ]]
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
        return

    # === Шаг: имя для записи ===
    if context.user_data.get("step") == "ask_name":
        context.user_data["full_name"] = text
        # Получаем доступные слоты
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

    # === Шаг: выбор слота ===
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

        # Записываем запрос в таблицу
        full_name = context.user_data.get("full_name", "Без имени")
        username_val = f"@{user.username}" if user.username else ""
        def write_request():
            sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            # первоначальные значения
            h_val = sheet.cell(cell.row, 8).value
            if not h_val:
                sheet.update_cell(cell.row, 8, "0")
            sheet.update_cell(cell.row, 9, "0")
            sheet.update_cell(cell.row, 12, lang)
        await run_in_thread(write_request)

        msg = "📨 Запрос отправлен! Ожидайте подтверждения администратора." if lang == 'ru' else "📨 Request sent! Wait for administrator confirmation."
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))

        # Уведомляем админа (всегда на русском) с inline-кнопками
        admin_text = f"📩 Новый запрос:\n👤 {full_name}\n💬 {username_val}\n🕒 {slot}\n\nНажмите кнопку для действия."
        # используем callback_data вида confirm:<row> или refuse:<row>
        try:
            await context.bot.send_message(
                ADMIN_ID,
                admin_text,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm:{cell.row}"),
                        InlineKeyboardButton("❌ Отказать", callback_data=f"refuse:{cell.row}")
                    ]
                ])
            )
            logger.info("Уведомление админу отправлено (inline)")
        except Exception as e:
            logger.error(f"Ошибка отправки админу: {e}")

        context.user_data.clear()
        return

    # === Получить ссылку ===
    if text in (get_main_menu(lang)[2][0], "📎 Получить ссылку", "📎 Get Link"):
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ У вас нет активной записи." if lang == 'ru' else "❌ You have no active booking."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        meet_link = row[10] if len(row) > 10 else ""
        if meet_link and meet_link != "pending":
            msg = f"🔗 Ваша ссылка: {meet_link}" if lang == 'ru' else f"🔗 Your link: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        # если нет ссылки, предлагается отправить сейчас или позже
        context.user_data["await_meet_creation"] = {"row": row_idx, "slot": slot}
        ask_msg = (
            "Хотите, чтобы ссылка на Google Meet была выслана прямо сейчас или за 15 минут до встречи?"
            if lang == 'ru' else
            "Do you want the Google Meet link sent right now or 15 minutes before the meeting?"
        )
        meet_buttons = [["🔗 Получить сейчас" if lang=="ru" else "🔗 Get now", "⏰ За 15 минут до встречи" if lang=="ru" else "⏰ 15 minutes before"]]
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup(meet_buttons, resize_keyboard=True))
        return

    # === Отмена записи пользователем ===
    if text in (get_main_menu(lang)[1][1], "❌ Отмена", "❌ Cancel"):
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

    # === Переадресация: если пользователь нажал "Получить сейчас/позже" ===
    if text in ("🔗 Получить сейчас", "🔗 Get now", "⏰ За 15 минут до встречи", "⏰ 15 minutes before"):
        # Сохраняем выбор и просим email
        choice = "now" if "сейчас" in text or "Get now" in text else "later"
        context.user_data["meet_choice"] = choice
        ask_msg = "Введите, пожалуйста, ваш email для отправки приглашения:" if lang == 'ru' else "Please enter your email to send the invitation:"
        cancel_button = [[ "Отмена" if lang=="ru" else "Cancel" ]]
        await update.message.reply_text(ask_msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
        return

    # === Пользователь ввёл email для meet (now / later) ===
    if "meet_choice" in context.user_data:
        email = text.strip()
        if not EMAIL_RE.match(email):
            msg = "❌ Неверный формат email. Попробуйте снова:" if lang == 'ru' else "❌ Invalid email format. Try again:"
            cancel_button = [[ "Отмена" if lang=="ru" else "Cancel" ]]
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(cancel_button, resize_keyboard=True))
            return
        choice = context.user_data.pop("meet_choice")
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            msg = "❌ Не найдена подтверждённая запись. Свяжитесь с администратором." if lang == 'ru' else "❌ Confirmed booking not found. Contact the administrator."
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        full_name = row[3] if len(row) > 3 else ""
        # now -> создаём событие и отправляем ссылку сейчас
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
                await context.bot.send_message(user_id, send_msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
                await update.message.reply_text("✅ Ссылка создана и отправлена.", reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            except Exception as e:
                logger.error(f"Ошибка создания события: {e}")
                msg = f"⚠️ Ошибка создания события: {e}" if lang == 'ru' else f"⚠️ Error creating event: {e}"
                await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))
            return
        else:
            # later -> сохраняем email и пометку pending
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

    # если команда не распознана:
    msg = "Не понял команду — попробуйте ещё раз." if lang == 'ru' else "Didn't understand the command — try again."
    await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(get_main_menu(lang), resize_keyboard=True))


# ========== CallbackQueryHandler для админских inline-кнопок ==========
async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data  # формат: "confirm:<row>" или "refuse:<row>"
    user = query.from_user
    if query.message.chat_id != ADMIN_ID:
        # Защита: только админ может нажимать
        await query.edit_message_text("Только администратор может выполнять это действие.")
        return

    try:
        action, row_str = data.split(":", 1)
        row = int(row_str)
    except Exception:
        await query.edit_message_text("Неверные данные callback.")
        return

    # читаем строку
    try:
        row_values = await run_in_thread(lambda: sheet.row_values(row))
    except Exception as e:
        logger.error(f"Ошибка чтения строки для admin action: {e}")
        await query.edit_message_text(f"Ошибка доступа к таблице: {e}")
        return

    slot_time = row_values[1] if len(row_values) > 1 else ""
    user_id_cell = row_values[5] if len(row_values) > 5 else ""
    user_lang = row_values[11] if len(row_values) > 11 else "ru"
    full_name = row_values[3] if len(row_values) > 3 else ""
    username_val = row_values[4] if len(row_values) > 4 else ""

    if action == "confirm":
        # Обновляем статус в таблице
        try:
            await run_in_thread(sheet.update_cell, row, 3, "Подтверждено")
        except Exception as e:
            logger.error(f"Ошибка записи подтверждения: {e}")
            await query.edit_message_text(f"Ошибка записи подтверждения: {e}")
            return

        # Отправляем сообщение пользователю (на его языке), без лишних welcome/choose_lang
        confirmed_msg = (
            f"✅ Ваша запись на {slot_time} подтверждена!\n"
            "Хотите, чтобы ссылка на Google Meet была выслана прямо сейчас или за 15 минут до встречи?"
            if user_lang == 'ru' else
            f"✅ Your booking for {slot_time} is confirmed!\n"
            "Do you want the Google Meet link sent right now or 15 minutes before the meeting?"
        )
        # кнопки на языке пользователя
        now_label = "🔗 Получить сейчас" if user_lang == "ru" else "🔗 Get now"
        later_label = "⏰ За 15 минут до встречи" if user_lang == "ru" else "⏰ 15 minutes before"
        try:
            if user_id_cell:
                await context.bot.send_message(int(user_id_cell), confirmed_msg,
                                               reply_markup=ReplyKeyboardMarkup([[now_label, later_label]], resize_keyboard=True))
        except Exception as e:
            logger.error(f"Ошибка отправки подтверждения пользователю: {e}")

        # Подтверждение в админском сообщении (редактируем сообщение)
        await query.edit_message_text(f"✅ Подтвержено: {full_name} — {slot_time}")
        # Уведомление админу в личный лог (не обязательно)
        try:
            await context.bot.send_message(ADMIN_ID, f"✅ Подтверждена запись: {full_name} — {slot_time}")
        except Exception:
            pass
        return

    if action == "refuse":
        # очищаем строку
        try:
            def clear_row():
                for c in range(3, 13):
                    sheet.update_cell(row, c, "")
            await run_in_thread(clear_row)
        except Exception as e:
            logger.error(f"Ошибка очистки строки при отказе: {e}")
            await query.edit_message_text(f"Ошибка при отказе: {e}")
            return

        refused_msg = (
            f"❌ Ваша запись на {slot_time} не подтверждена."
            if user_lang == 'ru' else
            f"❌ Your booking for {slot_time} is not confirmed."
        )
        try:
            if user_id_cell:
                await context.bot.send_message(int(user_id_cell), refused_msg)
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя об отказе: {e}")

        await query.edit_message_text(f"❌ Отказано: {full_name} — {slot_time}")
        try:
            await context.bot.send_message(ADMIN_ID, f"❌ Отказ: {full_name} — {slot_time}")
        except Exception:
            pass
        return

    await query.edit_message_text("Неизвестное действие.")


# ========== Фоновая задача: напоминания и создание Meet за 15 минут ==========
async def background_jobs(app):
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

            # Отправка Meet за 15 минут до встречи (если email задан и meet_link помечен как "pending")
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

# ========== Запуск приложения ==========
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(admin_callback_handler))

    # Запускаем фоновую задачу (JobQueue)
    try:
        # используем job_queue встроенный в telegram.ext
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
