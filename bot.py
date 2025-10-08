# bot.py
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
    """Ищет будущую запись пользователя.
    Возвращает (row_index (1-based), row_values, slot_str) или (None, None, None)."""
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

    # === Моя запись ===
    if text == "📖 Моя запись":
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[10] if len(row) > 10 else ""
            msg = f"📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}"
            if meet_link:
                msg += f"\n🔗 Ссылка: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        await update.message.reply_text("ℹ️ У вас нет активных записей.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Получить ссылку (меню) ===
    if text == "📎 Получить ссылку":
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ У вас нет активной записи.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        meet_link = row[10] if len(row) > 10 else ""
        if meet_link:
            await update.message.reply_text(f"🔗 Ваша ссылка: {meet_link}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        # нет ссылки — предложить создать
        context.user_data["await_meet_creation"] = {"row": row_idx, "slot": slot, "user_id": str(user_id), "full_name": row[3] if len(row) > 3 else ""}
        await update.message.reply_text(
            "Ссылка на встречу ещё не создана. Хотите получить её сейчас?",
            reply_markup=ReplyKeyboardMarkup([["🔗 Создать Google Meet сейчас", "⏰ Позже"]], resize_keyboard=True)
        )
        return

    # === Универсальная отмена шага ===
    if text == "Отмена":
        context.user_data.clear()
        await update.message.reply_text("❌ Действие отменено.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Отмена записи ===
    if text == "❌ Отмена":
        all_rows = await run_in_thread(sheet.get_all_values)
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) >= 6 and row[5].strip() == str(user_id):
                slot = row[1]
                def clear_row():
                    for c in range(3, 12):  # C..K
                        sheet.update_cell(i, c, "")
                await run_in_thread(clear_row)
                await update.message.reply_text(f"✅ Ваша запись на {slot} отменена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                try:
                    await context.bot.send_message(ADMIN_ID, f"❌ Отмена: {row[3]} ({row[4]}) — {slot}")
                except Exception as e:
                    logger.error(f"Ошибка уведомления админу при отмене: {e}")
                return
        await update.message.reply_text("❌ У вас нет записи для отмены.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Перенос ===
    if text == "🔁 Перенос":
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ У вас нет записи для переноса.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
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
            await update.message.reply_text("❌ Нет доступных слотов для переноса.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        context.user_data["step"] = "transfer_choose"
        context.user_data["transfer_from_row"] = row_idx
        await update.message.reply_text("Выберите новый слот для переноса:", reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        return

    if context.user_data.get("step") == "transfer_choose":
        new_slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, new_slot)
        except Exception:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        if (await run_in_thread(sheet.cell, cell.row, 3)).value.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        from_row = context.user_data.get("transfer_from_row")
        if not from_row:
            await update.message.reply_text("❌ Внутренняя ошибка. Повторите попытку.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        old_row_values = await run_in_thread(lambda: sheet.row_values(from_row))
        def do_transfer():
            for c in range(3, 12):
                sheet.update_cell(from_row, c, "")
            sheet.update_cell(cell.row, 3, "Подтверждено (перенос)")
            if len(old_row_values) >= 6:
                sheet.update_cell(cell.row, 4, old_row_values[3] if len(old_row_values) > 3 else "")
                sheet.update_cell(cell.row, 5, old_row_values[4] if len(old_row_values) > 4 else "")
                sheet.update_cell(cell.row, 6, old_row_values[5] if len(old_row_values) > 5 else "")
            transfers = int(old_row_values[7]) + 1 if len(old_row_values) > 7 and old_row_values[7].isdigit() else 1
            sheet.update_cell(cell.row, 8, str(transfers))
        await run_in_thread(do_transfer)
        await update.message.reply_text(f"✅ Запись перенесена на {new_slot}.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        try:
            await context.bot.send_message(ADMIN_ID, f"🔁 Перенос: {old_row_values[3]} ({old_row_values[4]}) — {old_row_values[1]} → {new_slot}")
        except Exception as e:
            logger.error(f"Ошибка уведомления админу о переносе: {e}")
        context.user_data.clear()
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\nДлительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Записаться (начало) ===
    if text == "📅 Записаться":
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            await update.message.reply_text(f"❌ У вас уже есть активная запись на {slot}.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        context.user_data["step"] = "ask_name"
        await update.message.reply_text("✏️ Введите ваше имя и фамилию:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
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
            await update.message.reply_text("❌ Нет доступных слотов.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        await update.message.reply_text("Выберите удобное время:", reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True))
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text.strip()
        try:
            cell = await run_in_thread(sheet.find, slot)
        except Exception:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        current_status = (await run_in_thread(sheet.cell, cell.row, 3)).value or ""
        if current_status.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return
        full_name = context.user_data.get("full_name", "Без имени")
        username_val = f"@{user.username}" if user.username else ""
        def write_request():
            sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            # ensure Email col exists (7), Переносы(8), Напомнено(9) etc exist
            if not sheet.cell(cell.row, 8).value:
                sheet.update_cell(cell.row, 8, "0")
            if not sheet.cell(cell.row, 9).value:
                sheet.update_cell(cell.row, 9, "0")
        await run_in_thread(write_request)
        await update.message.reply_text("📨 Запрос отправлен! Ожидайте подтверждения администратора.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
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
            # notify user: ask now/later (user chooses)
            if user_id_cell:
                try:
                    await context.bot.send_message(int(user_id_cell),
                        f"✅ Ваша запись на {slot_time} подтверждена!\nХотите получить ссылку на Google Meet сейчас или за 15 минут до начала?",
                        reply_markup=ReplyKeyboardMarkup([["🔗 Получить сейчас", "⏰ Позже"]], resize_keyboard=True)
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки пользователю (подтверждение): {e}")
            await update.message.reply_text(f"✅ Подтверждено: {uname} — {slot_time}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"⚠️ Ошибка подтверждения: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Отказать (админ) ===
    if text.startswith("❌ Отказать|"):
        try:
            _, uname, row_str = text.split("|")
            row = int(row_str)
            slot_time = (await run_in_thread(sheet.cell, row, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row, 6)).value
            def clear_row():
                for c in range(3, 12):
                    sheet.update_cell(row, c, "")
            await run_in_thread(clear_row)
            if user_id_cell:
                try:
                    await context.bot.send_message(int(user_id_cell), f"❌ Ваша запись на {slot_time} не подтверждена.")
                except Exception as e:
                    logger.error(f"Ошибка уведомления пользователю об отказе: {e}")
            await update.message.reply_text(f"❌ Отказано: {uname} — {slot_time}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"⚠️ Ошибка отказа: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Пользователь: выбрал сейчас / позже ===
    if text == "🔗 Получить сейчас":
        # set flag and ask email
        context.user_data["meet_choice"] = "now"
        await update.message.reply_text("Введите, пожалуйста, ваш email для отправки приглашения:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
        return

    if text == "⏰ Позже":
        context.user_data["meet_choice"] = "later"
        await update.message.reply_text("Введите, пожалуйста, email, на который вы хотите получить приглашение за 15 минут до встречи:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
        return

    # === Пользователь ввёл email (для now or later) ===
    if "meet_choice" in context.user_data and context.user_data["meet_choice"] in ("now", "later"):
        email = text.strip()
        if not EMAIL_RE.match(email):
            await update.message.reply_text("❌ Неверный формат email. Попробуйте снова:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
            return

        choice = context.user_data.pop("meet_choice")
        # найдём booking row
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ Не найдена подтверждённая запись. Свяжитесь с администратором.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        full_name = row[3] if len(row) > 3 else ""
        # save email in column G (7)
        await run_in_thread(sheet.update_cell, row_idx, 7, email)

        if choice == "now":
            # создаём событие прямо сейчас и отправляем ссылку
            event_start = parse_slot_datetime(slot)
            if not event_start:
                await update.message.reply_text("⚠️ Неверный формат времени слота. Обратитесь к администратору.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                return
            event_end = event_start + datetime.timedelta(hours=1)
            request_id = f"migrall-{row_idx}-{int(datetime.datetime.now().timestamp())}"
            event_body = {
                "summary": f"Консультация Migrall — {full_name}",
                "description": f"Консультация с {full_name}",
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
            except Exception as e:
                logger.exception("Ошибка создания события (now)")
                await update.message.reply_text(f"⚠️ Ошибка создания события: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                return

            meet_link = event.get("hangoutLink") or ""
            # сохраняем ссылку в колонку K (11)
            try:
                await run_in_thread(sheet.update_cell, row_idx, 11, meet_link)
            except Exception as e:
                logger.error(f"Ошибка записи ссылки в таблицу: {e}")

            # отправляем в чат и возвращаем в меню
            try:
                await context.bot.send_message(user_id, f"🔗 Ваша ссылка на Google Meet: {meet_link}")
            except Exception as e:
                logger.error(f"Ошибка отправки ссылки пользователю: {e}")

            await update.message.reply_text("✅ Ссылка создана и отправлена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        else:  # later
            # пометим, что нужно создать ссылку позже — сохраним в колонке J (Напомнено) значение "pending"
            try:
                await run_in_thread(sheet.update_cell, row_idx, 10, "pending")
            except Exception as e:
                logger.error(f"Ошибка записи pending: {e}")
            await update.message.reply_text("✅ Email сохранён. Ссылка будет автоматически отправлена за 15 минут до встречи.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

    # === Если не распознали команду ===
    await update.message.reply_text("Не понял команду — попробуйте ещё раз.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))


# ============= ФОНОВАЯ ЗАДАЧА (создание Meet за 15 минут) =============
async def background_jobs(app: Application):
    # запускается job-queue каждую минуту
    try:
        all_rows = await run_in_thread(sheet.get_all_values)
    except Exception as e:
        logger.error(f"Ошибка чтения Google Sheets в background_jobs: {e}")
        return

    now = datetime.datetime.now()
    for i, row in enumerate(all_rows[1:], start=2):
        # проверяем: Статус == "Подтверждено", Email (G) заполнен, Ссылка (K) пуста, Напомнено (J) == "pending"
        try:
            status = row[2].strip() if len(row) > 2 else ""
            email = row[6].strip() if len(row) > 6 else ""
            remind_flag = row[9].strip() if len(row) > 9 else ""
            link = row[10].strip() if len(row) > 10 else ""
            slot_text = row[1].strip() if len(row) > 1 else ""
            if status == "Подтверждено" and email and remind_flag == "pending" and not link:
                slot_dt = parse_slot_datetime(slot_text)
                if not slot_dt:
                    continue
                seconds_to = (slot_dt - now).total_seconds()
                # если между 0 и 15 минут включительно
                if 0 < seconds_to <= 900:
                    # создаём событие и рассылаем
                    request_id = f"migrall-{i}-{int(datetime.datetime.now().timestamp())}"
                    event_body = {
                        "summary": f"Консультация Migrall — {row[3] if len(row) > 3 else ''}",
                        "description": f"Автоматически создано за 15 минут до встречи",
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
                    except Exception as e:
                        logger.exception(f"Ошибка создания события в background для row {i}: {e}")
                        continue

                    meet_link = event.get("hangoutLink") or ""
                    # записываем ссылку и отмечаем, что напомнили
                    try:
                        await run_in_thread(sheet.update_cell, i, 11, meet_link)  # K
                        await run_in_thread(sheet.update_cell, i, 10, "sent")      # J = sent
                    except Exception as e:
                        logger.error(f"Ошибка записи ссылки в таблицу по row {i}: {e}")

                    # отправляем пользователю в Telegram
                    user_id = row[5].strip() if len(row) > 5 else ""
                    if user_id:
                        try:
                            await app.bot.send_message(int(user_id), f"🔗 Автоматическая отправка — ваша ссылка на Google Meet:\n{meet_link}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки юзеру (background) по row {i}: {e}")
    return

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Запускаем background job каждую минуту (job-queue должен быть доступен: python-telegram-bot[job-queue])
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
