# bot.py
import os
import json
import base64
import asyncio
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

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
async def run_in_thread(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, func, *args)

def parse_slot_datetime(slot_text: str):
    try:
        return datetime.datetime.strptime(slot_text, "%d.%m.%Y, %H:%M")
    except Exception:
        return None

def find_user_booking_sync(user_id: int):
    """Ищет будущую запись пользователя.
    Возвращает (row_index, row_data, slot_time_str) или (None, None, None). row_index — индекс в таблице (1-based)."""
    all_rows = sheet.get_all_values()
    now = datetime.datetime.now()
    # первая строка — заголовок
    for i, row in enumerate(all_rows[1:], start=2):
        # убедимся, что есть хотя бы до колонки User_ID (6)
        if len(row) >= 6:
            user_id_cell = row[5].strip()
            slot_text = row[1].strip()
            if user_id_cell and user_id_cell == str(user_id):
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

    # --- команда /start ---
    if text.lower() == "/start":
        await start(update, context)
        return

    # === Моя запись ===
    if text == "📖 Моя запись":
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            status = row[2] if len(row) > 2 else ""
            meet_link = row[6] if len(row) > 6 else ""
            msg = f"📋 Ваша запись:\n\n🗓 {slot}\nСтатус: {status}"
            if meet_link:
                msg += f"\n🔗 Ссылка: {meet_link}"
            await update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        await update.message.reply_text("ℹ️ У вас нет активных записей.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Получить ссылку (из меню) ===
    if text == "📎 Получить ссылку":
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ У вас нет активной записи.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        meet_link = row[6] if len(row) > 6 else ""
        if meet_link:
            await update.message.reply_text(f"🔗 Ваша ссылка: {meet_link}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        # если ссылки нет, предложим создать сейчас
        context.user_data["await_meet_creation"] = {"row": row_idx, "slot": slot, "user_id": str(user_id), "full_name": row[3] if len(row) > 3 else ""}
        await update.message.reply_text(
            "Ссылка на встречу ещё не создана. Хотите получить её сейчас?",
            reply_markup=ReplyKeyboardMarkup([["🔗 Создать Google Meet сейчас", "⏰ Позже"], ["Отмена"]], resize_keyboard=True)
        )
        return

    # === Отмена (универсальная) ===
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
                    for c in range(3, 10):  # C..I
                        sheet.update_cell(i, c, "")
                await run_in_thread(clear_row)
                await update.message.reply_text(f"✅ Ваша запись на {slot} отменена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                # уведомление админу
                try:
                    await context.bot.send_message(ADMIN_ID, f"❌ Отмена записи\n👤 {row[3]} ({row[4]})\n📅 {slot}")
                except Exception as e:
                    logger.error(f"Ошибка уведомления админу при отмене: {e}")
                return
        await update.message.reply_text("❌ У вас нет записи для отмены.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Перенос (начало) ===
    if text == "🔁 Перенос":
        row_idx, row, slot = await find_user_booking(user_id)
        if not row_idx:
            await update.message.reply_text("❌ У вас нет записи для переноса.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        # список будущих свободных слотов
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
        await update.message.reply_text("Выберите новый слот для переноса:", reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots] + [["Отмена"]], resize_keyboard=True))
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
        # перенос: скопировать данные, очистить старую строку
        old_row_values = await run_in_thread(lambda: sheet.row_values(from_row))
        def do_transfer():
            # очистка старой
            for c in range(3, 10):
                sheet.update_cell(from_row, c, "")
            # запись в новый
            sheet.update_cell(cell.row, 3, "Подтверждено (перенос)")
            # копируем колонки D(4)=name, E(5)=username, F(6)=user_id, G(7)=meet(if any), H(8)=transfers
            if len(old_row_values) >= 6:
                sheet.update_cell(cell.row, 4, old_row_values[3] if len(old_row_values) > 3 else "")
                sheet.update_cell(cell.row, 5, old_row_values[4] if len(old_row_values) > 4 else "")
                sheet.update_cell(cell.row, 6, old_row_values[5] if len(old_row_values) > 5 else "")
            # increment transfers
            transfers = int(old_row_values[7]) + 1 if len(old_row_values) > 7 and old_row_values[7].isdigit() else 1
            sheet.update_cell(cell.row, 8, str(transfers))
        await run_in_thread(do_transfer)
        await update.message.reply_text(f"✅ Запись перенесена на {new_slot}.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        # уведомить админа
        try:
            await context.bot.send_message(ADMIN_ID, f"🔁 Перенос записи\n👤 {old_row_values[3]} ({old_row_values[4]})\n📅 С {old_row_values[1]} → {new_slot}")
        except Exception as e:
            logger.error(f"Ошибка уведомления админу о переносе: {e}")
        context.user_data.clear()
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (возможен НДС 23%)\n"
            "Длительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Записаться (начало) ===
    if text == "📅 Записаться":
        # проверка на существующую будущую запись
        row_idx, row, slot = await find_user_booking(user_id)
        if row_idx:
            await update.message.reply_text(f"❌ У вас уже есть активная запись на {slot}.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return
        context.user_data["step"] = "ask_name"
        await update.message.reply_text("✏️ Введите ваше имя и фамилию:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
        return

    # === Получаем имя для записи ===
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
        await update.message.reply_text("Выберите удобное время:", reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots] + [["Отмена"]], resize_keyboard=True))
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
        # записываем в таблицу: C=Статус, D=Имя, E=Username, F=User_ID, G=Услуга (пусто)
        def write_request():
            sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
            sheet.update_cell(cell.row, 4, full_name)
            sheet.update_cell(cell.row, 5, username_val)
            sheet.update_cell(cell.row, 6, str(user_id))
            # Услуга (G) оставляем пустой пока
            sheet.update_cell(cell.row, 8, sheet.cell(cell.row, 8).value or "0")  # ensure Переносы есть
            sheet.update_cell(cell.row, 9, sheet.cell(cell.row, 9).value or "0")  # Напомнено
        await run_in_thread(write_request)
        await update.message.reply_text("📨 Запрос на запись отправлен! Ожидайте подтверждения администратора.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        # уведомление админу (видит username, не id)
        admin_msg = f"📩 Новый запрос:\n👤 {full_name}\n💬 {username_val}\n🕒 {slot}"
        try:
            # кнопки для админа: format "✅ Подтвердить|<username>|<row>"
            await context.bot.send_message(ADMIN_ID, admin_msg, reply_markup=ReplyKeyboardMarkup(
                [[f"✅ Подтвердить|{username_val}|{cell.row}", f"❌ Отказать|{username_val}|{cell.row}"]],
                resize_keyboard=True
            ))
            logger.info("Уведомление админу отправлено")
        except Exception as e:
            logger.error(f"Ошибка отправки админу: {e}")
        context.user_data.clear()
        return

    # === Подтвердить / Отказать (админ) ===
    if text.startswith("✅ Подтвердить|"):
        try:
            _, uname, row_str = text.split("|")
            row = int(row_str)
            # обновляем статус
            await run_in_thread(sheet.update_cell, row, 3, "Подтверждено")
            slot_time = (await run_in_thread(sheet.cell, row, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row, 6)).value
            full_name = (await run_in_thread(sheet.cell, row, 4)).value
            # уведомляем пользователя, но НЕ создаём Meet — спрашиваем у пользователя, когда отправить ссылку
            if user_id_cell:
                try:
                    await context.bot.send_message(int(user_id_cell),
                        f"✅ Ваша запись на {slot_time} подтверждена!\nХотите получить ссылку на Google Meet сейчас или позже?",
                        reply_markup=ReplyKeyboardMarkup([["🔗 Создать Google Meet сейчас", "⏰ Позже"], ["Отмена"]], resize_keyboard=True)
                    )
                    # сохраняем в контексте ожидание создания (по user_id)
                    # для безопасности сохраняем row и данные по user_id
                    # используем key "await_meet_for_<user_id>"
                    context.user_data[f"await_meet_for_{user_id_cell}"] = {"row": row, "slot": slot_time, "full_name": full_name}
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления пользователю о подтверждении: {e}")
            # подтверждение админу в чате администратора
            await update.message.reply_text(f"✅ Запись подтверждена: {uname} — {slot_time}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"⚠️ Ошибка подтверждения: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    if text.startswith("❌ Отказать|"):
        try:
            _, uname, row_str = text.split("|")
            row = int(row_str)
            slot_time = (await run_in_thread(sheet.cell, row, 2)).value
            user_id_cell = (await run_in_thread(sheet.cell, row, 6)).value
            # очистить запись
            def clear_row():
                for c in range(3, 10):
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

    # === Пользователь выбрал создать Meet сейчас или позже ===
    if text == "🔗 Создать Google Meet сейчас":
        # найдём контекст для этого пользователя
        key = f"await_meet_for_{user_id}"
        data = context.user_data.get(key)
        if not data:
            # возможно предыдущий ключ хранится по другому user_id (legacy) — попробуем поиск по таблице
            row_idx, row, slot = await find_user_booking(user_id)
            if not row_idx:
                await update.message.reply_text("❌ Не найдена подтверждённая запись.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                return
            row = row_idx
            slot = slot
        else:
            row = data["row"]
            slot = data["slot"]
        # создаём событие в календаре (1 час)
        try:
            event_start = parse_slot_datetime(slot)
            if not event_start:
                await update.message.reply_text("⚠️ Неверный формат времени слота.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                return
            event_end = event_start + datetime.timedelta(hours=1)

            request_id = f"migrall-{row}-{int(datetime.datetime.now().timestamp())}"
            event_body = {
                "summary": f"Консультация Migrall — {slot}",
                "description": f"Консультация с {user.first_name or ''} {user.last_name or ''}",
                "start": {"dateTime": event_start.isoformat(), "timeZone": "Europe/Lisbon"},
                "end": {"dateTime": event_end.isoformat(), "timeZone": "Europe/Lisbon"},
                "attendees": [{"email": ""}],  # необязательно, оставляем пустым
                "conferenceData": {
                    "createRequest": {
                        "requestId": request_id,
                        "conferenceSolutionKey": {"type": "hangoutsMeet"}
                    }
                }
            }

            # выполняем вставку в отдельном потоке (блокирующий)
            event = await run_in_thread(calendar_service.events().insert,
                                        calendarId=CALENDAR_ID,
                                        body=event_body,
                                        conferenceDataVersion=1)
            # Note: google-api-python-client returns resource; in run_in_thread above we passed method, not executed - therefore call in lambda
            # to guarantee execution via run_in_thread, adjust:
        except Exception as e:
            # workaround: call insert inside run_in_thread lambda
            try:
                event = await run_in_thread(lambda: calendar_service.events().insert(
                    calendarId=CALENDAR_ID,
                    body=event_body,
                    conferenceDataVersion=1
                ).execute())
            except Exception as e2:
                await update.message.reply_text(f"⚠️ Ошибка создания события: {e2}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
                logger.exception("Ошибка создания события")
                return

        # получаем ссылку
        meet_link = event.get("hangoutLink") or event.get("conferenceData", {}).get("entryPoints", [{}])[0].get("uri", "")
        # сохраняем ссылку в колонку G (7)
        try:
            await run_in_thread(sheet.update_cell, row, 7, meet_link)
        except Exception as e:
            logger.error(f"Ошибка записи ссылки в таблицу: {e}")

        await context.bot.send_message(user_id, f"🔗 Ссылка на Google Meet: {meet_link}")
        await update.message.reply_text("✅ Ссылка создана и отправлена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        # очистим временные данные
        context.user_data.pop(f"await_meet_for_{user_id}", None)
        return

    if text == "⏰ Позже":
        # пользователь отложил получение ссылки
        # мы просто уведомим и вернём в меню
        await update.message.reply_text("⏳ Хорошо. Когда будете готовы получить ссылку — нажмите «📎 Получить ссылку» в меню.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        # не стираем запись — пользователь сможет запросить ссылку позже
        return

    # === Если не распознали команду ===
    await update.message.reply_text("Не понял команду, попробуйте ещё раз.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("🚀 Бот запущен в режиме webhook")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
