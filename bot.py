import os
import json
import base64
import asyncio
import datetime
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

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
    filters,
    ContextTypes,
)

# =======================
# Конфигурация / переменные окружения
# =======================
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://telegram-consultation-bot.onrender.com/webhook")

# =======================
# Google Sheets
# =======================
sheets_creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDS"])
sheets_creds_dict = json.loads(sheets_creds_json)

sheets_scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(sheets_creds_dict, sheets_scope)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# =======================
# Google Calendar (Meet)
# =======================
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

# =======================
# Главное меню
# =======================
main_menu = [
    ["📅 Записаться на консультацию Migrall"],
    ["📋 Моя запись"],
    ["🔁 Перенести запись", "❌ Отменить запись"],
    ["ℹ️ Инфо"]
]

# =======================
# Вспомогательные функции
# =======================
def now():
    return datetime.datetime.now()

def parse_slot_time(slot_time_str: str):
    """Парсит строку вида 'dd.mm.YYYY, HH:MM' -> datetime; возвращает None при ошибке"""
    try:
        return datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
    except Exception:
        return None

def find_user_future_booking_row(user_id: int):
    """
    Ищет в таблице будущую запись пользователя.
    Возвращает (row_list, slot_time_str, row_index_in_sheet) или (None, None, None)
    row_index_in_sheet — реальный номер строки в таблице (1-based).
    """
    all_slots = sheet.get_all_values()
    # Пропускаем заголовок, начинаем с 2-й строки
    for idx in range(2, len(all_slots) + 1):
        row = all_slots[idx - 1]
        if len(row) < 5:
            continue
        slot_time_str = row[1].strip() if len(row) > 1 else ""
        booked_user_id = row[4].strip() if len(row) > 4 else ""
        if booked_user_id == str(user_id):
            slot_dt = parse_slot_time(slot_time_str)
            if slot_dt and slot_dt > now():
                return row, slot_time_str, idx
    return None, None, None

def list_free_slots():
    """Возвращает список slot строк, которые считаем свободными (колонка C пустая и статус не 'Ожидает подтверждения' или 'Подтверждено')"""
    result = []
    all_slots = sheet.get_all_values()[1:]  # skip header
    for row in all_slots:
        slot = row[1].strip() if len(row) > 1 else ""
        name = row[2].strip() if len(row) > 2 else ""
        status = row[5].strip() if len(row) > 5 else ""
        if slot and name == "" and status == "":
            result.append(slot)
    return result

# =======================
# Хэндлеры
# =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий админа на Confirm / Reject"""
    query = update.callback_query
    await query.answer()

    data = query.data  # ожидаем формат "action:row:user_id"
    parts = data.split(":")
    if len(parts) < 3:
        await query.edit_message_text("Неверные данные.")
        return

    action, row_str, user_id_str = parts[0], parts[1], parts[2]
    try:
        row_index = int(row_str)
        target_user_id = int(user_id_str)
    except ValueError:
        await query.edit_message_text("Неверные параметры.")
        return

    # Получим слот и имя из таблицы (строка row_index)
    try:
        row_values = sheet.row_values(row_index)
    except Exception as e:
        await query.edit_message_text(f"Ошибка доступа к таблице: {e}")
        return

    slot_time_str = row_values[1] if len(row_values) > 1 else ""
    name = row_values[2] if len(row_values) > 2 else ""
    admin_username = query.from_user.username or query.from_user.first_name

    if action == "confirm":
        # Пометим как подтверждённое
        try:
            sheet.update_cell(row_index, 6, "Подтверждено")  # колонка F = 6
        except Exception as e:
            await query.edit_message_text(f"Ошибка при обновлении статуса: {e}")
            return

        # Установим user_data для пользователя, чтобы он мог выбрать опцию Meet
        # application.user_data: ключ — chat_id/user_id
        user_store = context.application.user_data.setdefault(target_user_id, {})
        user_store["step"] = "meet_option"
        user_store["slot_row"] = row_index

        # Уведомим администратора (заменяем кнопку на текст)
        await query.edit_message_text(f"✅ Запрос подтверждён админом ({admin_username}). Слот: {slot_time_str}")

        # Отправим сообщение пользователю с кнопками (Сейчас / Перед встречей)
        try:
            keyboard = ReplyKeyboardMarkup([["Сейчас", "Перед встречей"], ["Отмена"]], resize_keyboard=True)
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"✅ Ваша заявка на консультацию {slot_time_str} подтверждена!\n\n"
                    "Запись подтверждается после оплаты.\n"
                    "Хотите, чтобы ссылка Google Meet была отправлена сейчас или перед встречей?"
                ),
                reply_markup=keyboard
            )
        except Exception:
            # если не удалось отправить (пользователь не начинал чат), просто логгируем (или можно попросить админа связаться)
            await context.bot.send_message(ADMIN_ID, f"Не удалось отправить сообщение пользователю {target_user_id}. Он, возможно, не запускал бота.")
        return

    elif action == "reject":
        # Очистим запись в таблице (колонки 3..10)
        try:
            for col in range(3, 11):
                sheet.update_cell(row_index, col, "")
        except Exception as e:
            await query.edit_message_text(f"Ошибка при очистке слота: {e}")
            return

        await query.edit_message_text(f"❌ Запрос отклонён админом ({admin_username}). Слот {slot_time_str} освобождён.")

        # Уведомим пользователя об отказе
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"❌ К сожалению, ваша заявка на консультацию {slot_time_str} не подтверждена.\n"
                    "Пожалуйста, выберите другой слот или свяжитесь с администратором."
                ),
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
        except Exception:
            await context.bot.send_message(ADMIN_ID, f"Не удалось отправить уведомление об отказе пользователю {target_user_id}.")
        return

    else:
        await query.edit_message_text("Неизвестное действие.")
        return

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    chat_id = update.message.chat_id
    user_id = user.id
    username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"

    # Отмена в любом месте
    if text == "Отмена":
        context.user_data.clear()
        await update.message.reply_text("❌ Действие отменено.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # Стартовое меню
    if text == "📅 Записаться на консультацию Migrall":
        # Проверяем, есть ли уже будущая запись
        _, slot_time_str, _ = find_user_future_booking_row(user_id)
        if slot_time_str:
            await update.message.reply_text(
                f"❌ У вас уже есть активная запись на {slot_time_str}.\nЧтобы изменить её, используйте «🔁 Перенести запись» или «❌ Отменить запись».",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        # Просят имя
        context.user_data["step"] = "name"
        await update.message.reply_text("Введите ваше имя (для записи):", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
        return

    # Получаем имя
    if context.user_data.get("step") == "name":
        name = text
        # простая валидация
        if len(name) < 2 or any(ch.isdigit() for ch in name):
            await update.message.reply_text("Пожалуйста, введите корректное имя (без цифр).")
            return

        context.user_data["name"] = name
        context.user_data["step"] = "choose_slot"

        free = list_free_slots()
        if not free:
            context.user_data.clear()
            await update.message.reply_text("❌ Нет свободных слотов.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        # Показываем свободные слоты
        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup([[s] for s in free] + [["Отмена"]], resize_keyboard=True)
        )
        return

    # Выбираем слот (создаём PENDING запрос)
    if context.user_data.get("step") == "choose_slot":
        slot_chosen = text
        name = context.user_data.get("name")
        if not name:
            context.user_data.clear()
            await update.message.reply_text("Ошибка состояния. Попробуйте заново.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        # Найдём строку с этим слотом
        try:
            cell = sheet.find(slot_chosen)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        # Проверим, действительно ли свободен (имя пусто и статус пуст)
        current_name = sheet.cell(cell.row, 3).value or ""
        current_status = sheet.cell(cell.row, 6).value or ""
        if current_name.strip() != "" or current_status.strip() != "":
            await update.message.reply_text("❌ К сожалению, этот слот уже занят. Выберите другой.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        # Запишем данные в таблицу как pending (Ожидает подтверждения)
        try:
            sheet.update_cell(cell.row, 3, name)  # name -> C
            sheet.update_cell(cell.row, 4, username)  # username -> D
            sheet.update_cell(cell.row, 5, str(user_id))  # user_id -> E
            sheet.update_cell(cell.row, 6, "Ожидает подтверждения")  # status -> F
            sheet.update_cell(cell.row, 7, "0")  # transfers
            sheet.update_cell(cell.row, 8, "0")  # reminder
            sheet.update_cell(cell.row, 9, "")   # email
            sheet.update_cell(cell.row, 10, "")  # meet_link
        except Exception as e:
            await update.message.reply_text(f"Ошибка при записи в таблицу: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        # Уведомляем клиента
        await update.message.reply_text(
            "📩 Запрос на запись отправлен администратору. Пожалуйста, ожидайте подтверждения.\n"
            "Запись подтверждается окончательно после оплаты.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        # Уведомляем админа с inline кнопками
        try:
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm:{cell.row}:{user_id}"),
                    InlineKeyboardButton("❌ Отказать", callback_data=f"reject:{cell.row}:{user_id}")
                ]
            ])
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🆕 Новый запрос на консультацию\n"
                    f"👤 Имя: {name}\n"
                    f"🗓 Слот: {slot_chosen}\n"
                    f"🧑‍💻 Пользователь: {username} ({user_id})\n\n"
                    "Подтвердите или отклоните запрос:"
                ),
                reply_markup=keyboard
            )
        except Exception:
            await update.message.reply_text("Не удалось отправить уведомление админу. Свяжитесь с администратором напрямую.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

        # очищаем local user state (далее админ подтвердит и поставит user_data для meet)
        context.user_data.clear()
        return

    # === Перенос записи (сохраняем поведение) ===
    if text == "🔁 Перенести запись":
        row, slot_time_str, row_index = find_user_future_booking_row(user_id)
        if not row:
            await update.message.reply_text("❌ У вас нет активной записи для переноса.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        free = list_free_slots()
        if not free:
            await update.message.reply_text("❌ Нет свободных слотов для переноса.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        context.user_data["step"] = "reschedule"
        context.user_data["old_slot_row"] = row_index
        context.user_data["old_slot_time"] = slot_time_str
        await update.message.reply_text(
            f"Ваша текущая запись: {slot_time_str}\nВыберите новый слот:",
            reply_markup=ReplyKeyboardMarkup([[s] for s in free] + [["Отмена"]], resize_keyboard=True)
        )
        return

    if context.user_data.get("step") == "reschedule":
        # переносим как раньше
        old_row_index = context.user_data.get("old_slot_row")
        old_slot_time = context.user_data.get("old_slot_time")
        try:
            new_cell = sheet.find(text)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Выбранный слот не найден.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        if sheet.cell(new_cell.row, 3).value.strip() != "" or sheet.cell(new_cell.row, 6).value.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят. Выберите другой.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.user_data.clear()
            return

        # читаем старые данные
        old_row = sheet.row_values(old_row_index)
        name = old_row[2] if len(old_row) > 2 else ""
        uname = old_row[3] if len(old_row) > 3 else ""
        uid = old_row[4] if len(old_row) > 4 else ""
        transfers = int(old_row[6]) if len(old_row) > 6 and old_row[6].isdigit() else 0
        transfers += 1

        # очистим старую запись
        for col in range(3, 11):
            sheet.update_cell(old_row_index, col, "")

        # запишем новую
        sheet.update_cell(new_cell.row, 3, name)
        sheet.update_cell(new_cell.row, 4, uname)
        sheet.update_cell(new_cell.row, 5, uid)
        sheet.update_cell(new_cell.row, 6, "Подтверждено")  # перенос означает, что раньше был подтверждён
        sheet.update_cell(new_cell.row, 7, str(transfers))
        sheet.update_cell(new_cell.row, 8, "0")

        await update.message.reply_text(f"✅ Ваша запись перенесена на {text}.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

        # уведомим админа
        await context.bot.send_message(
            ADMIN_ID,
            f"🔁 Перенос записи\n👤 {name}\n📅 С {old_slot_time} → {text}\n🧑‍💻 {username} ({user_id})"
        )

        context.user_data.clear()
        return

    # === Отмена записи ===
    if text == "❌ Отменить запись":
        row, slot_time_str, row_index = find_user_future_booking_row(user_id)
        if not row:
            await update.message.reply_text("❌ У вас нет активной записи для отмены.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        name = row[2] if len(row) > 2 else ""
        uname = row[3] if len(row) > 3 else ""
        try:
            for col in range(3, 11):
                sheet.update_cell(row_index, col, "")
            await update.message.reply_text(f"✅ Ваша запись на {slot_time_str} отменена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

            # уведомим админа
            await context.bot.send_message(
                ADMIN_ID,
                f"❌ Отмена записи\n👤 {name}\n📅 {slot_time_str}\n🧑‍💻 {uname} ({user_id})"
            )
        except Exception as e:
            await update.message.reply_text(f"Ошибка при отмене: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    # === Моя запись ===
    if text == "📋 Моя запись":
        row, slot_time_str, row_index = find_user_future_booking_row(user_id)
        if not row:
            await update.message.reply_text("ℹ️ У вас нет активных записей.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        name = row[2]
        transfers = row[6] if len(row) > 6 else "0"
        meet_link = row[9] if len(row) > 9 else ""
        status = row[5] if len(row) > 5 else ""

        msg = f"📋 Ваша текущая запись:\n\n🗓 {slot_time_str}\n👤 {name}\n🔁 Переносов: {transfers}\nСтатус: {status}"
        if meet_link:
            msg += f"\n🔗 Ссылка: {meet_link}"

        await update.message.reply_text(
            msg,
            reply_markup=ReplyKeyboardMarkup([["🔁 Перенести запись", "❌ Отменить запись"], ["Отмена"]], resize_keyboard=True)
        )
        return

    # === Опции Meet после подтверждения админом ===
    # Здесь мы поддерживаем сценарий: админ подтвердил и поставил user_data для пользователя.
    if context.application.user_data.get(user_id, {}).get("step") == "meet_option":
        # Пользователь отвечает "Сейчас" или "Перед встречей"
        row_index = context.application.user_data[user_id].get("slot_row")
        if not row_index:
            # сбой состояния
            context.application.user_data.pop(user_id, None)
            await update.message.reply_text("Ошибка состояния. Свяжитесь с администратором.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        if text == "Сейчас":
            # запрашиваем email
            context.application.user_data[user_id]["step"] = "get_email"
            await update.message.reply_text("Введите вашу электронную почту для отправки ссылки:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True))
            return
        elif text == "Перед встречей":
            # пометим pending — отправим meet автоматически за 15 минут (должна быть фоновая задача или cron)
            try:
                sheet.update_cell(row_index, 10, "pending")  # meet_link column J set to pending flag
                await update.message.reply_text("✅ Отлично, ссылка будет выслана перед встречей.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            except Exception as e:
                await update.message.reply_text(f"Ошибка при пометке встречи: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            context.application.user_data.pop(user_id, None)
            return
        else:
            await update.message.reply_text("Выберите: Сразу (Сейчас) или Перед встречей.")
            return

    # === Получаем email после подтверждения ===
    if context.application.user_data.get(user_id, {}).get("step") == "get_email":
        row_index = context.application.user_data[user_id].get("slot_row")
        email = text.strip()
        # простая проверка e-mail
        if "@" not in email or "." not in email:
            await update.message.reply_text("Введите корректный e-mail.")
            return

        # Получаем время слота
        slot_time_str = sheet.cell(row_index, 2).value
        slot_time = parse_slot_time(slot_time_str)
        if not slot_time:
            await update.message.reply_text("Ошибка формата слота.")
            context.application.user_data.pop(user_id, None)
            return

        # Создаём событие в календаре с Meet
        try:
            event = {
                "summary": "Консультация Migrall",
                "description": "Консультация по переезду.",
                "start": {"dateTime": slot_time.isoformat(), "timeZone": "Europe/Lisbon"},
                "end": {"dateTime": (slot_time + datetime.timedelta(hours=1)).isoformat(), "timeZone": "Europe/Lisbon"},
                "attendees": [{"email": email}],
                "conferenceData": {
                    "createRequest": {
                        "requestId": f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}",
                        "conferenceSolutionKey": {"type": "hangoutsMeet"},
                    }
                },
            }
            created_event = calendar_service.events().insert(
                calendarId=CALENDAR_ID,
                body=event,
                conferenceDataVersion=1
            ).execute()

            meet_link = created_event.get("hangoutLink", "Ссылка не доступна")
            sheet.update_cell(row_index, 9, email)
            sheet.update_cell(row_index, 10, meet_link)

            await update.message.reply_text(f"✅ Ссылка на Google Meet выслана на {email}:\n{meet_link}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        except Exception as e:
            await update.message.reply_text(f"Ошибка при создании Meet: {e}", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))

        context.application.user_data.pop(user_id, None)
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            """Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸 

🔹 Что разберем:
✅ Анализ кейса
✅ Варианты легализации
✅ Пошаговый план
✅ Ответы на вопросы

💰 Стоимость: 120 €
⏳ Длительность: 1 час

📩 Пишите в @migrallpt""",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # Если ничего не подошло
    await update.message.reply_text("Не понял 🤔. Попробуйте снова.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))


# =======================
# Фоновая задача: напоминания и отправка meet по флагу 'pending'
# =======================
async def background_jobs(app: Application):
    """
    Запускает два действия:
    - проверяет слоты с meet_link == "pending" и отправляет Meet за 15 минут
    - отправляет напоминание за 24 часа (если нужен)
    """
    while True:
        try:
            all_slots = sheet.get_all_values()[1:]
            now_dt = datetime.datetime.now()
            for row in all_slots:
                slot_time_str = row[1].strip() if len(row) > 1 else ""
                if not slot_time_str:
                    continue
                slot_dt = parse_slot_time(slot_time_str)
                if not slot_dt:
                    continue

                user_id = row[4].strip() if len(row) > 4 else ""
                meet_field = row[9].strip() if len(row) > 9 else ""
                reminded = row[7].strip() if len(row) > 7 else "0"

                # Напоминание за 24 часа
                if user_id and reminded == "0" and 0 < (slot_dt - now_dt).total_seconds() <= 86400:
                    try:
                        await app.bot.send_message(int(user_id), f"⏰ Напоминаем! У вас консультация {slot_time_str}.")
                        cell = sheet.find(slot_time_str)
                        sheet.update_cell(cell.row, 8, "1")  # колонка H = 8
                    except Exception:
                        pass

                # Отправка Meet за 15 минут, если meet_field == 'pending'
                if user_id and meet_field == "pending" and 0 < (slot_dt - now_dt).total_seconds() <= 900:
                    email = row[8].strip() if len(row) > 8 else None
                    if email:
                        try:
                            event = {
                                "summary": "Консультация Migrall",
                                "description": "Консультация по переезду.",
                                "start": {"dateTime": slot_dt.isoformat(), "timeZone": "Europe/Lisbon"},
                                "end": {"dateTime": (slot_dt + datetime.timedelta(hours=1)).isoformat(), "timeZone": "Europe/Lisbon"},
                                "attendees": [{"email": email}],
                                "conferenceData": {
                                    "createRequest": {
                                        "requestId": f"migrall-{user_id}-{int(datetime.datetime.now().timestamp())}",
                                        "conferenceSolutionKey": {"type": "hangoutsMeet"},
                                    }
                                },
                            }
                            created_event = calendar_service.events().insert(
                                calendarId=CALENDAR_ID,
                                body=event,
                                conferenceDataVersion=1
                            ).execute()

                            meet_link = created_event.get("hangoutLink", "Ссылка не доступна")
                            cell = sheet.find(slot_time_str)
                            sheet.update_cell(cell.row, 10, meet_link)  # J column -> link
                            await app.bot.send_message(int(user_id), f"✅ Ссылка на Google Meet:\n{meet_link}")
                        except Exception:
                            pass
        except Exception:
            pass

        await asyncio.sleep(60)


# =======================
# Запуск приложения
# =======================
def main():
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # фоновая задача
    application.job_queue.run_repeating(lambda ctx: asyncio.create_task(background_jobs(application)), interval=60, first=5)

    # Запуск webhook'а (если нужно)
    # Если deploy на render/Heroku — используется webhook. Для локального теста можно запускать polling.
    async def _run():
        await application.initialize()
        await application.start()
        try:
            # если у тебя webhook настроен, используй start_webhook. Иначе можно запустить polling.
            await application.updater.start_webhook(
                listen="0.0.0.0",
                port=PORT,
                url_path="webhook",
                webhook_url=WEBHOOK_URL,
            )
        except Exception:
            # fallback to polling если webhook не настроен / в локальной среде
            await application.updater.start_polling()
        await application.updater.idle()

    import asyncio as _asyncio
    _asyncio.run(_run())

if __name__ == "__main__":
    main()
