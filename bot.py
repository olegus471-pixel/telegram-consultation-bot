import os
import json
import base64
import asyncio
import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =======================
# Переменные окружения
# =======================
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = "https://telegram-consultation-bot.onrender.com/webhook"

# =======================
# Google Sheets (Расписание)
# =======================
sheets_creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDS"])
sheets_creds_dict = json.loads(sheets_creds_json)
sheets_creds = gspread.service_account_from_dict(sheets_creds_dict)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# =======================
# Google Calendar (ops@migrall.com)
# =======================
calendar_creds_json = base64.b64decode(os.environ["GOOGLE_CALENDAR_CREDS"])
calendar_creds_dict = json.loads(calendar_creds_json)

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_EMAIL = 'ops@migrall.com'  # Workspace аккаунт
credentials = service_account.Credentials.from_service_account_info(
    calendar_creds_dict, scopes=SCOPES
)
delegated_credentials = credentials.with_subject(SERVICE_ACCOUNT_EMAIL)
calendar_service = build('calendar', 'v3', credentials=delegated_credentials)

# =======================
# Главное меню
# =======================
main_menu = [["📅 Записаться на консультацию Migrall", "ℹ️ Инфо"]]

# =======================
# Хэндлеры
# =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    user_id = user.id
    username = user.username if user.username else f"{user.first_name} {user.last_name or ''}"

    # === Начало записи ===
    if text == "📅 Записаться на консультацию Migrall":
        all_slots = sheet.get_all_values()
        for row in all_slots[1:]:
            if str(user_id) in row:
                await update.message.reply_text("❌ У вас уже есть активная запись. Перенос возможен, но не новая запись.")
                return

        await update.message.reply_text("Введите ваше имя (для записи):")
        context.user_data["step"] = "name"
        return

    # === Получаем имя ===
    if context.user_data.get("step") == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "choose_slot"

        all_slots = sheet.get_all_values()[1:]
        free_slots = [row[1].strip() for row in all_slots if row[2].strip() == ""]  # B = слот, C = имя
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

    # === Выбираем слот ===
    if context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text
        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.")
            return

        if sheet.cell(cell.row, 3).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте снова.")
            return

        # Записываем данные в Google Sheets
        sheet.update_cell(cell.row, 3, name)        # C - имя
        sheet.update_cell(cell.row, 4, username)    # D - username
        sheet.update_cell(cell.row, 5, str(user_id)) # E - user_id
        sheet.update_cell(cell.row, 6, "Консультация") # F - услуга
        sheet.update_cell(cell.row, 7, "0")         # G - переносы
        sheet.update_cell(cell.row, 8, "0")         # H - напоминание
        sheet.update_cell(cell.row, 9, "")          # I - email
        sheet.update_cell(cell.row, 10, "")         # J - meet_link

        context.user_data["slot_row"] = cell.row

        # Спрашиваем про ссылку Meet
        await update.message.reply_text(
            "Хотите, чтобы ссылка на Google Meet была выслана прямо сейчас или перед встречей?",
            reply_markup=ReplyKeyboardMarkup([["Сейчас", "Перед встречей"]], resize_keyboard=True)
        )
        context.user_data["step"] = "meet_option"
        return

    # === Выбор Meet ===
    if context.user_data.get("step") == "meet_option":
        row = context.user_data["slot_row"]
        if text == "Сейчас":
            await update.message.reply_text("Введите вашу электронную почту для отправки ссылки:")
            context.user_data["step"] = "get_email"
            return
        elif text == "Перед встречей":
            await update.message.reply_text("✅ Отлично, ссылка будет выслана перед встречей.")
            sheet.update_cell(row, 10, "pending")  # отметка для Meet ссылки позже
            context.user_data.clear()
            return
        else:
            await update.message.reply_text("Выберите вариант: Сейчас или Перед встречей.")
            return

    # === Получаем email и создаём Meet ===
    if context.user_data.get("step") == "get_email":
        email = text.strip()
        row = context.user_data["slot_row"]
        slot_time_str = sheet.cell(row, 2).value.strip()

        # Защита от неправильного формата даты
        try:
            slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
        except ValueError:
            await update.message.reply_text("❌ Ошибка: неверный формат даты в таблице.")
            context.user_data.clear()
            return

        # создаем событие в календаре
        event = {
            'summary': 'Консультация Migrall',
            'description': 'Консультация по переезду',
            'start': {'dateTime': slot_time.isoformat(), 'timeZone': 'Europe/Lisbon'},
            'end': {'dateTime': (slot_time + datetime.timedelta(hours=1)).isoformat(), 'timeZone': 'Europe/Lisbon'},
            'attendees': [{'email': email}],
            'conferenceData': {
                'createRequest': {
                    'requestId': f'unique-{user_id}',
                    'conferenceSolutionKey': {'type': 'hangoutsMeet'}
                }
            }
        }

        try:
            created_event = calendar_service.events().insert(
                calendarId=SERVICE_ACCOUNT_EMAIL,
                body=event,
                conferenceDataVersion=1
            ).execute()
            meet_link = created_event['conferenceData']['entryPoints'][0]['uri']
            sheet.update_cell(row, 9, email)
            sheet.update_cell(row, 10, meet_link)
            await update.message.reply_text(f"✅ Ссылка на Google Meet выслана на {email}:\n{meet_link}")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка при создании встречи: {e}")

        context.user_data.clear()
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            """Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸 

🔹 Что разберем на консультации?
✅ Анализируем ваш кейс
✅ Рассматриваем варианты легализации
✅ Прописываем пошаговый план
✅ Отвечаем на все вопросы

💰 Стоимость: 120 €
⏳ Длительность: 1 час

*К сумме может быть добавлен НДС 23%"""
        )
        return

    await update.message.reply_text("Не понял 🤔. Попробуйте снова.")

# =======================
# Фоновые задачи: напоминания и Meet-ссыл
