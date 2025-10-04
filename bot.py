import os
import json
import base64
import asyncio
import datetime
import smtplib
from email.mime.text import MIMEText
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

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

sheets_scope = ["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(sheets_creds_dict, sheets_scope)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# =======================
# Google Calendar (Meet)
# =======================
calendar_creds_json = base64.b64decode(os.environ["GOOGLE_CALENDAR_CREDS"])
calendar_creds_dict = json.loads(calendar_creds_json)

calendar_scopes = ['https://www.googleapis.com/auth/calendar']
calendar_credentials = Credentials.from_service_account_info(calendar_creds_dict, scopes=calendar_scopes)
calendar_service = build('calendar', 'v3', credentials=calendar_credentials)
CALENDAR_ID = 'migrallportugal@gmail.com'

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
            if str(user_id) in row:  # уже есть запись
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
        free_slots = [row[1].strip() for row in all_slots if row[2].strip() == ""]  # колонка B = слот, C = имя
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

        # Записываем данные
        sheet.update_cell(cell.row, 3, name)         # имя
        sheet.update_cell(cell.row, 4, username)     # username
        sheet.update_cell(cell.row, 5, str(user_id)) # user_id
        sheet.update_cell(cell.row, 6, "Консультация") # услуга
        sheet.update_cell(cell.row, 7, "0")          # переносы
        sheet.update_cell(cell.row, 8, "0")          # напоминание
        sheet.update_cell(cell.row, 9, "")           # email для ссылки
        sheet.update_cell(cell.row, 10, "")          # meet_link

        context.user_data["slot_row"] = cell.row

        # Спрашиваем пр
