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
sheets_scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
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
        free_slots = [row[1].strip() for row in all_slots if len(row) > 2 and row[2].strip() == ""]

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
        sheet.update_cell(cell.row, 3, name)  # имя
        sheet.update_cell(cell.row, 4, username)  # username
        sheet.update_cell(cell.row, 5, str(user_id))  # user_id
        sheet.update_cell(cell.row, 6, "Консультация")  # услуга
        sheet.update_cell(cell.row, 7, "0")  # переносы
        sheet.update_cell(cell.row, 8, "0")  # напоминание
        sheet.update_cell(cell.row, 9, "")  # email
        sheet.update_cell(cell.row, 10, "")  # meet_link

        context.user_data["slot_row"] = cell.row

        await update.message.reply_text(
            "✅ Вы успешно записаны на консультацию!\n"
            "Хотите получить ссылку на встречу сейчас или перед консультацией?",
            reply_markup=ReplyKeyboardMarkup([["Сейчас", "Перед встречей"]], resize_keyboard=True)
        )
        context.user_data["step"] = "meet_option"
        return

    # === Выбор Meet ===
    if context.user_data.get("step") == "meet_option":
        row = context.user_data["slot_row"]
        slot_time_str = sheet.cell(row, 2).value.strip()

        # Проверяем формат даты
        try:
            slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
        except ValueError:
            await update.message.reply_text("❌ Ошибка формата даты в таблице. Свяжитесь с администратором.")
            context.user_data.clear()
            return

        if text == "Сейчас":
            # создаем событие без email, сразу выдаем ссылку
            event = {
                'summary': 'Консультация Migrall',
                'description': 'Консультация по переезду.',
                'start': {'dateTime': slot_time.isoformat(), 'timeZone': 'Europe/Lisbon'},
                'end': {'dateTime': (slot_time + datetime.timedelta(hours=1)).isoformat(), 'timeZone': 'Europe/Lisbon'},
                'conferenceData': {
                    'createRequest': {
                        'requestId': f'migrall-{user_id}',
                        'conferenceSolutionKey': {'type': 'hangoutsMeet'}
                    }
                },
            }
            try:
                created_event = calendar_service.events().insert(
                    calendarId=CALENDAR_ID,
                    body=event,
                    conferenceDataVersion=1
                ).execute()
                meet_link = created_event.get('hangoutLink', '')
                sheet.update_cell(row, 10, meet_link)
                await update.message.reply_text(
                    f"✅ Ваша запись подтверждена!\n"
                    f"Ссылка на Google Meet:\n{meet_link}\n\n"
                    f"🕐 Мы также напомним вам о консультации за день до начала."
                )
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка при создании встречи: {e}")

            context.user_data.clear()
            return

        elif text == "Перед встречей":
            await update.message.reply_text(
                "✅ Отлично, ссылка будет выслана за 15 минут до консультации.\n"
                "🕐 Мы также напомним вам о встрече за день до начала."
            )
            sheet.update_cell(row, 10, "pending")
            context.user_data.clear()
            return

        else:
            await update.message.reply_text("Выберите вариант: Сейчас или Перед встречей.")
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
# Фоновая задача: напоминания и Meet перед встречей
# =======================
async def background_jobs(app: Application):
    while True:
        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()
        for row in all_slots:
            slot_time_str = row[1].strip() if len(row) > 1 else ""
            user_id = row[4].strip() if len(row) > 4 else ""
            meet_status = row[10].strip() if len(row) > 10 else ""
            reminded = row[8].strip() if len(row) > 8 else "0"

            if not slot_time_str or not user_id:
                continue

            try:
                slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
            except ValueError:
                continue

            # Напоминание за 24 часа
            if reminded == "0" and 0 < (slot_time - now).total_seconds() <= 86400:
                try:
                    await app.bot.send_message(int(user_id), f"⏰ Напоминаем! У вас консультация {slot_time_str}.")
                    cell = sheet.find(slot_time_str)
                    sheet.update_cell(cell.row, 8, "1")
                except:
                    pass

            # Отправка Meet за 15 минут, если выбрали "Перед встречей"
            if meet_status == "pending" and 0 < (slot_time - now).total_seconds() <= 900:
                try:
                    event = {
                        'summary': 'Консультация Migrall',
                        'description': 'Консультация по переезду.',
                        'start': {'dateTime': slot_time.isoformat(), 'timeZone': 'Europe/Lisbon'},
                        'end': {'dateTime': (slot_time + datetime.timedelta(hours=1)).isoformat(), 'timeZone': 'Europe/Lisbon'},
                        'conferenceData': {
                            'createRequest': {
                                'requestId': f'migrall-{user_id}',
                                'conferenceSolutionKey': {'type': 'hangoutsMeet'}
                            }
                        },
                    }
                    created_event = calendar_service.events().insert(
                        calendarId=CALENDAR_ID,
                        body=event,
                        conferenceDataVersion=1
                    ).execute()
                    meet_link = created_event.get('hangoutLink', '')
                    cell = sheet.find(slot_time_str)
                    sheet.update_cell(cell.row, 10, meet_link)
                    await app.bot.send_message(int(user_id), f"✅ Ссылка на Google Meet:\n{meet_link}")
                except:
                    pass

        await asyncio.sleep(60)

# =======================
# Создаём приложение
# =======================
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

# =======================
# Асинхронный запуск
# =======================
async def main():
    await app.bot.set_webhook(WEBHOOK_URL)
    print("Webhook установлен:", WEBHOOK_URL)
    await app.initialize()
    await app.start()
    await app.updater.start_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )
    asyncio.create_task(background_jobs(app))
    await asyncio.Event().wait()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
