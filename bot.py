import os
import json
import base64
import asyncio
import datetime
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
# Вспомогательная функция
# =======================
def find_user_booking(user_id: int):
    """Возвращает (row, slot_time_str) если есть запись в будущем, иначе None"""
    all_slots = sheet.get_all_values()[1:]
    now = datetime.datetime.now()
    for row in all_slots:
        if len(row) < 5:
            continue
        slot_time_str = row[1].strip()
        booked_user_id = row[4].strip()
        if booked_user_id == str(user_id):
            try:
                slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
                if slot_time > now:
                    return row, slot_time_str
            except ValueError:
                continue
    return None, None

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
    username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"

    # === Отмена любого шага ===
    if text == "Отмена":
        context.user_data.clear()
        await update.message.reply_text(
            "❌ Действие отменено.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Новая запись ===
    if text == "📅 Записаться на консультацию Migrall":
        _, slot_time_str = find_user_booking(user_id)
        if slot_time_str:
            await update.message.reply_text(
                f"❌ У вас уже есть активная запись на {slot_time_str}.\n"
                "Чтобы изменить её, выберите «Перенести запись» или «Отменить запись».",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        await update.message.reply_text(
            "Введите ваше имя (для записи):",
            reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True)
        )
        context.user_data["step"] = "name"
        return

    # === Получаем имя ===
    if context.user_data.get("step") == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "choose_slot"

        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()
        free_slots = []
        for row in all_slots:
            if len(row) > 2 and row[2].strip() == "":
                try:
                    slot_time_str = row[1].strip()
                    slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
                    if slot_time > now:
                        free_slots.append(slot_time_str)
                except ValueError:
                    continue

        if not free_slots:
            await update.message.reply_text(
                "❌ Нет свободных слотов на будущее.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            context.user_data.clear()
            return

        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots] + [["Отмена"]], resize_keyboard=True)
        )
        return

    # === Выбираем слот ===
    if context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text
        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден.")
            return

        if sheet.cell(cell.row, 3).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте другой.")
            return

        # Записываем данные
        sheet.update_cell(cell.row, 3, name)
        sheet.update_cell(cell.row, 4, username)
        sheet.update_cell(cell.row, 5, str(user_id))
        sheet.update_cell(cell.row, 6, "Консультация")
        sheet.update_cell(cell.row, 7, "0")
        sheet.update_cell(cell.row, 8, "0")

        await update.message.reply_text(
            f"✅ {name}, вы записаны на {slot}.\n"
            "Консультация будет проведена после оплаты.\n"
            "Для оплаты напишите в @migrallpt.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        # 🔔 Уведомление админу
        await context.bot.send_message(
            ADMIN_ID,
            f"🔔 Новая запись!\n👤 {name}\n📅 {slot}\n🧑‍💻 {username} ({user_id})"
        )

        context.user_data.clear()
        return

    # === Моя запись ===
    if text == "📋 Моя запись":
        current_row, slot_time_str = find_user_booking(user_id)
        if not current_row:
            await update.message.reply_text(
                "ℹ️ У вас нет активных записей.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        name = current_row[2]
        transfers = current_row[6]
        meet_link = current_row[9] if len(current_row) > 9 else ""

        msg = f"📋 Ваша текущая запись:\n\n🗓 Дата и время: {slot_time_str}\n👤 Имя: {name}\n🔁 Переносов: {transfers}"
        if meet_link:
            msg += f"\n🔗 Ссылка: {meet_link}"

        await update.message.reply_text(
            msg,
            reply_markup=ReplyKeyboardMarkup([["🔁 Перенести запись", "❌ Отменить запись"], ["Отмена"]], resize_keyboard=True)
        )
        return

    # === Перенос записи ===
    if text == "🔁 Перенести запись":
        current_row, slot_time_str = find_user_booking(user_id)
        if not current_row:
            await update.message.reply_text(
                "❌ У вас нет активной записи для переноса.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()
        free_slots = []
        for row in all_slots:
            if len(row) > 2 and row[2].strip() == "":
                try:
                    slot_time_str = row[1].strip()
                    slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
                    if slot_time > now:
                        free_slots.append(slot_time_str)
                except ValueError:
                    continue

        if not free_slots:
            await update.message.reply_text(
                "❌ Нет свободных слотов для переноса на будущее.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        context.user_data["step"] = "reschedule"
        context.user_data["old_slot_time"] = slot_time_str
        await update.message.reply_text(
            f"Ваша текущая запись: {slot_time_str}\nВыберите новый слот:",
            reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots] + [["Отмена"]], resize_keyboard=True)
        )
        return

    # === Обработка шага переноса ===
    if context.user_data.get("step") == "reschedule":
        old_slot_time = context.user_data["old_slot_time"]
        try:
            new_cell = sheet.find(text)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Выбранный слот не найден.")
            return

        if sheet.cell(new_cell.row, 3).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Выберите другой.")
            return

        old_cell = sheet.find(old_slot_time)
        name = sheet.cell(old_cell.row, 3).value
        username = sheet.cell(old_cell.row, 4).value
        user_id = sheet.cell(old_cell.row, 5).value
        transfers = int(sheet.cell(old_cell.row, 7).value or "0") + 1

        for col in range(3, 11):
            sheet.update_cell(old_cell.row, col, "")

        sheet.update_cell(new_cell.row, 3, name)
        sheet.update_cell(new_cell.row, 4, username)
        sheet.update_cell(new_cell.row, 5, user_id)
        sheet.update_cell(new_cell.row, 6, "Консультация")
        sheet.update_cell(new_cell.row, 7, str(transfers))
        sheet.update_cell(new_cell.row, 8, "0")

        await update.message.reply_text(
            f"✅ Ваша запись перенесена на {text}.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        # 🔔 Уведомление админу
        await context.bot.send_message(
            ADMIN_ID,
            f"🔁 Перенос записи\n👤 {name}\n📅 С {old_slot_time} → {text}\n🧑‍💻 {username} ({user_id})"
        )

        context.user_data.clear()
        return

    # === Отмена записи ===
    if text == "❌ Отменить запись":
        current_row, slot_time_str = find_user_booking(user_id)
        if not current_row:
            await update.message.reply_text(
                "❌ У вас нет активной записи для отмены.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        try:
            cell = sheet.find(slot_time_str)
            name = sheet.cell(cell.row, 3).value
            username = sheet.cell(cell.row, 4).value
            for col in range(3, 11):
                sheet.update_cell(cell.row, col, "")
            await update.message.reply_text(
                f"✅ Ваша запись на {slot_time_str} отменена.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )

            # 🔔 Уведомление админу
            await context.bot.send_message(
                ADMIN_ID,
                f"❌ Отмена записи\n👤 {name}\n📅 {slot_time_str}\n🧑‍💻 {username} ({user_id})"
            )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Ошибка при отмене записи: {e}",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            """Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸 

🔹 Что разберем:
✅ Ваш кейс
✅ Варианты легализации
✅ Пошаговый план
✅ Ответы на вопросы

💰 Стоимость: 120 €
⏳ Длительность: 1 час

📩 Пишите в @migrallpt — поможем!""",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Если ничего не подошло ===
    await update.message.reply_text(
        "Не понял 🤔. Попробуйте снова.",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

# =======================
# Приложение
# =======================
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

# =======================
# Запуск
# =======================
async def main():
    await app.bot.set_webhook(WEBHOOK_URL)
    await app.initialize()
    await app.start()
    await app.updater.start_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )
    await asyncio.Event().wait()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
