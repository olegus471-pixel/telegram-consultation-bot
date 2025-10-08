import os
import json
import base64
import asyncio
import datetime
import logging

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
WEBHOOK_URL = "https://telegram-consultation-bot.onrender.com/webhook"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============= GOOGLE SHEETS =============
sheets_creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDS"])
sheets_creds_dict = json.loads(sheets_creds_json)

sheets_scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(sheets_creds_dict, sheets_scope)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# ============= GOOGLE CALENDAR =============
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

# ============= ГЛАВНОЕ МЕНЮ =============
main_menu = [
    ["📅 Записаться", "🔁 Перенос", "❌ Отмена"],
    ["ℹ️ Инфо"]
]

# ============= ХЭНДЛЕРЫ =============

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

# === Основная логика ===
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    user_id = user.id
    username = user.username if user.username else f"{user.first_name} {user.last_name or ''}"

    # === Отмена записи ===
    if text == "❌ Отмена":
        all_slots = sheet.get_all_values()[1:]
        for row in all_slots:
            if str(user_id) in row:
                slot_time = row[1]
                sheet.update_cell(all_slots.index(row)+2, 3, "")
                sheet.update_cell(all_slots.index(row)+2, 4, "")
                sheet.update_cell(all_slots.index(row)+2, 5, "")
                await update.message.reply_text(f"🗑 Ваша запись на {slot_time} отменена.")
                await start(update, context)
                return
        await update.message.reply_text("❌ У вас нет активной записи.")
        await start(update, context)
        return

    # === Перенос записи ===
    if text == "🔁 Перенос":
        await update.message.reply_text("Введите новую дату/время для переноса:")
        context.user_data["step"] = "transfer"
        return

    if context.user_data.get("step") == "transfer":
        new_slot = text
        all_slots = sheet.get_all_values()[1:]
        for row in all_slots:
            if str(user_id) in row:
                sheet.update_cell(all_slots.index(row)+2, 2, new_slot)
                await update.message.reply_text(f"✅ Запись перенесена на {new_slot}.")
                await start(update, context)
                return
        await update.message.reply_text("❌ У вас нет записи для переноса.")
        await start(update, context)
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            "ℹ️ Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸\n\n"
            "Стоимость: 120 € (может быть добавлен НДС 23%)\n"
            "Длительность: 1 час\n\n"
            "Чтобы записаться — выберите 📅 Записаться.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Запись ===
    if text == "📅 Записаться":
        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()
        for row in all_slots:
            if str(user_id) in row:
                slot_time_str = row[1]
                try:
                    slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
                    if slot_time > now:
                        await update.message.reply_text("❌ У вас уже есть активная запись.")
                        return
                except:
                    continue

        free_slots = [r[1].strip() for r in all_slots if r[2].strip() == ""]
        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.")
            return

        slot_buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup(slot_buttons, resize_keyboard=True)
        )
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text
        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.")
            return

        if sheet.cell(cell.row, 3).value.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.")
            return

        # === Отправляем запрос админу ===
        sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
        sheet.update_cell(cell.row, 4, username)
        sheet.update_cell(cell.row, 5, str(user_id))

        await update.message.reply_text(
            "📨 Запрос на запись отправлен! ⏳\nОжидайте подтверждения администратора.\n\n"
            "Консультация проводится после оплаты. 💶"
        )

        # === Уведомляем админа ===
        await context.bot.send_message(
            ADMIN_ID,
            f"📩 Пользователь @{username} ({user_id}) хочет записаться на {slot}.\n\n"
            f"Подтвердить или отказать?",
            reply_markup=ReplyKeyboardMarkup(
                [[f"✅ Подтвердить {user_id} {cell.row}", f"❌ Отказать {user_id} {cell.row}"]],
                resize_keyboard=True
            )
        )

        context.user_data.clear()
        return

    # === Подтверждение / отказ от админа ===
    if text.startswith("✅ Подтвердить"):
        _, uid, row = text.split()
        uid, row = int(uid), int(row)
        slot_time_str = sheet.cell(row, 2).value

        sheet.update_cell(row, 3, "Подтверждено")

        await context.bot.send_message(uid, f"✅ Ваша запись на {slot_time_str} подтверждена!")
        await update.message.reply_text(f"✅ Подтверждено: пользователь {uid}, слот {slot_time_str}")
        return

    if text.startswith("❌ Отказать"):
        _, uid, row = text.split()
        uid, row = int(uid), int(row)
        slot_time_str = sheet.cell(row, 2).value

        sheet.update_cell(row, 3, "")

        await context.bot.send_message(uid, f"❌ Слот {slot_time_str} не подтверждён.")
        await update.message.reply_text(f"❌ Отказано пользователю {uid}, слот {slot_time_str}")
        return

    await update.message.reply_text("🤔 Не понял команду.")
    await start(update, context)

# ============= ФОНОВАЯ ЗАДАЧА =============
async def background_jobs(app: Application):
    while True:
        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()

        for row in all_slots:
            if len(row) < 9:
                continue

            slot_time_str = row[1].strip()
            user_id = row[4].strip()
            reminded = row[8].strip() if len(row) > 8 else "0"

            if not slot_time_str or not user_id:
                continue

            try:
                slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
            except ValueError:
                continue

            # напоминание за 24 часа
            if reminded == "0" and 0 < (slot_time - now).total_seconds() <= 86400:
                try:
                    await app.bot.send_message(int(user_id), f"⏰ Напоминаем: у вас консультация {slot_time_str}.")
                    cell = sheet.find(slot_time_str)
                    sheet.update_cell(cell.row, 8, "1")
                except:
                    pass

        await asyncio.sleep(60)

# ============= ЗАПУСК БОТА =============
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Фоновая задача
    job_queue = app.job_queue
    job_queue.run_repeating(lambda _: asyncio.create_task(background_jobs(app)), interval=60, first=5)

    logger.info("🚀 Бот запущен в режиме webhook")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    main()
