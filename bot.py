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
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)

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
    "https://www.googleapis.com/auth/drive",
]
sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(
    sheets_creds_dict, sheets_scope
)
sheets_client = gspread.authorize(sheets_creds)
sheet = sheets_client.open("Расписание").worksheet("График")

# =======================
# Google Calendar
# =======================
calendar_creds_json = base64.b64decode(os.environ["GOOGLE_CALENDAR_CREDS"])
calendar_creds_dict = json.loads(calendar_creds_json)
calendar_scopes = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.events",
]
calendar_credentials = Credentials.from_service_account_info(
    calendar_creds_dict, scopes=calendar_scopes, subject="ops@migrall.com"
)
calendar_service = build("calendar", "v3", credentials=calendar_credentials)
CALENDAR_ID = "ops@migrall.com"

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
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True),
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    user_id = user.id
    username = user.username if user.username else f"{user.first_name} {user.last_name or ''}"

    # === Начало записи ===
    if text == "📅 Записаться на консультацию Migrall":
        all_slots = sheet.get_all_values()
        now = datetime.datetime.now()

        for row in all_slots[1:]:
            if str(user_id) in row:
                try:
                    slot_time = datetime.datetime.strptime(row[1], "%d.%m.%Y, %H:%M")
                    if slot_time > now:
                        await update.message.reply_text(
                            "❌ У вас уже есть активная запись. Перенос возможен, но не новая запись."
                        )
                        return
                except Exception:
                    pass

        await update.message.reply_text("Введите ваше имя (для записи):")
        context.user_data["step"] = "name"
        return

    # === Получаем имя ===
    if context.user_data.get("step") == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "choose_slot"

        all_slots = sheet.get_all_values()[1:]
        free_slots = [row[1].strip() for row in all_slots if row[2].strip() == ""]
        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.")
            context.user_data.clear()
            return

        slot_buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup(slot_buttons, resize_keyboard=True),
        )
        return

    # === Выбор слота ===
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

        # 🕓 Временно блокируем слот, помечая статус "Ожидание подтверждения"
        sheet.update_cell(cell.row, 3, name)
        sheet.update_cell(cell.row, 4, username)
        sheet.update_cell(cell.row, 5, str(user_id))
        sheet.update_cell(cell.row, 6, "Ожидание подтверждения")

        # Уведомляем пользователя
        await update.message.reply_text(
            "📩 Запрос на запись отправлен. Ожидайте подтверждения.\n\n"
            "После оплаты администратор подтвердит встречу."
        )

        # Уведомляем администратора
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{cell.row}"),
                InlineKeyboardButton("❌ Отказать", callback_data=f"decline_{cell.row}"),
            ]
        ])
        await context.bot.send_message(
            ADMIN_ID,
            f"🆕 Новый запрос на консультацию!\n"
            f"👤 {name}\n"
            f"🗓 {slot}\n"
            f"🧑‍💻 @{username} ({user_id})",
            reply_markup=keyboard,
        )
        context.user_data.clear()
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            """Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸 

🔹 Что разберем на консультации?
✅ Анализируем именно ваш кейс
✅ Рассматриваем все возможные варианты легализации
✅ Прописываем пошаговый план действий
✅ Отвечаем на все ваши вопросы

💰 Стоимость: 120 €
⏳ Длительность: 1 час

📩 Остались вопросы? Пишите в @migrallpt — поможем!"""
        )
        return

    await update.message.reply_text("Не понял 🤔. Попробуйте снова.")

# =======================
# Обработка кнопок админа
# =======================
async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("confirm_"):
        row = int(query.data.split("_")[1])
        user_id = sheet.cell(row, 5).value
        slot_time = sheet.cell(row, 2).value
        sheet.update_cell(row, 6, "Подтверждено")

        await context.bot.send_message(
            int(user_id),
            f"✅ Ваша консультация на {slot_time} подтверждена!\n"
            "Хотите получить ссылку на Google Meet сейчас или перед встречей?",
            reply_markup=ReplyKeyboardMarkup([["Сейчас", "Перед встречей"]], resize_keyboard=True),
        )
        await query.edit_message_text("✅ Запись подтверждена.")

    elif query.data.startswith("decline_"):
        row = int(query.data.split("_")[1])
        user_id = sheet.cell(row, 5).value
        slot_time = sheet.cell(row, 2).value

        # очищаем слот
        for col in range(3, 11):
            sheet.update_cell(row, col, "")

        await context.bot.send_message(
            int(user_id),
            f"❌ К сожалению, слот {slot_time} не подтвержден.\nПожалуйста, выберите другое время.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True),
        )
        await query.edit_message_text("❌ Запись отклонена.")

# =======================
# Фоновая задача
# =======================
async def background_jobs(app: Application):
    while True:
        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()

        for row in all_slots:
            if len(row) < 9:
                continue
            slot_time_str = row[1].strip()
            user_id = row[4].strip()
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
                    await app.bot.send_message(
                        int(user_id), f"⏰ Напоминаем! У вас консультация {slot_time_str}."
                    )
                    cell = sheet.find(slot_time_str)
                    sheet.update_cell(cell.row, 8, "1")
                except:
                    pass

        await asyncio.sleep(60)

# =======================
# Основной запуск
# =======================
async def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(admin_callback))

    # Запуск фоновой задачи
    asyncio.create_task(background_jobs(app))

    # Настраиваем webhook
    await app.bot.set_webhook(WEBHOOK_URL)

    # Запускаем сервер webhook
    await app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )

if __name__ == "__main__":
    asyncio.run(main())
