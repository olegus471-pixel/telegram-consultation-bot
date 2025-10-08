import os
import json
import base64
import logging

from oauth2client.service_account import ServiceAccountCredentials
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
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

# ============= GOOGLE CALENDAR (если нужно) =============
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
    ["📅 Записаться", "📖 Моя запись"],
    ["🔁 Перенос", "❌ Отмена"],
    ["ℹ️ Инфо"]
]


# ============= /start =============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "👋 Привет! Я бот для записи на консультацию Migrall.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )


# ============= ОСНОВНАЯ ЛОГИКА =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    user_id = str(user.id)
    username = f"@{user.username}" if user.username else "—"

    # === Моя запись ===
    if text == "📖 Моя запись":
        all_slots = sheet.get_all_values()[1:]
        for row in all_slots:
            if row[5] == user_id:
                day, slot, status = row[0], row[1], row[2]
                await update.message.reply_text(f"📅 {day} — {slot}\nСтатус: {status}")
                await start(update, context)
                return
        await update.message.reply_text("ℹ️ У вас нет активной записи.")
        await start(update, context)
        return

    # === Отмена записи ===
    if text == "❌ Отмена":
        all_slots = sheet.get_all_values()[1:]
        for i, row in enumerate(all_slots, start=2):
            if row[5] == user_id:
                slot_time = row[1]
                for col in range(3, 7):
                    sheet.update_cell(i, col, "")
                await update.message.reply_text(f"🗑 Ваша запись на {slot_time} отменена.")
                await start(update, context)
                return
        await update.message.reply_text("❌ У вас нет активной записи.")
        await start(update, context)
        return

    # === Перенос ===
    if text == "🔁 Перенос":
        all_slots = sheet.get_all_values()[1:]
        user_row = next((r for r in all_slots if r[5] == user_id), None)
        if not user_row:
            await update.message.reply_text("❌ У вас нет записи для переноса.")
            await start(update, context)
            return
        free_slots = [r[1] for r in all_slots if r[2].strip() == ""]
        if not free_slots:
            await update.message.reply_text("❌ Нет доступных слотов для переноса.")
            await start(update, context)
            return
        buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            "Выберите новое время для переноса:",
            reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True)
        )
        context.user_data["step"] = "transfer"
        return

    # === Перенос (шаг 2) ===
    if context.user_data.get("step") == "transfer":
        new_slot = text
        try:
            cell = sheet.find(new_slot)
        except:
            await update.message.reply_text("❌ Такой слот не найден.")
            await start(update, context)
            return

        all_slots = sheet.get_all_values()[1:]
        user_row_index = None
        for i, row in enumerate(all_slots, start=2):
            if row[5] == user_id:
                user_row_index = i
                break

        if not user_row_index:
            await update.message.reply_text("❌ У вас нет записи для переноса.")
            await start(update, context)
            return

        if sheet.cell(cell.row, 3).value.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.")
            await start(update, context)
            return

        # перенос
        sheet.update_cell(cell.row, 3, "Подтверждено (перенос)")
        sheet.update_cell(cell.row, 4, sheet.cell(user_row_index, 4).value)
        sheet.update_cell(cell.row, 5, sheet.cell(user_row_index, 5).value)
        sheet.update_cell(cell.row, 6, sheet.cell(user_row_index, 6).value)

        # очистка старого
        for col in range(3, 7):
            sheet.update_cell(user_row_index, col, "")

        await update.message.reply_text(f"✅ Запись перенесена на {new_slot}.")
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
        context.user_data["step"] = "ask_name"
        await update.message.reply_text("✏️ Введите, пожалуйста, ваше имя и фамилию:")
        return

    # === Получаем имя ===
    if context.user_data.get("step") == "ask_name":
        context.user_data["full_name"] = text
        all_slots = sheet.get_all_values()[1:]
        free_slots = [r[1] for r in all_slots if r[2].strip() == ""]
        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.")
            await start(update, context)
            return
        buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            f"Спасибо, {text}! Теперь выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True)
        )
        context.user_data["step"] = "choose_slot"
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        slot = text
        try:
            cell = sheet.find(slot)
        except:
            await update.message.reply_text("❌ Слот не найден.")
            await start(update, context)
            return

        if sheet.cell(cell.row, 3).value.strip() != "":
            await update.message.reply_text("❌ Этот слот уже занят.")
            await start(update, context)
            return

        full_name = context.user_data.get("full_name")

        # Запись в таблицу
        sheet.update_cell(cell.row, 3, "Ожидает подтверждения")
        sheet.update_cell(cell.row, 4, full_name)
        sheet.update_cell(cell.row, 5, username)
        sheet.update_cell(cell.row, 6, user_id)

        await update.message.reply_text(
            "📨 Запрос на запись отправлен!\nОжидайте подтверждения администратора.\n\n"
            "Консультация проводится после оплаты 💶"
        )

        # Inline кнопки для админа
        admin_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"approve|{user_id}|{cell.row}"),
                InlineKeyboardButton("❌ Отказать", callback_data=f"reject|{user_id}|{cell.row}")
            ]
        ])

        await context.bot.send_message(
            ADMIN_ID,
            f"📩 Пользователь {full_name} ({username}, {user_id}) хочет записаться на {slot}.",
            reply_markup=admin_keyboard
        )

        await start(update, context)
        return

    await update.message.reply_text("🤔 Не понял команду.")
    await start(update, context)


# ============= CALLBACK ОТ АДМИНА =============
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    action, user_id, row = query.data.split('|')
    user_id = int(user_id)
    row = int(row)
    slot_time = sheet.cell(row, 2).value

    if action == "approve":
        sheet.update_cell(row, 3, "Подтверждено")
        await context.bot.send_message(user_id, f"✅ Ваша запись на {slot_time} подтверждена!")
        await query.edit_message_text(f"✅ Подтверждено: {user_id}, слот {slot_time}")

    elif action == "reject":
        for col in range(3, 7):
            sheet.update_cell(row, col, "")
        await context.bot.send_message(user_id, f"❌ Слот {slot_time} не подтверждён.")
        await query.edit_message_text(f"❌ Отказано пользователю {user_id}, слот {slot_time}")


# ============= ЗАПУСК =============
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))

    logger.info("🚀 Бот запущен в режиме webhook")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=WEBHOOK_URL,
    )


if __name__ == "__main__":
    main()
