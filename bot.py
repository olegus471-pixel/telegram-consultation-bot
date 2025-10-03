import os, json, base64
from oauth2client.service_account import ServiceAccountCredentials
import gspread

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =======================
# Переменные окружения
# =======================
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])

# =======================
# Подключение к Google Sheets
# =======================
creds_json = base64.b64decode(os.environ["GOOGLE_CREDS"])
creds_dict = json.loads(creds_json)

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open("Расписание").worksheet("график")  # имя листа с учетом регистра

# =======================
# Главное меню
# =======================
main_menu = [["📅 Записаться на консультацию", "ℹ️ Инфо"]]

# =======================
# Хэндлеры
# =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для записи на консультацию.\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "📅 Записаться на консультацию":
        await update.message.reply_text("Введите ваше имя:")
        context.user_data["step"] = "name"

    elif context.user_data.get("step") == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "choose_slot"

        # Получаем список слотов
        all_slots = sheet.get_all_values()[1:]  # пропускаем заголовки
        free_slots = [row[0].strip() for row in all_slots if row[1].strip() == ""]

        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.")
            context.user_data.clear()
            return

        # Отправляем кнопки
        slot_buttons = [[s] for s in free_slots]
        await update.message.reply_text(
            "Выберите удобное время:",
            reply_markup=ReplyKeyboardMarkup(slot_buttons, resize_keyboard=True)
        )

    elif context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text.strip()

        # Проверка занятости
        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.")
            return

        current_value = sheet.cell(cell.row, 2).value
        if current_value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте снова.")
            return

        # Запись в таблицу
        sheet.update_cell(cell.row, 2, name)        # имя клиента
        sheet.update_cell(cell.row, 3, "Консультация")  # услуга

        await update.message.reply_text(
            f"✅ Запись принята!\nИмя: {name}\nУслуга: Консультация\nКогда: {slot}",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        # Уведомление админу
        await context.bot.send_message(
            ADMIN_ID,
            f"📌 Новая запись:\nИмя: {name}\nУслуга: Консультация\nКогда: {slot}"
        )

        context.user_data.clear()

    elif text == "ℹ️ Инфо":
        await update.message.reply_text("ℹ️ Консультации проходят онлайн. Длительность: 1 час.")

    else:
        await update.message.reply_text("Не понял 🤔. Попробуйте снова.")

# =======================
# Запуск бота
# =======================
if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен!")
    app.run_polling()
