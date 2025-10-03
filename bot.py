import os
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==== Настройки ====
TOKEN = os.environ["TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])  # твой Telegram ID

# Подключение к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Расписание").график  # таблица "График работы"

# Главное меню
main_menu = [["📅 Записаться на консультацию", "ℹ️ Инфо"]]

# ==== Команды ====
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
        free_slots = [row[0] for row in all_slots if row[1] == ""]

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
        slot = text

        # Проверка занятости
        cell = sheet.find(slot)
        current_value = sheet.cell(cell.row, 2).value

        if current_value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте снова.")
            return

        # Запись в таблицу
        sheet.update_cell(cell.row, 2, name)  # колонка 2 = имя клиента
        sheet.update_cell(cell.row, 3, "Консультация")

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

# ==== Запуск ====
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

print("Бот запущен!")
app.run_polling()
