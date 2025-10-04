import os
import json
import base64
import asyncio
import datetime
from oauth2client.service_account import ServiceAccountCredentials
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
# Google Sheets
# =======================
creds_json = base64.b64decode(os.environ["GOOGLE_CREDS"])
creds_dict = json.loads(creds_json)

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open("Расписание").worksheet("График")

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

    # 1. Начало записи
    if text == "📅 Записаться на консультацию Migrall":
        # Проверяем, есть ли уже запись
        all_slots = sheet.get_all_values()
        for row in all_slots[1:]:
            if str(user_id) in row:  # ищем user_id в строке
                await update.message.reply_text("❌ У вас уже есть активная запись. Перенос возможен, но не новая запись.")
                return

        await update.message.reply_text("Введите ваше имя (для записи):")
        context.user_data["step"] = "name"
        return

    # 2. Получаем имя
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

    # 3. Запись слота
    if context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text
        try:
            cell = sheet.find(slot)
        except gspread.CellNotFound:
            await update.message.reply_text("❌ Слот не найден. Попробуйте снова.")
            return

        # проверяем, что колонка C (имя) пуста
        if sheet.cell(cell.row, 3).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят. Попробуйте снова.")
            return

        # Записываем данные
        sheet.update_cell(cell.row, 3, name)        # имя (C)
        sheet.update_cell(cell.row, 4, username)    # username (D)
        sheet.update_cell(cell.row, 5, str(user_id)) # user_id (E)
        sheet.update_cell(cell.row, 6, "Консультация") # услуга (F)
        sheet.update_cell(cell.row, 7, "0")         # переносы (G)
        sheet.update_cell(cell.row, 8, "0")         # напоминание (H)

        await update.message.reply_text(
            f"""✅ Запись принята! 
Обращаем внимание, что консультация платная - 120 Euro. К сумме может быть добавлен IVA. 
Оплата производится перед консультацией. Подробности уточняйте у @migrallpt 

Имя: {name}
Username: @{username}
Услуга: Консультация
Когда: {slot}""",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        await context.bot.send_message(
            ADMIN_ID,
            f"📌 Новая запись:\nИмя: {name}\nUsername: @{username}\nУслуга: Консультация\nКогда: {slot}"
        )
        context.user_data.clear()
        return

    # 4. Инфо
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            """Консультация по легализации в Португалии 🇵🇹 и Испании 🇪🇸 

🔹 Что разберем на консультации?
✅ Анализируем именно ваш кейс
✅ Рассматриваем все возможные варианты легализации
✅ Прописываем пошаговый план, включая самостоятельные шаги
✅ Отвечаем на все ваши вопросы

💰 Стоимость: 120 €
⏳ Длительность: 1 час

*К сумме может быть добавлен НДС 23%

📩 Готовы записаться или остались вопросы? Пишите – поможем!"""
        )
        return

    await update.message.reply_text("Не понял 🤔. Попробуйте снова.")

# =======================
# Задача: Напоминания за 24 часа
# =======================
async def reminder_job(app: Application):
    while True:
        all_slots = sheet.get_all_values()[1:]
        now = datetime.datetime.now()
        for row in all_slots:
            slot_time_str = row[1].strip()  # колонка B
            username = row[3].strip() if len(row) > 3 else ""
            user_id = row[4].strip() if len(row) > 4 else ""
            reminded = row[7].strip() if len(row) > 7 else "0"

            if not slot_time_str or not user_id:
                continue

            try:
                slot_time = datetime.datetime.strptime(slot_time_str, "%Y-%m-%d %H:%M")
            except ValueError:
                continue

            if reminded == "0" and 0 < (slot_time - now).total_seconds() <= 86400:  # 24 часа
                try:
                    await app.bot.send_message(
                        int(user_id),
                        f"⏰ Напоминаем! У вас консультация {slot_time_str}. Ждем вас!"
                    )
                    cell = sheet.find(slot_time_str)
                    sheet.update_cell(cell.row, 8, "1")  # помечаем напоминание
                except Exception as e:
                    print("Ошибка отправки напоминания:", e)

        await asyncio.sleep(3600)  # проверка каждый час

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
    print("Бот запущен через Webhook")

    # запускаем задачу-напоминатель
    asyncio.create_task(reminder_job(app))

    # держим процесс живым
    await asyncio.Event().wait()

# запускаем в существующем loop
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
