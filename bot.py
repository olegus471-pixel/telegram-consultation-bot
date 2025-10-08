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
    InlineKeyboardButton
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
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
# Вспомогательные функции
# =======================
def find_user_booking(user_id: int):
    all_slots = sheet.get_all_values()[1:]
    now = datetime.datetime.now()
    for row in all_slots:
        if len(row) < 6:
            continue
        slot_time_str = row[1].strip()
        booked_user_id = row[5].strip()
        if booked_user_id == str(user_id):
            try:
                slot_time = datetime.datetime.strptime(slot_time_str, "%d.%m.%Y, %H:%M")
                if slot_time > now:
                    return row, slot_time_str
            except ValueError:
                continue
    return None, None


def parse_datetime(dt_str: str):
    return datetime.datetime.strptime(dt_str, "%d.%m.%Y, %H:%M")

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

    # === Отмена ===
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
                f"❌ У вас уже есть активная запись на {slot_time_str}.",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
            return

        await update.message.reply_text("Введите ваше имя:")
        context.user_data["step"] = "get_name"
        return

    # === Получаем имя ===
    if context.user_data.get("step") == "get_name":
        context.user_data["name"] = text
        all_slots = sheet.get_all_values()[1:]

        now = datetime.datetime.now()
        free_slots = [
            row[1].strip() for row in all_slots
            if len(row) > 2 and row[2].strip() == "" and parse_datetime(row[1]) > now
        ]

        if not free_slots:
            await update.message.reply_text("❌ Нет свободных слотов.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        context.user_data["step"] = "choose_slot"
        await update.message.reply_text(
            "Выберите время:",
            reply_markup=ReplyKeyboardMarkup([[s] for s in free_slots], resize_keyboard=True)
        )
        return

    # === Выбор слота ===
    if context.user_data.get("step") == "choose_slot":
        name = context.user_data["name"]
        slot = text
        try:
            cell = sheet.find(slot)
        except:
            await update.message.reply_text("❌ Слот не найден.")
            return

        if sheet.cell(cell.row, 3).value not in ("", None):
            await update.message.reply_text("❌ Этот слот уже занят.")
            return

        # Временно резервируем слот
        sheet.update_cell(cell.row, 3, name)
        sheet.update_cell(cell.row, 4, username)
        sheet.update_cell(cell.row, 5, str(user_id))
        sheet.update_cell(cell.row, 6, "Ожидает подтверждения")

        await update.message.reply_text(
            f"📨 Запрос на запись отправлен.\n⏳ Ожидайте подтверждения. Запись активируется после оплаты.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )

        # Уведомление админу
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm|{slot}|{user_id}|{name}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"reject|{slot}|{user_id}|{name}")
            ]
        ])
        await context.bot.send_message(
            ADMIN_ID,
            f"🔔 Запрос на запись:\n👤 {name}\n📅 {slot}\n🧑‍💻 {username}",
            reply_markup=keyboard
        )
        context.user_data.clear()
        return

    # === Моя запись ===
    if text == "📋 Моя запись":
        row, slot_time_str = find_user_booking(user_id)
        if not row:
            await update.message.reply_text("ℹ️ У вас нет активной записи.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
            return

        await update.message.reply_text(
            f"📅 Ваша запись на {slot_time_str}\nСтатус: {row[6]}",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return

    # === Инфо ===
    if text == "ℹ️ Инфо":
        await update.message.reply_text(
            "💬 Консультация по вопросам легализации в Португалии и Испании 🇵🇹🇪🇸\n\n💰 Стоимость: 120 €\n⏳ 1 час.",
            reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
        )
        return


# === Обработка подтверждения/отклонения ===
async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        action, slot, user_id, name = query.data.split("|")
    except Exception:
        await query.message.reply_text("⚠️ Ошибка: неверный формат данных.")
        return

    user_id = int(user_id)
    cell = sheet.find(slot)

    if action == "confirm":
        sheet.update_cell(cell.row, 6, "Подтверждено")
        await context.bot.send_message(
            user_id,
            f"✅ Ваша запись на {slot} подтверждена!\n"
            f"Когда вы хотите получить ссылку на встречу?",
            reply_markup=ReplyKeyboardMarkup([["Сейчас"], ["Позже"]], resize_keyboard=True)
        )
        await query.message.edit_text(f"✅ Запись {name} на {slot} подтверждена.")
    elif action == "reject":
        sheet.update_cell(cell.row, 6, "Отклонено")
        await context.bot.send_message(user_id, "❌ Ваша заявка на консультацию отклонена.")
        await query.message.edit_text(f"❌ Запись {name} на {slot} отклонена.")


# === Получаем e-mail и создаем Google Meet ===
async def handle_email_and_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user_id = update.message.from_user.id

    if context.user_data.get("step") == "get_email":
        slot = context.user_data["slot"]
        send_now = context.user_data["send_now"]

        # Сохраняем почту
        context.user_data["email"] = text

        # Если сразу — создаем встречу сейчас
        if send_now:
            await create_and_send_meet(update, context, slot, text)
        else:
            # если позже — запланируем отправку за 15 минут
            event_time = parse_datetime(slot)
            send_time = event_time - datetime.timedelta(minutes=15)
            delay = (send_time - datetime.datetime.now()).total_seconds()
            context.job_queue.run_once(send_meet_job, delay, data={
                "slot": slot,
                "email": text,
                "user_id": user_id
            })
            await update.message.reply_text(
                f"⏰ Ссылка будет отправлена за 15 минут до встречи ({slot}).",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
        context.user_data.clear()


async def send_meet_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    slot = data["slot"]
    email = data["email"]
    user_id = data["user_id"]

    await create_and_send_meet(None, context, slot, email, user_id)


async def create_and_send_meet(update, context, slot, email, user_id=None):
    event_time = parse_datetime(slot)
    event_end = event_time + datetime.timedelta(hours=1)

    event = {
        "summary": "Консультация Migrall",
        "description": "Онлайн-консультация",
        "start": {"dateTime": event_time.isoformat(), "timeZone": "Europe/Lisbon"},
        "end": {"dateTime": event_end.isoformat(), "timeZone": "Europe/Lisbon"},
        "attendees": [{"email": email}],
        "conferenceData": {"createRequest": {"requestId": f"meet-{event_time.timestamp()}"}}
    }

    try:
        created_event = calendar_service.events().insert(
            calendarId=CALENDAR_ID,
            body=event,
            conferenceDataVersion=1
        ).execute()

        meet_link = created_event["hangoutLink"]

        if update:
            await update.message.reply_text(
                f"🔗 Ваша ссылка на встречу: {meet_link}",
                reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True)
            )
        elif user_id:
            await context.bot.send_message(user_id, f"🔗 Ваша ссылка на встречу: {meet_link}")

    except Exception as e:
        if update:
            await update.message.reply_text(f"⚠️ Ошибка создания встречи: {e}")
        elif user_id:
            await context.bot.send_message(user_id, f"⚠️ Ошибка создания встречи: {e}")


# === Обработка выбора “Сейчас” или “Позже” ===
async def handle_send_option(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.message.from_user.id

    row, slot_time_str = find_user_booking(user_id)
    if not row:
        await update.message.reply_text("❌ Запись не найдена.", reply_markup=ReplyKeyboardMarkup(main_menu, resize_keyboard=True))
        return

    if text == "Сейчас":
        context.user_data["step"] = "get_email"
        context.user_data["slot"] = slot_time_str
        context.user_data["send_now"] = True
        await update.message.reply_text("Введите ваш e-mail, на который отправить ссылку:")
        return
    elif text == "Позже":
        context.user_data["step"] = "get_email"
        context.user_data["slot"] = slot_time_str
        context.user_data["send_now"] = False
        await update.message.reply_text("Введите ваш e-mail, на который будет отправлена ссылка перед встречей:")
        return


# =======================
# Приложение
# =======================
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(handle_admin_action))
app.add_handler(MessageHandler(filters.Regex("^(Сейчас|Позже)$"), handle_send_option))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
app.add_handler(MessageHandler(filters.Regex(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), handle_email_and_link))

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
