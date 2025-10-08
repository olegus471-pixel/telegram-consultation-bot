# ============= ФОНОВАЯ ЗАДАЧА (создание Meet за 15 минут) =============
async def background_jobs(app: Application):
    # запускается job-queue каждую минуту
    try:
        all_rows = await run_in_thread(sheet.get_all_values)
    except Exception as e:
        logger.error(f"Ошибка чтения Google Sheets в background_jobs: {e}")
        return

    now = datetime.datetime.now()
    for i, row in enumerate(all_rows[1:], start=2):
        try:
            # проверяем: Статус == "Подтверждено", Email (G) заполнен, Ссылка (K) пуста, Напомнено (J) == "pending"
            status = row[2].strip() if len(row) > 2 else ""
            email = row[6].strip() if len(row) > 6 else ""
            remind_flag = row[9].strip() if len(row) > 9 else ""
            link = row[10].strip() if len(row) > 10 else ""
            slot_text = row[1].strip() if len(row) > 1 else ""

            if status == "Подтверждено" and email and remind_flag == "pending" and not link:
                slot_dt = parse_slot_datetime(slot_text)
                if not slot_dt:
                    continue

                seconds_to = (slot_dt - now).total_seconds()
                # если между 0 и 15 минут включительно
                if 0 < seconds_to <= 900:
                    request_id = f"migrall-{i}-{int(datetime.datetime.now().timestamp())}"
                    event_body = {
                        "summary": f"Консультация Migrall — {row[3] if len(row) > 3 else ''}",
                        "description": f"Автоматически создано за 15 минут до встречи",
                        "start": {"dateTime": slot_dt.isoformat(), "timeZone": "Europe/Lisbon"},
                        "end": {"dateTime": (slot_dt + datetime.timedelta(hours=1)).isoformat(), "timeZone": "Europe/Lisbon"},
                        "attendees": [{"email": email}],
                        "conferenceData": {
                            "createRequest": {
                                "requestId": request_id,
                                "conferenceSolutionKey": {"type": "hangoutsMeet"}
                            }
                        }
                    }

                    try:
                        event = await run_in_thread(lambda: calendar_service.events().insert(
                            calendarId=CALENDAR_ID,
                            body=event_body,
                            conferenceDataVersion=1
                        ).execute())
                    except Exception as e:
                        logger.exception(f"Ошибка создания события в background для row {i}: {e}")
                        continue

                    meet_link = event.get("hangoutLink") or ""
                    # записываем ссылку и отмечаем, что напомнили
                    try:
                        await run_in_thread(sheet.update_cell, i, 11, meet_link)  # K
                        await run_in_thread(sheet.update_cell, i, 10, "sent")      # J = sent
                    except Exception as e:
                        logger.error(f"Ошибка записи ссылки в таблицу по row {i}: {e}")

                    # отправляем пользователю в Telegram
                    user_id = row[5].strip() if len(row) > 5 else ""
                    if user_id:
                        try:
                            await app.bot.send_message(int(user_id), f"🔗 Автоматическая отправка — ваша ссылка на Google Meet:\n{meet_link}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки юзеру (background) по row {i}: {e}")
        except Exception as e:
            logger.error(f"Ошибка обработки строки {i} в background_jobs: {e}")

    # просто корректный конец функции
    return
