import asyncio
import logging
import uuid
import qrcode
import io
import json
import os
import csv
import hashlib
from datetime import datetime
from functools import lru_cache
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont
 
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
 
# ─────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────
BOT_TOKEN     = "8611692515:AAG38UtJT4jGavLTcWcaDPqxc9jFuFr0yWU"
ADMIN_IDS     = {5782652474, 1170098928}   # set для O(1) проверки
 
CONCERT_NAME  = "Dirty Moritz Clubshow"
CONCERT_DATE  = "дата и время"
CONCERT_PLACE = "место проведения"
TICKET_PRICE  = "340 ₽"
 
SBP_NUMBER    = "7 916 957 69 48 (Озон Банк)"
CARD_NUMBER   = "2200 7017 8294 6930"
 
MAIN_PHOTO_URL = "https://lh4.googleusercontent.com/proxy/9Psk3QoVjbtUUnnarWKI31dg6_yrLe5h4vXu_oW_ZhRqMjpZ2S9UKNxD6_yFZUoUwp4c"
 
TICKETS_DB    = "tickets.json"
# ─────────────────────────────────────────
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
 
bot    = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp     = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)
 
# ═══════════════════════════════════════════
#  IN-MEMORY КЭШ — основное ускорение
#  Все операции работают с dict в RAM.
#  На диск сбрасываем только при изменениях.
# ═══════════════════════════════════════════
 
_db_cache: dict | None = None
_db_lock = asyncio.Lock()          # защита от concurrent writes
 
 
def _load_from_disk() -> dict:
    if os.path.exists(TICKETS_DB):
        with open(TICKETS_DB, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}
 
 
def _save_to_disk(data: dict):
    tmp = TICKETS_DB + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, TICKETS_DB)          # атомарная замена
 
 
def _get_db() -> dict:
    """Возвращает кэш (при первом вызове читает с диска)."""
    global _db_cache
    if _db_cache is None:
        _db_cache = _load_from_disk()
    return _db_cache
 
 
async def _flush_db():
    """Асинхронно сбрасывает кэш на диск (не блокирует event loop)."""
    data = dict(_get_db())               # снапшот
    await asyncio.to_thread(_save_to_disk, data)
 
 
# ══════════════════════════════════════════
#  ЦВЕТА ДЛЯ QR-КОДОВ
# ══════════════════════════════════════════
 
QR_PALETTES = [
    ("#1a1a2e", "#e8e4f0", "#7c3aed"),
    ("#0f2027", "#d4f5e9", "#10b981"),
    ("#1a0a00", "#fff3e0", "#f59e0b"),
    ("#1a0020", "#fce7f3", "#ec4899"),
    ("#001a1a", "#e0f7fa", "#06b6d4"),
    ("#0d1117", "#f0f6ff", "#3b82f6"),
    ("#1a1500", "#fffbeb", "#eab308"),
    ("#0a1a0a", "#f0fdf4", "#22c55e"),
]
 
 
def _palette_for(ticket_id: str):
    idx = int(hashlib.md5(ticket_id.encode()).hexdigest(), 16) % len(QR_PALETTES)
    return QR_PALETTES[idx]
 
 
# ══════════════════════════════════════════
#  БАЗА БИЛЕТОВ (работает через кэш)
# ══════════════════════════════════════════
 
async def add_ticket(ticket_id: str, user_id: int, username: str):
    async with _db_lock:
        _get_db()[ticket_id] = {
            "user_id":       user_id,
            "username":      username,
            "issued_at":     datetime.now().isoformat(),
            "used":          False,
            "used_at":       None,
            "scan_attempts": 0,
        }
        await _flush_db()
 
 
def check_ticket(ticket_id: str) -> dict | None:
    return _get_db().get(ticket_id)
 
 
async def mark_used(ticket_id: str) -> bool:
    async with _db_lock:
        info = _get_db().get(ticket_id)
        if not info:
            return False
        info["scan_attempts"] = info.get("scan_attempts", 0) + 1
        if info["used"]:
            await _flush_db()
            return False
        info["used"]    = True
        info["used_at"] = datetime.now().isoformat()
        await _flush_db()
        return True
 
 
async def mark_unused(ticket_id: str):
    async with _db_lock:
        info = _get_db().get(ticket_id)
        if info:
            info["used"]          = False
            info["used_at"]       = None
            info["scan_attempts"] = 0
            await _flush_db()
 
 
def get_user_tickets(user_id: int) -> list:
    return [
        {"id": tid, **info}
        for tid, info in _get_db().items()
        if info["user_id"] == user_id
    ]
 
 
def get_all_stats() -> dict:
    db    = _get_db()
    total = len(db)
    used  = sum(1 for t in db.values() if t["used"])
    scams = sum(1 for t in db.values() if t.get("scan_attempts", 0) > 1)
    return {"total": total, "used": used, "active": total - used, "double_scan": scams}
 
 
def find_tickets_by_username(query: str) -> list:
    query = query.lower().lstrip("@")
    return [
        {"id": tid, **info}
        for tid, info in _get_db().items()
        if query in info.get("username", "").lower()
    ]
 
 
def export_tickets_csv() -> bytes:
    db  = _get_db()
    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["ticket_id", "user_id", "username", "issued_at",
                "used", "used_at", "scan_attempts"])
    for tid, info in sorted(db.items(), key=lambda x: x[1]["issued_at"], reverse=True):
        w.writerow([
            tid, info["user_id"], info["username"],
            info["issued_at"], info["used"],
            info.get("used_at", ""), info.get("scan_attempts", 0),
        ])
    return buf.getvalue().encode("utf-8-sig")
 
 
# ══════════════════════════════════════════
#  ШРИФТЫ — кэшируем при старте
# ══════════════════════════════════════════
 
_FONT_BIG:  ImageFont.FreeTypeFont | None = None
_FONT_SMALL: ImageFont.FreeTypeFont | None = None
 
_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
]
 
 
def _init_fonts():
    global _FONT_BIG, _FONT_SMALL
    for fp in _FONT_PATHS:
        if os.path.exists(fp):
            try:
                _FONT_BIG   = ImageFont.truetype(fp, 18)
                _FONT_SMALL = ImageFont.truetype(fp, 13)
                return
            except Exception:
                pass
    _FONT_BIG = _FONT_SMALL = ImageFont.load_default()
 
 
# ══════════════════════════════════════════
#  ГЕНЕРАЦИЯ QR
# ══════════════════════════════════════════
 
def _generate_qr_sync(ticket_id: str, username: str = "") -> bytes:
    """CPU-bound — вызываем через asyncio.to_thread."""
    fill_color, back_color, accent = _palette_for(ticket_id)
 
    qr = qrcode.QRCode(
        version=2,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=12,
        border=3,
    )
    qr.add_data(ticket_id)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color=fill_color, back_color=back_color).convert("RGBA")
    qr_w, qr_h = qr_img.size
 
    pad_top, pad_bottom, pad_side = 20, 100, 20
    canvas_w = qr_w + pad_side * 2
    canvas_h = qr_h + pad_top + pad_bottom
 
    canvas = Image.new("RGBA", (canvas_w, canvas_h), back_color)
    draw   = ImageDraw.Draw(canvas)
    b      = 4
    draw.rectangle([b, b, canvas_w - b - 1, canvas_h - b - 1], outline=accent, width=b)
    canvas.paste(qr_img, (pad_side, pad_top))
 
    font_big   = _FONT_BIG   or ImageFont.load_default()
    font_small = _FONT_SMALL or ImageFont.load_default()
 
    text_y = qr_h + pad_top + 8
    draw.text((canvas_w // 2, text_y),      CONCERT_NAME,          fill=fill_color, font=font_big,   anchor="mt")
    draw.text((canvas_w // 2, text_y + 26), f"#{ticket_id[:8]}…",  fill=accent,     font=font_small, anchor="mt")
    if username:
        draw.text((canvas_w // 2, text_y + 48), username[:30], fill=fill_color, font=font_small, anchor="mt")
 
    buf = io.BytesIO()
    canvas.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()
 
 
async def generate_qr(ticket_id: str, username: str = "") -> bytes:
    """Генерирует QR в отдельном потоке, не блокируя event loop."""
    return await asyncio.to_thread(_generate_qr_sync, ticket_id, username)
 
 
def _decode_qr_sync(image_bytes: bytes) -> str | None:
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        detector = cv2.QRCodeDetector()
        data, points, _ = detector.detectAndDecode(img)

        if data:
            return data.strip().upper()

    except Exception as e:
        logger.error(f"QR decode error: {e}")

    return None
 
 
async def decode_qr_from_bytes(image_bytes: bytes) -> str | None:
    return await asyncio.to_thread(_decode_qr_sync, image_bytes)
 
 
# ══════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ══════════════════════════════════════════
 
class PaymentStates(StatesGroup):
    waiting_screenshot = State()
 
class AdminStates(StatesGroup):
    check_mode  = State()
    search_mode = State()
 
 
# ══════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════
 
def kb_main(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🎟  Купить билет",   callback_data="buy_ticket")],
        [InlineKeyboardButton(text="📋  Мои билеты",     callback_data="my_tickets")],
        [InlineKeyboardButton(text="ℹ️  О мероприятии",  callback_data="ticket_info")],
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑  Панель администратора", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
 
def kb_payment_method() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱  СБП",              callback_data="pay_sbp")],
        [InlineKeyboardButton(text="💳  Перевод на карту", callback_data="pay_card")],
        [InlineKeyboardButton(text="◀️  Назад",            callback_data="back_main")],
    ])
 
def kb_paid() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅  Я оплатил(а)",  callback_data="i_paid")],
        [InlineKeyboardButton(text="◀️  Назад",         callback_data="buy_ticket")],
    ])
 
def kb_after_info() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟  Купить билет", callback_data="buy_ticket")],
        [InlineKeyboardButton(text="◀️  Назад",        callback_data="back_main")],
    ])
 
def kb_admin_confirm(user_id: int, ticket_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅  Подтвердить", callback_data=f"confirm:{user_id}:{ticket_id}"),
        InlineKeyboardButton(text="❌  Отклонить",   callback_data=f"decline:{user_id}:{ticket_id}"),
    ]])
 
def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍  Проверить QR / ID",   callback_data="admin_check")],
        [InlineKeyboardButton(text="🔎  Найти по @username",  callback_data="admin_search")],
        [InlineKeyboardButton(text="📊  Статистика",          callback_data="admin_stats")],
        [InlineKeyboardButton(text="📋  Все билеты",          callback_data="admin_list")],
        [InlineKeyboardButton(text="📤  Экспорт CSV",         callback_data="admin_export")],
        [InlineKeyboardButton(text="◀️  Главное меню",        callback_data="back_main")],
    ])
 
def kb_admin_check_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️  Панель администратора", callback_data="admin_panel")],
    ])
 
def kb_ticket_action(ticket_id: str, is_used: bool) -> InlineKeyboardMarkup:
    toggle_text = "🔄  Снять отметку «использован»" if is_used else "✅  Отметить вручную"
    toggle_cb   = f"unuse:{ticket_id}" if is_used else f"forceuse:{ticket_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle_text, callback_data=toggle_cb)],
        [InlineKeyboardButton(text="◀️  Панель администратора", callback_data="admin_panel")],
    ])
 
def kb_my_ticket(ticket_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄  Получить QR снова", callback_data=f"reissue_qr:{ticket_id}")],
        [InlineKeyboardButton(text="◀️  Назад",             callback_data="my_tickets")],
    ])
 
 
# ══════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════
 
async def safe_edit(call: CallbackQuery, text: str, markup=None):
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=text, reply_markup=markup, parse_mode="HTML")
        else:
            await call.message.edit_text(text=text, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"safe_edit: {e}")
    try:
        await call.answer()
    except Exception:
        pass
 
 
async def send_main_screen(target, is_admin: bool = False):
    caption = (
        f"👋 <b>Добро пожаловать!</b>\n\n"
        f"🎵 <b>{CONCERT_NAME}</b>\n"
        f"📅 {CONCERT_DATE}\n"
        f"📍 {CONCERT_PLACE}\n\n"
        f"Стоимость билета: <b>{TICKET_PRICE}</b>\n\n"
        f"Выберите действие:"
    )
    markup = kb_main(is_admin=is_admin)
    if isinstance(target, CallbackQuery):
        try:
            await target.message.delete()
        except Exception:
            pass
        await bot.send_photo(
            chat_id=target.from_user.id,
            photo=MAIN_PHOTO_URL,
            caption=caption,
            reply_markup=markup,
            parse_mode="HTML",
        )
        try:
            await target.answer()
        except Exception:
            pass
    else:
        await target.answer_photo(
            photo=MAIN_PHOTO_URL,
            caption=caption,
            reply_markup=markup,
            parse_mode="HTML",
        )
 
 
def _admin_panel_text(stats: dict) -> str:
    scam_line = f"\n⚠️ Попыток двойного прохода: <b>{stats['double_scan']}</b>" if stats["double_scan"] else ""
    return (
        f"👑 <b>Панель администратора</b>\n\n"
        f"🎵 <b>{CONCERT_NAME}</b>\n"
        f"📅 {CONCERT_DATE}  |  📍 {CONCERT_PLACE}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎫 Всего билетов:  <b>{stats['total']}</b>\n"
        f"✅ Прошло:         <b>{stats['used']}</b>\n"
        f"🟢 Ещё не прошли: <b>{stats['active']}</b>"
        f"{scam_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
 
 
async def _notify_admins(text: str, exclude_id: int | None = None):
    """Рассылает сообщение всем администраторам параллельно."""
    tasks = [
        bot.send_message(aid, text, parse_mode="HTML")
        for aid in ADMIN_IDS if aid != exclude_id
    ]
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"_notify_admins error: {r}")
 
 
# ══════════════════════════════════════════
#  /start
# ══════════════════════════════════════════
 
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await send_main_screen(message, is_admin=message.from_user.id in ADMIN_IDS)
 
 
# ══════════════════════════════════════════
#  /admin  /export  /find
# ══════════════════════════════════════════
 
@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    stats = get_all_stats()
    await message.answer(_admin_panel_text(stats), reply_markup=kb_admin_panel(), parse_mode="HTML")
 
 
@router.message(Command("export"))
async def cmd_export(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    fname = f"tickets_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    csv_bytes = await asyncio.to_thread(export_tickets_csv)
    await message.answer_document(
        BufferedInputFile(csv_bytes, filename=fname),
        caption="📤 Экспорт всех билетов",
    )
 
 
@router.message(Command("find"))
async def cmd_find(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /find @username")
        return
    results = find_tickets_by_username(args[1])
    if not results:
        await message.answer(f"❌ Билеты для «{args[1]}» не найдены.")
        return
    lines = []
    for t in results:
        status  = "✅" if t["used"] else "🟢"
        issued  = t["issued_at"][:16].replace("T", " ")
        scans   = t.get("scan_attempts", 0)
        line    = f"{status} <code>{t['id']}</code>\n    👤 {t['username']}  📅 {issued}"
        if t.get("used_at"):
            line += f"\n    🚪 Прошёл: {t['used_at'][:16].replace('T', ' ')}"
        if scans > 1:
            line += f"\n    ⚠️ Попыток скана: {scans}"
        lines.append(line)
    await message.answer(
        f"🔎 Найдено {len(results)} билет(ов):\n\n" + "\n\n".join(lines),
        parse_mode="HTML",
    )
 
 
# ══════════════════════════════════════════
#  КЛИЕНТСКИЕ КОЛБЭКИ
# ══════════════════════════════════════════
 
@router.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await send_main_screen(call, is_admin=call.from_user.id in ADMIN_IDS)
 
 
@router.callback_query(F.data == "ticket_info")
async def cb_ticket_info(call: CallbackQuery):
    await safe_edit(
        call,
        f"ℹ️ <b>О мероприятии</b>\n\n"
        f"🎵 <b>{CONCERT_NAME}</b>\n"
        f"📅 Дата и время: <b>{CONCERT_DATE}</b>\n"
        f"📍 Место: <b>{CONCERT_PLACE}</b>\n"
        f"💰 Стоимость: <b>{TICKET_PRICE}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"После оплаты вы получите уникальный <b>QR-код</b> — это ваш билет.\n"
        f"Покажите его при входе — организатор отсканирует код.\n\n"
        f"📌 <b>Каждый QR-код одноразовый</b> — повторный проход невозможен.",
        markup=kb_after_info(),
    )
 
 
@router.callback_query(F.data == "buy_ticket")
async def cb_buy_ticket(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(
        call,
        f"💳 <b>Покупка билета</b>\n\n"
        f"🎵 <b>{CONCERT_NAME}</b>\n"
        f"💰 Стоимость: <b>{TICKET_PRICE}</b>\n\n"
        f"Выберите удобный способ оплаты:",
        markup=kb_payment_method(),
    )
 
 
@router.callback_query(F.data == "pay_sbp")
async def cb_pay_sbp(call: CallbackQuery, state: FSMContext):
    await state.update_data(payment_method="СБП")
    await safe_edit(
        call,
        f"📱 <b>Оплата через СБП</b>\n\n"
        f"Переведите <b>{TICKET_PRICE}</b> по номеру:\n\n"
        f"<code>{SBP_NUMBER}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 В комментарии укажите: <b>Билет на концерт</b>\n\n"
        f"После оплаты нажмите «Я оплатил(а)» и пришлите скриншот.",
        markup=kb_paid(),
    )
 
 
@router.callback_query(F.data == "pay_card")
async def cb_pay_card(call: CallbackQuery, state: FSMContext):
    await state.update_data(payment_method="Карта")
    await safe_edit(
        call,
        f"💳 <b>Оплата переводом на карту</b>\n\n"
        f"Переведите <b>{TICKET_PRICE}</b> на карту:\n\n"
        f"<code>{CARD_NUMBER}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 В комментарии укажите: <b>Билет на концерт</b>\n\n"
        f"После оплаты нажмите «Я оплатил(а)» и пришлите скриншот.",
        markup=kb_paid(),
    )
 
 
@router.callback_query(F.data == "i_paid")
async def cb_i_paid(call: CallbackQuery, state: FSMContext):
    await state.set_state(PaymentStates.waiting_screenshot)
    await safe_edit(
        call,
        "📸 <b>Пришлите скриншот оплаты</b>\n\n"
        "Я передам его администратору. После подтверждения вы получите QR-код билета.",
    )
 
 
# ══════════════════════════════════════════
#  МОИ БИЛЕТЫ
# ══════════════════════════════════════════
 
@router.callback_query(F.data == "my_tickets")
async def cb_my_tickets(call: CallbackQuery):
    tickets = get_user_tickets(call.from_user.id)
    if not tickets:
        await safe_edit(
            call,
            "📋 <b>Мои билеты</b>\n\nУ вас пока нет билетов.\nКупите билет, чтобы он появился здесь!",
            markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎟  Купить билет", callback_data="buy_ticket")],
                [InlineKeyboardButton(text="◀️  Назад",        callback_data="back_main")],
            ]),
        )
        return
 
    buttons = []
    text    = "📋 <b>Мои билеты</b>\n\n"
    for t in tickets:
        status   = "✅ Использован" if t["used"] else "🟢 Активен"
        short_id = t["id"][:8] + "…"
        text    += f"🎫 <code>{t['id']}</code>\n{status}\n\n"
        label    = f"{'🟢' if not t['used'] else '✅'}  {short_id}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"view_ticket:{t['id']}")])
 
    buttons.append([InlineKeyboardButton(text="◀️  Назад", callback_data="back_main")])
    await safe_edit(call, text, markup=InlineKeyboardMarkup(inline_keyboard=buttons))
 
 
@router.callback_query(F.data.startswith("view_ticket:"))
async def cb_view_ticket(call: CallbackQuery):
    ticket_id = call.data.split(":", 1)[1]
    info      = check_ticket(ticket_id)
    if not info or info["user_id"] != call.from_user.id:
        await call.answer("Билет не найден.", show_alert=True)
        return
 
    status = "✅ Использован" if info["used"] else "🟢 Активен"
    issued = info["issued_at"][:16].replace("T", " в ")
    await safe_edit(
        call,
        f"🎫 <b>Ваш билет</b>\n\n"
        f"🎵 <b>{CONCERT_NAME}</b>\n"
        f"📅 {CONCERT_DATE}\n"
        f"📍 {CONCERT_PLACE}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{ticket_id}</code>\n"
        f"📌 Статус: <b>{status}</b>\n"
        f"🕐 Выдан: {issued}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Покажите QR-код на входе.",
        markup=kb_my_ticket(ticket_id),
    )
 
 
@router.callback_query(F.data.startswith("reissue_qr:"))
async def cb_reissue_qr(call: CallbackQuery):
    ticket_id = call.data.split(":", 1)[1]
    info      = check_ticket(ticket_id)
    if not info or info["user_id"] != call.from_user.id:
        await call.answer("Билет не найден.", show_alert=True)
        return
 
    await call.answer("Генерирую QR…")
    uname    = info.get("username", "")
    qr_bytes = await generate_qr(ticket_id, username=uname)
    status   = "✅ Использован" if info["used"] else "🟢 Активен"
 
    await bot.send_photo(
        chat_id=call.from_user.id,
        photo=BufferedInputFile(qr_bytes, filename="ticket_qr.png"),
        caption=(
            f"🎫 <b>QR-код вашего билета</b>\n\n"
            f"🎵 <b>{CONCERT_NAME}</b>\n"
            f"📅 {CONCERT_DATE}\n"
            f"📍 {CONCERT_PLACE}\n\n"
            f"🆔 ID: <code>{ticket_id}</code>\n"
            f"📌 Статус: <b>{status}</b>\n\n"
            f"Покажите этот QR-код на входе."
        ),
        parse_mode="HTML",
    )
 
 
# ══════════════════════════════════════════
#  СКРИНШОТ ОПЛАТЫ
# ══════════════════════════════════════════
 
@router.message(PaymentStates.waiting_screenshot, F.photo)
async def received_screenshot(message: Message, state: FSMContext):
    data      = await state.get_data()
    method    = data.get("payment_method", "не указан")
    user      = message.from_user
    uname     = f"@{user.username}" if user.username else f"id:{user.id}"
    ticket_id = str(uuid.uuid4()).replace("-", "").upper()[:16]
 
    caption = (
        f"💰 <b>Новая оплата!</b>\n\n"
        f"👤 {uname}\n"
        f"🆔 User ID: <code>{user.id}</code>\n"
        f"💳 Способ: <b>{method}</b>\n"
        f"🎫 Ticket ID: <code>{ticket_id}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Подтвердите или отклоните:"
    )
 
    tasks = [
        bot.send_photo(
            chat_id=aid,
            photo=message.photo[-1].file_id,
            caption=caption,
            reply_markup=kb_admin_confirm(user.id, ticket_id),
            parse_mode="HTML",
        )
        for aid in ADMIN_IDS
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Не удалось отправить скриншот администратору: {r}")
 
    await state.clear()
    await message.answer(
        "✅ <b>Скриншот отправлен!</b>\n\n"
        "Ожидайте подтверждения. Как только оплата подтверждена — получите QR-код. 🎫",
        parse_mode="HTML",
    )
 
 
@router.message(PaymentStates.waiting_screenshot)
async def wrong_file_type(message: Message):
    await message.answer("📸 Пожалуйста, пришлите именно <b>скриншот</b> (фото).", parse_mode="HTML")
 
 
# ══════════════════════════════════════════
#  ADMIN: ПОДТВЕРДИТЬ / ОТКЛОНИТЬ
#  ✅ ИСПРАВЛЕНО: Блокировка двойного подтверждения
# ══════════════════════════════════════════
 
@router.callback_query(F.data.startswith("confirm:"))
async def cb_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return

    _, user_id_str, ticket_id = call.data.split(":", 2)
    user_id = int(user_id_str)

    # 🔒 КРИТИЧНО: Проверяем, не был ли билет уже выдан
    if check_ticket(ticket_id):
        await call.answer("⚠️ Этот билет уже обработан!", show_alert=True)
        # Удаляем кнопки, чтобы других админов не прельщало нажимать
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    # Имя администратора
    admin = call.from_user
    admin_name = f"@{admin.username}" if admin.username else f"{admin.full_name} (id:{admin.id})"

    try:
        user_info = await bot.get_chat(user_id)
        uname     = f"@{user_info.username}" if user_info.username else str(user_id)
    except Exception:
        uname = str(user_id)

    # Выдаём билет в БД
    await add_ticket(ticket_id, user_id, uname)

    # Генерируем QR
    qr_bytes = await generate_qr(ticket_id, username=uname)

    # Отправляем QR пользователю
    try:
        await bot.send_photo(
            chat_id=user_id,
            photo=BufferedInputFile(qr_bytes, filename="ticket_qr.png"),
            caption=(
                f"🎉 <b>Оплата подтверждена!</b>\n\n"
                f"🎵 <b>{CONCERT_NAME}</b>\n"
                f"📅 {CONCERT_DATE}\n"
                f"📍 {CONCERT_PLACE}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎫 ID: <code>{ticket_id}</code>\n\n"
                f"Покажите QR-код на входе.\n"
                f"💡 В разделе «Мои билеты» можно получить QR снова."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Не удалось отправить QR пользователю {user_id}: {e}")

    # Обновляем сообщение — удаляем кнопки и добавляем статус
    try:
        await call.message.edit_caption(
            call.message.caption + f"\n\n✅ <b>ПОДТВЕРЖДЕНО</b> — {admin_name}",
            parse_mode="HTML",
            reply_markup=None,  # 🔒 Удаляем кнопки
        )
    except Exception:
        pass

    # Уведомляем остальных администраторов
    notify_text = (
        f"✅ <b>Оплата подтверждена</b>\n\n"
        f"👤 Покупатель: {uname}\n"
        f"🎫 Ticket ID: <code>{ticket_id}</code>\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Подтвердил: <b>{admin_name}</b>"
    )
    asyncio.create_task(_notify_admins(notify_text, exclude_id=call.from_user.id))

    await call.answer("✅ Билет выдан!", show_alert=False)
 
 
@router.callback_query(F.data.startswith("decline:"))
async def cb_decline(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return

    _, user_id_str, ticket_id_declined = call.data.split(":", 2)
    user_id = int(user_id_str)

    # 🔒 КРИТИЧНО: Если билет уже в БД — не отклоняем (значит, другой админ подтвердил)
    if check_ticket(ticket_id_declined):
        await call.answer("⚠️ Билет уже подтверждён!", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    # Имя администратора
    admin = call.from_user
    admin_name = f"@{admin.username}" if admin.username else f"{admin.full_name} (id:{admin.id})"

    # Отправляем отказ пользователю
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "❌ <b>Оплата отклонена.</b>\n\n"
                "Администратор не смог подтвердить оплату.\n"
                "Если считаете это ошибкой — свяжитесь с нами.\n\n"
                "Попробовать снова: /start"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass

    # Обновляем сообщение
    try:
        await call.message.edit_caption(
            call.message.caption + f"\n\n❌ <b>ОТКЛОНЕНО</b> — {admin_name}",
            parse_mode="HTML",
            reply_markup=None,  # 🔒 Удаляем кнопки
        )
    except Exception:
        pass

    # Уведомляем остальных администраторов
    notify_text = (
        f"❌ <b>Оплата отклонена</b>\n\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"🎫 Ticket ID: <code>{ticket_id_declined}</code>\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Отклонил: <b>{admin_name}</b>"
    )
    asyncio.create_task(_notify_admins(notify_text, exclude_id=call.from_user.id))

    await call.answer("❌ Отклонено.", show_alert=False)
 
 
# ══════════════════════════════════════════
#  ADMIN ПАНЕЛЬ — колбэки
# ══════════════════════════════════════════
 
@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    stats = get_all_stats()
    await safe_edit(call, _admin_panel_text(stats), markup=kb_admin_panel())
 
 
@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    stats  = get_all_stats()
    db     = _get_db()
    recent = sorted(db.items(), key=lambda x: x[1]["issued_at"], reverse=True)[:5]
 
    recent_text = ""
    for tid, info in recent:
        issued  = info["issued_at"][:16].replace("T", " ")
        status  = "✅" if info["used"] else "🟢"
        scans   = info.get("scan_attempts", 0)
        entry   = f"{status} <code>{tid[:8]}…</code> — {info['username']} ({issued})"
        if info.get("used_at"):
            entry += f"\n      🚪 {info['used_at'][:16].replace('T', ' ')}"
        if scans > 1:
            entry += f"  ⚠️ x{scans}"
        recent_text += entry + "\n\n"
 
    scam_line = f"\n⚠️ Попыток двойного прохода: <b>{stats['double_scan']}</b>" if stats["double_scan"] else ""
 
    await safe_edit(
        call,
        f"📊 <b>Статистика билетов</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎫 Всего выдано:   <b>{stats['total']}</b>\n"
        f"✅ Прошло:         <b>{stats['used']}</b>\n"
        f"🟢 Ещё не прошли: <b>{stats['active']}</b>"
        f"{scam_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Последние 5 билетов:</b>\n\n"
        f"{recent_text or 'Нет данных'}",
        markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️  Назад", callback_data="admin_panel")]
        ]),
    )
 
 
@router.callback_query(F.data == "admin_list")
async def cb_admin_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    db = _get_db()
    if not db:
        await call.answer("Билетов пока нет.", show_alert=True)
        return
 
    lines = []
    for tid, info in sorted(db.items(), key=lambda x: x[1]["issued_at"], reverse=True):
        status  = "✅" if info["used"] else "🟢"
        issued  = info["issued_at"][:16].replace("T", " ")
        scans   = info.get("scan_attempts", 0)
        line    = f"{status} <code>{tid}</code>\n    👤 {info['username']}  📅 {issued}"
        if info.get("used_at"):
            line += f"\n    🚪 {info['used_at'][:16].replace('T', ' ')}"
        if scans > 1:
            line += f"  ⚠️ Попыток: {scans}"
        lines.append(line)
 
    text = "📋 <b>Все билеты</b>\n\n" + "\n\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n\n…и ещё"
 
    await safe_edit(
        call, text,
        markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️  Назад", callback_data="admin_panel")]
        ]),
    )
 
 
@router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    await call.answer("Готовлю файл…")
    fname     = f"tickets_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    csv_bytes = await asyncio.to_thread(export_tickets_csv)
    await bot.send_document(
        chat_id=call.from_user.id,
        document=BufferedInputFile(csv_bytes, filename=fname),
        caption="📤 Экспорт всех билетов",
    )
 
 
# ══════════════════════════════════════════
#  ADMIN: ПРОВЕРКА БИЛЕТОВ НА ВХОДЕ
# ══════════════════════════════════════════
 
@router.callback_query(F.data == "admin_check")
async def cb_admin_check(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminStates.check_mode)
    await safe_edit(
        call,
        "🔍 <b>Проверка билета на входе</b>\n\n"
        "• Пришлите <b>фото QR-кода</b> — бот прочитает сам\n"
        "• Или введите <b>ID билета</b> текстом\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Ожидаю фото или ID:",
        markup=kb_admin_check_back(),
    )
 
 
@router.callback_query(F.data == "admin_search")
async def cb_admin_search(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminStates.search_mode)
    await safe_edit(
        call,
        "🔎 <b>Поиск по @username</b>\n\nВведите @username или часть имени:",
        markup=kb_admin_check_back(),
    )
 
 
@router.message(AdminStates.search_mode, F.text)
async def admin_search_by_name(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    results = find_tickets_by_username(message.text.strip())
    if not results:
        await message.answer(
            f"❌ Билеты для «{message.text.strip()}» не найдены.",
            reply_markup=kb_admin_check_back(),
        )
        return
    lines = []
    for t in results:
        status  = "✅" if t["used"] else "🟢"
        issued  = t["issued_at"][:16].replace("T", " ")
        scans   = t.get("scan_attempts", 0)
        line    = f"{status} <code>{t['id']}</code>\n    👤 {t['username']}  📅 {issued}"
        if t.get("used_at"):
            line += f"\n    🚪 {t['used_at'][:16].replace('T', ' ')}"
        if scans > 1:
            line += f"  ⚠️ Попыток: {scans}"
        lines.append(line)
    await message.answer(
        f"🔎 Найдено {len(results)}:\n\n" + "\n\n".join(lines),
        reply_markup=kb_admin_check_back(),
        parse_mode="HTML",
    )
 
 
@router.message(AdminStates.check_mode, F.photo)
async def admin_check_by_photo(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    photo      = message.photo[-1]
    file       = await bot.get_file(photo.file_id)
    file_bytes = await bot.download_file(file.file_path)
    image_data = file_bytes.read() if hasattr(file_bytes, "read") else bytes(file_bytes)
    ticket_id  = await decode_qr_from_bytes(image_data)
 
    if not ticket_id:
        await message.answer(
            "❌ <b>QR-код не распознан.</b>\n\nПопробуйте чётче или введите ID вручную.",
            reply_markup=kb_admin_check_back(),
            parse_mode="HTML",
        )
        return
    await _process_ticket_check(message, ticket_id)
 
 
@router.message(AdminStates.check_mode, F.text)
async def admin_check_by_text(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await _process_ticket_check(message, message.text.strip().upper())
 
 
async def _process_ticket_check(message: Message, ticket_id: str):
    info = check_ticket(ticket_id)
 
    if info is None:
        await message.answer(
            f"❌ <b>Билет не найден!</b>\n\n"
            f"🎫 ID: <code>{ticket_id}</code>\n\n"
            f"Билета нет в базе. Возможно, введён неверный ID.",
            reply_markup=kb_admin_check_back(),
            parse_mode="HTML",
        )
        return
 
    if info["used"]:
        # Увеличиваем счётчик и уведомляем других админов параллельно
        async with _db_lock:
            _get_db()[ticket_id]["scan_attempts"] = _get_db()[ticket_id].get("scan_attempts", 1) + 1
            await _flush_db()
 
        issued  = info["issued_at"][:16].replace("T", " в ")
        used_at = (info.get("used_at") or "")[:16].replace("T", " в ")
        scans   = _get_db()[ticket_id]["scan_attempts"]
 
        alert_text = (
            f"🚨 <b>ПОПЫТКА ДВОЙНОГО ПРОХОДА!</b>\n\n"
            f"🎫 ID: <code>{ticket_id}</code>\n"
            f"👤 Владелец: {info['username']}\n"
            f"🕐 Билет выдан: {issued}\n"
            f"✅ Впервые использован: {used_at}\n"
            f"⚠️ Всего попыток: <b>{scans}</b>"
        )
        asyncio.create_task(_notify_admins(alert_text, exclude_id=message.from_user.id))
 
        await message.answer(
            f"🚫 <b>ПРОХОД ЗАПРЕЩЁН!</b>\n\n"
            f"🎫 ID: <code>{ticket_id}</code>\n"
            f"👤 Владелец: {info['username']}\n"
            f"🕐 Выдан: {issued}\n"
            f"✅ Уже использован: {used_at}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"❗ <b>НЕ ПРОПУСКАЙТЕ!</b> Это повторная попытка (×{scans}).",
            reply_markup=kb_ticket_action(ticket_id, is_used=True),
            parse_mode="HTML",
        )
        return
 
    await mark_used(ticket_id)
    issued = info["issued_at"][:16].replace("T", " в ")
    await message.answer(
        f"✅ <b>БИЛЕТ ДЕЙСТВИТЕЛЕН!</b>\n\n"
        f"🎫 ID: <code>{ticket_id}</code>\n"
        f"👤 Владелец: {info['username']}\n"
        f"🕐 Выдан: {issued}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎉 <b>Пропускайте!</b> Билет активирован.",
        reply_markup=kb_ticket_action(ticket_id, is_used=True),
        parse_mode="HTML",
    )
 
 
# ══════════════════════════════════════════
#  ADMIN: ПРИНУДИТЕЛЬНО ОТМЕТИТЬ / СНЯТЬ
# ══════════════════════════════════════════
 
@router.callback_query(F.data.startswith("forceuse:"))
async def cb_forceuse(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    ticket_id = call.data.split(":", 1)[1]
    if not check_ticket(ticket_id):
        await call.answer("Билет не найден.", show_alert=True)
        return
    await mark_used(ticket_id)
    await call.answer("✅ Отмечен как использованный.", show_alert=True)
    try:
        await call.message.edit_reply_markup(reply_markup=kb_ticket_action(ticket_id, is_used=True))
    except Exception:
        pass
 
 
@router.callback_query(F.data.startswith("unuse:"))
async def cb_unuse(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа.", show_alert=True)
        return
    ticket_id = call.data.split(":", 1)[1]
    if not check_ticket(ticket_id):
        await call.answer("Билет не найден.", show_alert=True)
        return
    await mark_unused(ticket_id)
    await call.answer("🔄 Отметка снята — билет снова активен.", show_alert=True)
    try:
        await call.message.edit_reply_markup(reply_markup=kb_ticket_action(ticket_id, is_used=False))
    except Exception:
        pass
 
 
# ══════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════
 
async def main():
    _init_fonts()                                          # кэшируем шрифты до старта
    _get_db()                                              # прогреваем кэш БД
    logger.info("Бот запущен ✅")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,                     # игнорируем апдейты за время простоя
        )
    finally:
        await _flush_db()                                  # финальный сброс на диск при остановке
        await bot.session.close()
 
 
if __name__ == "__main__":
    asyncio.run(main())
