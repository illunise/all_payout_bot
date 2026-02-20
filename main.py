import os
import asyncio
import time
import requests
import random
import logging
from math import isfinite
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)
import csv
from database import (
    insert_withdraw,
    init_db,
    get_pending_withdraws,
    get_withdraws_by_ids,
    get_withdraw_by_id,
    mark_withdraw_processing,
    get_processing_withdraws,
    update_withdraw_status,
)
from bappaVenture import BA_check_payout_status, BA_check_payin_status, BA_create_payout_order
from wellness import (
    wln_check_payin_status,
    wln_check_payout_payment_status,
    wln_create_payout_payment,
)

from downloader import download_withdraw_csv
from config import *

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ================= CONSTANT =======================

ASK_PAYOUT_ORDER_ID = 1
ASK_PAYIN_ORDER_ID = 2
ASK_SEARCH_WITHDRAW_ID = 3
ASK_SEND_WITHDRAW_IDS = 4
ASK_SEND_WITHDRAW_GATEWAY = 5
PAYOUT_CREATE_DELAY_SEC = 5.0
STATUS_CHECK_DELAY_SEC = 5.0

# ==================================================


# ================= HELPERS =======================

def get_bank_name_from_ifsc(ifsc_code: str) -> str:
    url = f"https://ifsc.razorpay.com/{ifsc_code}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises error for bad HTTP status
    except requests.RequestException as e:
        raise RuntimeError(f"Network error: {e}")

    try:
        data = response.json()
    except ValueError:
        raise RuntimeError("Invalid JSON response received")

    bank_name = data.get("BANK")

    if not bank_name:
        raise RuntimeError("Bank name not found in response")

    return bank_name

def load_file_lines(filepath: str) -> list:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        raise RuntimeError(f"{filepath} not found")

    if not lines:
        raise RuntimeError(f"{filepath} is empty")

    return lines


def split_text_chunks(text: str, max_chars: int = 3500) -> list:
    if not text:
        return []

    chunks = []
    current = []
    current_len = 0

    for line in text.splitlines():
        line_with_newline = line + "\n"
        line_len = len(line_with_newline)

        if current and current_len + line_len > max_chars:
            chunks.append("".join(current).rstrip())
            current = []
            current_len = 0

        current.append(line_with_newline)
        current_len += line_len

    if current:
        chunks.append("".join(current).rstrip())

    return chunks


async def send_ids_txt(reply_target, ids: list, filename: str, caption: str) -> None:
    if not ids:
        return

    file_content = "\n".join(str(x) for x in ids if x is not None and str(x).strip())
    if not file_content:
        return

    file_data = BytesIO(file_content.encode("utf-8"))
    file_data.name = filename
    await reply_target.reply_document(document=file_data, caption=caption)

# ==================================================

# 🔐 Check permission
def has_permission(user_id, feature):
    return feature in ADMINS.get(user_id, [])


def can_check_payin(user_id) -> bool:
    return has_permission(user_id, "ba_payin_status") or has_permission(user_id, "wln_payin_status")


def can_check_payout(user_id) -> bool:
    return has_permission(user_id, "ba_payout_status")


def detect_payin_gateway(order_id: str) -> str:
    oid = (order_id or "").strip().upper()
    if oid.startswith("WLN") or oid.startswith("WNL"):
        return "wln"
    return "ba"


def detect_payout_gateway(order_id: str) -> str:
    oid = (order_id or "").strip().upper()
    if oid.startswith("PORD_") or oid.startswith("WLN") or oid.startswith("WNL"):
        return "wln"
    return "ba"


def format_withdraw_status(status_code) -> str:
    mapping = {
        0: "Created",
        1: "Processing",
        2: "Success",
        3: "Failed",
    }
    return mapping.get(status_code, f"Unknown ({status_code})")


# /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id not in ADMINS:
        await update.message.reply_text("⛔ You are not authorized.")
        return

    keyboard = []

    # Show only allowed features
    if has_permission(user_id, "download_csv"):
        keyboard.append(
            [InlineKeyboardButton("Download CSV 📥", callback_data="download_csv")]
        )

    if can_check_payin(user_id):
        keyboard.append(
            [InlineKeyboardButton("Payin Check", callback_data="payin_status")]
        )

    if can_check_payout(user_id):
        keyboard.append(
            [InlineKeyboardButton("Payout Check", callback_data="payout_status")]
        )
        keyboard.append(
            [InlineKeyboardButton("Search Withdraw ID", callback_data="search_withdraw")]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🛠 *Admin Panel*\n\nChoose an action:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )


# Button handler
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    feature = query.data

    await query.answer()

    if feature == "payin_status":
        if not can_check_payin(user_id):
            await query.edit_message_text("⛔ You don't have permission for this feature.")
            return
    elif feature == "payout_status":
        if not can_check_payout(user_id):
            await query.edit_message_text("⛔ You don't have permission for this feature.")
            return
    elif feature == "search_withdraw":
        if not can_check_payout(user_id):
            await query.edit_message_text("⛔ You don't have permission for this feature.")
            return
    elif not has_permission(user_id, feature):
        await query.edit_message_text("⛔ You don't have permission for this feature.")
        return

    # =============================
    # Feature: Download CSV
    # =============================
    if feature == "download_csv":
        status = await query.edit_message_text("🔐 Logging in...")

        try:
            loop = asyncio.get_event_loop()
            csv_path = await loop.run_in_executor(
                None, download_withdraw_csv
            )

            total_ids = process_csv_and_save(csv_path)

            await status.edit_text("📤 Sending file...")

            with open(csv_path, "rb") as csv_file:
                await query.message.reply_document(
                    document=csv_file,
                    filename=os.path.basename(csv_path)
                )

            await status.edit_text(f"{total_ids} IDs Saved in Database\n\n✅ CSV Sent Successfully!")

        except Exception as e:
            await status.edit_text(f"❌ Error:\n{str(e)}")

    elif feature == "payin_status":
        await query.edit_message_text("📝 Please enter Payin Order ID:")
        return ASK_PAYIN_ORDER_ID

    elif feature == "payout_status":
        await query.edit_message_text("📝 Please enter Payout Order ID:")
        return ASK_PAYOUT_ORDER_ID

    elif feature == "search_withdraw":
        await query.edit_message_text("📝 Please enter Withdraw ID:")
        return ASK_SEARCH_WITHDRAW_ID


async def handle_payout_order_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not can_check_payout(user_id):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    raw_order_id = update.message.text.strip()
    gateway = detect_payout_gateway(raw_order_id)

    if gateway == "wln":
        order_id = raw_order_id
    else:
        order_id = raw_order_id if raw_order_id.startswith("IND-") else f"IND-{raw_order_id}"

    await update.message.reply_text(f"🔍 Detected `{gateway.upper()}` gateway. Checking payout status for: {order_id}", parse_mode="Markdown")

    if gateway == "wln":
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, wln_check_payout_payment_status, order_id)
        except Exception as e:
            await update.message.reply_text(f"❌ Error:\n{str(e)}")
            return ConversationHandler.END

        data_obj = result if isinstance(result, dict) else {}

        status = str(
            data_obj.get("status_code")
            or data_obj.get("status")
            or data_obj.get("payout_status")
            or ""
        ).strip().lower()

        amount = data_obj.get("amount") or "0"
        msg = (
            "💎 *WELLNESS PAYOUT STATUS*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"

            f"🆔 *Order ID:* `{data_obj.get('order_id', 'NA')}`\n"
            f"🆔 *Payout ID:* `{data_obj.get('payout_id', 'NA')}`\n\n"
            f"💰 *Amount:* `₹{amount}`\n"
            f"🕒 *Created At:* `{data_obj.get('created_at', 'NA')}`\n\n"
        )

        if status in ("success", "completed"):
            msg += "📊 *Status:* ✅ *SUCCESS*\n"
        elif status in ("failed", "rejected"):
            msg += "📊 *Status:* ❌ *FAILED*\n"
        elif status in ("pending", "processing", "initiated"):
            msg += "📊 *Status:* ⏳ *PENDING*\n"
        else:
            msg += f"📊 *Status:* ⚠️ *{status.upper() or 'UNKNOWN'}*\n"

        msg += "\n━━━━━━━━━━━━━━━━━━━━━━"

        await update.message.reply_text(msg, parse_mode="Markdown")
        return ConversationHandler.END

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, BA_check_payout_status, order_id)
    except Exception as e:
        await update.message.reply_text(f"❌ Error:\n{str(e)}")
        return ConversationHandler.END

    status = result.get("msg", {}).get("status")

    orderid = result.get("msg", {}).get("orderid")

    bank_acc = result.get("msg", {}).get("account_no")
    ifsc = result.get("msg", {}).get("ifsccode")
    amount = result.get("msg", {}).get("amount")
    bankname = result.get("msg", {}).get("bankname")

    print(result)

    msg = (
        "🏦 *BappaVenture Payout Status*\n\n"
        "============================\n\n"
        f"*Order ID:* `{orderid}`\n\n"
        f"*Bank:* {bankname}\n"
        f"*IFSC:* `{ifsc}`\n"
        f"*Account:* `{bank_acc}`\n"
        f"*Amount:* ₹{amount}\n\n"
        "============================\n\n"
    )


    if status == "1":
        msg += f"*Status: ✅ Success*\n"
    elif status == "3":
        msg += f"*Status: ❌ Failed*\n"
    elif status == "0":
        msg += f"*Status: ⏱ Pending*\n"
    else:
        msg += f"*Status: ⚠️ Unknown*\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


    return ConversationHandler.END

async def handle_payin_order_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not can_check_payin(user_id):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    order_id = update.message.text.strip()
    gateway = detect_payin_gateway(order_id)

    if gateway == "wln" and not has_permission(user_id, "wln_payin_status"):
        await update.message.reply_text("⛔ You don't have Wellness payin permission.")
        return ConversationHandler.END

    if gateway == "ba" and not has_permission(user_id, "ba_payin_status"):
        await update.message.reply_text("⛔ You don't have BappaVenture payin permission.")
        return ConversationHandler.END

    sent_message = await update.message.reply_text(
        f"🔍 Detected `{gateway.upper()}` gateway. Checking payin status for Order ID: {order_id}",
        parse_mode="Markdown"
    )

    try:
        loop = asyncio.get_event_loop()
        if gateway == "wln":
            result = await loop.run_in_executor(None, wln_check_payin_status, order_id)
        else:
            result = await loop.run_in_executor(None, BA_check_payin_status, order_id)

        print(result)

        if gateway == "wln":
            status = result.get("data", {}).get("status")
            amount = result.get("data", {}).get("amount")
            utr = result.get("data", {}).get("utr")
            txn_datetime = result.get("data", {}).get("datetime")
            txn_id = result.get("data", {}).get("order_id")

            msg = (
                "💠 *Wellness Payin Status*\n\n"
                "============================\n\n"
                f"*Order ID:* `{order_id}`\n\n"
                f"*Transaction ID:* `{txn_id}`\n"
                f"*UTR:* `{utr}`\n"
                f"*Amount:* ₹{amount}\n"
                f"*Date:* {txn_datetime}\n\n"
                "============================\n\n"
            )

            if status == "Success":
                msg += "*Status: ✅ Success*\n"
            elif status == "Failed":
                msg += "*Status: ❌ Failed*\n"
            elif status == "Pending":
                msg += "*Status: ⏱ Pending*\n"
            else:
                msg += "*Status: ⚠️ Unknown*\n"
        else:
            status = result.get("status")
            txn_id = result.get("transactionid")
            amount = result.get("amount")
            utr = result.get("utr")
            txn_datetime = result.get("date")

            msg = (
                "💳 *BappaVenture Payin Status*\n\n"
                "============================\n\n"
                f"*Order ID:* `{order_id}`\n"
                f"*Transaction ID:* `{txn_id}`\n\n"
                f"*UTR:* `{utr}`\n"
                f"*Amount:* ₹{amount}\n"
                f"*Date:* {txn_datetime}\n\n"
                "============================\n\n"
            )

            if status == "success":
                msg += "*Status: ✅ Success*\n"
            elif status == "failed":
                msg += "*Status: ❌ Failed*\n"
            elif status == "pending":
                msg += "*Status: ⏱ Pending*\n"
            else:
                msg += "*Status: ⚠️ Unknown*\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"❌ Error:\n{str(e)}")

    await sent_message.delete()

    return ConversationHandler.END


async def handle_search_withdraw_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not can_check_payout(user_id):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    withdraw_id = (update.message.text or "").strip()
    if not withdraw_id:
        await update.message.reply_text("❌ Withdraw ID is required. Please enter a valid ID.")
        return ASK_SEARCH_WITHDRAW_ID

    row = get_withdraw_by_id(withdraw_id)
    if not row:
        await update.message.reply_text(f"❌ Withdraw ID not found: `{withdraw_id}`", parse_mode="Markdown")
        return ConversationHandler.END

    (
        withdraw_request_id,
        beneficiary_name,
        account_number,
        ifsc_code,
        amount,
        status,
        order_id,
        payment_method,
        created_at,
        updated_at,
    ) = row

    msg = (
        "💸 *WITHDRAW REQUEST DETAILS*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"🆔 *Withdraw ID:* `{withdraw_request_id}`\n"
        f"📊 *Status:* `{format_withdraw_status(status)}`\n"
        f"💰 *Amount:* `₹{amount:,.2f}`\n\n"

        "👤 *Beneficiary Information*\n"
        "──────────────────\n"
        f"🏦 *Name:* `{beneficiary_name or 'NA'}`\n"
        f"🔢 *Account No:* `{account_number or 'NA'}`\n"
        f"🏛 *IFSC Code:* `{ifsc_code or 'NA'}`\n"
        f"💳 *Payment Method:* `{payment_method or 'NA'}`\n\n"

        "📦 *Transaction Info*\n"
        "──────────────────\n"
        f"🧾 *Order ID:* `{order_id or 'NA'}`\n"
        f"🕒 *Created At:* `{created_at or 'NA'}`\n"
        f"🔄 *Updated At:* `{updated_at or 'NA'}`\n"

        "\n━━━━━━━━━━━━━━━━━━"
    )

    await update.message.reply_text(msg, parse_mode="Markdown")
    return ConversationHandler.END

def parse_withdraw_ids(raw_text: str):
    lines = raw_text.replace("\r", "\n").split("\n")
    ids = []

    for line in lines:
        wd_id = line.strip().strip("`").strip("'").strip('"').strip(",")
        if wd_id and wd_id not in ids:
            ids.append(wd_id)

    return ids


async def sendwithdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payout_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    context.user_data["sendwithdraw_ids"] = []
    await update.message.reply_text(
        "📝 *Send Withdraw IDs*\n"
        "Send one withdraw ID per line.\n\n"
        "*Example:*\n"
        "`WD-111`\n"
        "`WD-222`\n"
        "`WD-333`",
        parse_mode="Markdown"
    )
    return ASK_SEND_WITHDRAW_IDS


async def handle_sendwithdraw_ids(update: Update, context: ContextTypes.DEFAULT_TYPE):
    withdraw_ids = parse_withdraw_ids(update.message.text or "")

    if not withdraw_ids:
        await update.message.reply_text(
            "❌ No valid IDs found. Send one withdraw ID per line."
        )
        return ASK_SEND_WITHDRAW_IDS

    context.user_data["sendwithdraw_ids"] = withdraw_ids

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("BappaVenture", callback_data="sendwd_gateway:ba")],
            [InlineKeyboardButton("Wellness", callback_data="sendwd_gateway:wln")],
        ]
    )
    await update.message.reply_text(
        f"✅ Found *{len(withdraw_ids)}* IDs.\nSelect gateway:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    return ASK_SEND_WITHDRAW_GATEWAY


async def handle_sendwithdraw_gateway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    if not has_permission(user_id, "ba_payout_status"):
        await query.edit_message_text("⛔ You don't have permission.")
        return ConversationHandler.END

    selected_gateway = query.data.split(":", 1)[1]
    payment_method = None

    if selected_gateway == "ba":
        payment_method = "BappaVenture"
    elif selected_gateway == "wln":
        payment_method = "Wellness"

    withdraw_ids = context.user_data.get("sendwithdraw_ids", [])

    if not withdraw_ids:
        await query.edit_message_text("❌ No withdraw IDs found. Please run /sendwithdraw again.")
        return ConversationHandler.END

    await query.edit_message_text(
        f"⏳ Creating payouts for *{len(withdraw_ids)}* withdraw IDs via *{payment_method}*...",
        parse_mode="Markdown"
    )
    progress_message = query.message

    rows = get_withdraws_by_ids(withdraw_ids)
    row_map = {}
    for row in rows:
        row_map[row[0]] = row

    success_items = []
    failed_items = []
    success_ids = []
    failed_ids = []

    # Load numbers & emails once
    try:
        numbers_list = load_file_lines("datas/mobile_numbers.txt")
        emails_list = load_file_lines("datas/gmail_ids.txt")
    except Exception as e:
        await query.message.reply_text(f"❌ Setup error: {str(e)}")
        context.user_data.pop("sendwithdraw_ids", None)
        return ConversationHandler.END

    loop = asyncio.get_event_loop()
    total_withdraw_ids = len(withdraw_ids)
    processed_count = 0
    progress_step = max(1, total_withdraw_ids // 10)

    for idx, wd_id in enumerate(withdraw_ids, start=1):
        processed_count += 1

        if processed_count == 1 or processed_count % progress_step == 0 or processed_count == total_withdraw_ids:
            try:
                await progress_message.edit_text(
                    "🚀 *PAYOUT CREATION IN PROGRESS*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"

                    "📊 *Processing Overview*\n"
                    f"• Total Requests : `{total_withdraw_ids}`\n"
                    f"• Completed      : `{processed_count}/{total_withdraw_ids}`\n\n"

                    "🆔 *Current Withdraw ID*\n"
                    f"`{wd_id}`\n\n"

                    f"🏦 *Gateway:* `{payment_method}`\n\n"

                    "⏳ *Step 2 of 3*\n"
                    "_Creating payout request at gateway..._\n\n"

                    "━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

        row = row_map.get(wd_id)
        if not row:
            failed_items.append(f"{wd_id} -> Not found in DB")
            failed_ids.append(wd_id)
            continue

        _, beneficiary_name, account_number, ifsc_code, amount, status, _, _ = row

        if status not in (0, 1):
            failed_items.append(f"{wd_id} -> Status {status}, skipped")
            failed_ids.append(wd_id)
            continue

        try:
            if idx > 1 and PAYOUT_CREATE_DELAY_SEC > 0:
                await asyncio.sleep(PAYOUT_CREATE_DELAY_SEC)

            bank_name = await loop.run_in_executor(None, get_bank_name_from_ifsc, ifsc_code)

            if not numbers_list:
                failed_items.append(f"{wd_id} -> No phone numbers left in datas/mobile_numbers.txt")
                continue

            if not emails_list:
                failed_items.append(f"{wd_id} -> No emails left in datas/gmail_ids.txt")
                continue

            # Random pick
            phone_number = random.choice(numbers_list)
            numbers_list.remove(phone_number)

            email_id = random.choice(emails_list)
            emails_list.remove(email_id)

            order_id = None
            response = None

            if selected_gateway == "ba":
                request_order_id = wd_id if wd_id.startswith("IND-") else f"IND-{wd_id}"
                response = await loop.run_in_executor(
                    None,
                    BA_create_payout_order,
                    request_order_id,
                    account_number,
                    ifsc_code,
                    int(float(amount)),
                    bank_name,
                    beneficiary_name or "NA",
                    phone_number,
                    email_id
                )

                if not isinstance(response, dict):
                    failed_items.append(f"{wd_id} -> Invalid BA API response")
                    failed_ids.append(wd_id)
                    continue

                msg_data = response.get("msg", {})
                ba_status_code = str(response.get("status", "")).strip()
                ba_error_text = str(response.get("error", "")).strip().lower()
                ba_accepted = ba_error_text in {
                    "request accepted successfully",
                    "accepted",
                    "success",
                    "ok",
                    "",
                }

                # BA success can come as: {"status": 200, "error": "Request Accepted Successfully"}
                if ba_status_code == "400":
                    failed_items.append(f"{wd_id} -> BA API error: {response.get('error')}")
                    failed_ids.append(wd_id)
                    continue

                if isinstance(msg_data, dict) and msg_data:
                    msg_status = str(msg_data.get("status", "")).strip().lower()
                    ba_msg_success = {"0", "1", "success", "pending", "processing", "true"}
                    ba_msg_failed = {"3", "failed", "false", "rejected", "error", "declined"}

                    if msg_status in ba_msg_failed:
                        failed_items.append(f"{wd_id} -> BA rejected: {response}")
                        failed_ids.append(wd_id)
                        continue
                    if msg_status and msg_status not in ba_msg_success:
                        failed_items.append(f"{wd_id} -> BA unknown status: {response}")
                        failed_ids.append(wd_id)
                        continue

                    order_id = msg_data.get("orderid") or response.get("orderid") or request_order_id
                elif ba_status_code == "200" and ba_accepted:
                    order_id = response.get("orderid") or request_order_id
                else:
                    failed_items.append(f"{wd_id} -> BA invalid response: {response}")
                    failed_ids.append(wd_id)
                    continue

            elif selected_gateway == "wln":
                request_order_id = f"PORD_{int(time.time() * 1000)}_{idx}"
                payout_id = wd_id if wd_id.startswith("WLN-") else f"WLN-{wd_id}"
                response = await loop.run_in_executor(
                    None,
                    wln_create_payout_payment,
                    request_order_id,
                    payout_id,
                    int(float(amount)),
                    account_number,
                    ifsc_code,
                    bank_name,
                    beneficiary_name or "NA",
                    email_id
                )

                if not isinstance(response, dict):
                    failed_items.append(f"{wd_id} -> Invalid WLN API response")
                    failed_ids.append(wd_id)
                    continue

                if response.get("error"):
                    failed_items.append(f"{wd_id} -> WLN API error: {response.get('error')}")
                    failed_ids.append(wd_id)
                    continue

                order_id = response.get("payout_id") or response.get("order_id")

            else:
                failed_items.append(f"{wd_id} -> Invalid gateway selected")
                failed_ids.append(wd_id)
                continue

            if not order_id:
                failed_items.append(f"{wd_id} -> Missing order ID in API response")
                failed_ids.append(wd_id)
                continue

            mark_withdraw_processing(wd_id, order_id, payment_method)
            success_items.append(f"{wd_id} -> {order_id}")
            success_ids.append(wd_id)

        except Exception as e:
            failed_items.append(f"{wd_id} -> {str(e)}")
            failed_ids.append(wd_id)

    result_parts = [
        "📤 *Payout Creation Summary*",
        f"*Gateway:* {payment_method}",
        f"*Total Input:* {len(withdraw_ids)}",
        f"*Success:* {len(success_items)}",
        f"*Failed:* {len(failed_items)}",
    ]

    try:
        await progress_message.edit_text(
            "✅ *Payout Creation Completed*\n"
            f"*Gateway:* {payment_method}\n"
            f"*Processed:* {processed_count}/{total_withdraw_ids}\n"
            "Step 3/3: Final summary sent below.",
            parse_mode="Markdown"
        )
    except Exception:
        pass

    await query.message.reply_text("\n".join(result_parts), parse_mode="Markdown")
    await send_ids_txt(
        query.message,
        success_ids,
        "payout_success_ids.txt",
        "Success withdraw IDs"
    )
    await send_ids_txt(
        query.message,
        failed_ids,
        "payout_failed_ids.txt",
        "Failed withdraw IDs"
    )

    context.user_data.pop("sendwithdraw_ids", None)
    return ConversationHandler.END


async def pending_withdraws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payout_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return

    limit = None
    if context.args:
        try:
            limit = float(context.args[0])
            if not isfinite(limit) or limit < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid limit.\n\nUse: `/pendingwithdraws <amount>`\nExample: `/pendingwithdraws 5000`",
                parse_mode="Markdown"
            )
            return

    pending_rows = get_pending_withdraws()

    if not pending_rows:
        await update.message.reply_text("✅ No pending withdraws found.")
        return

    async def send_copy_blocks(ids, summary_text):
        await update.message.reply_text(summary_text, parse_mode="Markdown")
        await send_ids_txt(
            update.message,
            ids,
            "pending_withdraw_ids.txt",
            "Pending withdraw IDs"
        )

    if limit is None:
        ids = [wd_id for wd_id, _, _ in pending_rows if wd_id]
        summary = f"📝 *Pending Withdraw IDs*\n*Total:* {len(ids)}"
        await send_copy_blocks(ids, summary)
        return

    selected = []
    skipped = 0
    running_total = 0.0
    for wd_id, amount, _ in pending_rows:
        if not wd_id:
            continue
        if running_total + amount <= limit:
            running_total += amount
            selected.append(f"{wd_id}")
        else:
            skipped += 1

    if not selected:
        await update.message.reply_text(
            f"⚠️ No pending withdraw can fit within ₹{limit:.2f}."
        )
        return

    summary = (
        f"📌 *Pending Withdraw IDs within cumulative limit ₹{limit:.2f}*\n\n"
        f"*Selected:* {len(selected)}\n"
        f"*Total Amount:* ₹{running_total:.2f}\n"
        f"*Skipped:* {skipped}"
    )
    await send_copy_blocks(selected, summary)


async def checkstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payout_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return

    processing_rows = get_processing_withdraws()
    if not processing_rows:
        await update.message.reply_text("✅ No processing withdraws found.")
        return

    total_rows = sum(1 for withdraw_id, order_id, payment_method in processing_rows if withdraw_id and order_id and payment_method)
    if total_rows == 0:
        await update.message.reply_text("ℹ️ Processing rows found, but none had valid withdraw_id/order_id/payment_method.")
        return
    progress_message = await update.message.reply_text(
        "🔄 *Check Status Started*\n"
        f"*Total Processing IDs:* {total_rows}\n"
        "Step 1/3: Preparing checks...",
        parse_mode="Markdown"
    )

    ba_success_ids = []
    ba_failed_ids = []
    wln_success_ids = []
    wln_failed_ids = []
    ba_pending_count = 0
    wln_pending_count = 0
    checked_count = 0
    processed_count = 0
    progress_step = max(1, total_rows // 10)

    success_states = {"1", "2", "success", "completed", "approved", "done", "paid", "true"}
    failed_states = {"3", "4", "failed", "failure", "rejected", "cancelled", "canceled", "declined", "false"}
    pending_states = {"0", "pending", "processing", "inprocess", "queued", "initiated"}

    loop = asyncio.get_event_loop()

    for idx, (withdraw_id, order_id, payment_method) in enumerate(processing_rows, start=1):
        if not withdraw_id or not order_id or not payment_method:
            continue

        method = str(payment_method).strip().lower()
        checked_count += 1
        processed_count += 1

        if processed_count == 1 or processed_count % progress_step == 0 or processed_count == total_rows:
            gateway_name = "BappaVenture" if method in ("bappaventure", "ba") else "Wellness" if method in ("wellness", "wln") else "Unknown"
            await progress_message.edit_text(
                "🔄 *PAYOUT STATUS CHECK IN PROGRESS*\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"

                "📊 *Processing Summary*\n"
                f"• Total Requests : `{total_rows}`\n"
                f"• Completed      : `{processed_count}/{total_rows}`\n\n"

                "🆔 *Current Withdraw ID*\n"
                f"`{withdraw_id}`\n\n"

                f"🏦 *Gateway:* `{gateway_name}`\n\n"

                "⏳ *Step 2 of 3*\n"
                "_Checking payout status from gateway..._\n\n"

                "━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode="Markdown"
            )

        try:
            if idx > 1 and STATUS_CHECK_DELAY_SEC > 0:
                await asyncio.sleep(STATUS_CHECK_DELAY_SEC)

            if method in ("bappaventure", "ba"):
                result = await loop.run_in_executor(None, BA_check_payout_status, order_id)
                msg_obj = result.get("msg", {}) if isinstance(result, dict) else {}
                ba_status = str(msg_obj.get("status", "")).strip().lower()

                if not ba_status:
                    ba_status = str(result.get("status", "")).strip().lower()

                if ba_status in success_states:
                    update_withdraw_status(withdraw_id, 2)
                    ba_success_ids.append(withdraw_id)
                elif ba_status in failed_states:
                    update_withdraw_status(withdraw_id, 3)
                    ba_failed_ids.append(withdraw_id)
                else:
                    ba_pending_count += 1

            elif method in ("wellness", "wln"):
                result = await loop.run_in_executor(None, wln_check_payout_payment_status, order_id)
                data_obj = result.get("data", {}) if isinstance(result, dict) else {}
                gateway_obj = result.get("gateway", {}) if isinstance(result, dict) else {}

                wln_status = str(data_obj.get("status", "")).strip().lower()
                if not wln_status:
                    wln_status = str(result.get("status_code", "")).strip().lower()
                if not wln_status:
                    wln_status = str(data_obj.get("payout_status", "")).strip().lower()
                if not wln_status:
                    wln_status = str(gateway_obj.get("gateway_status", "")).strip().lower()
                if not wln_status:
                    wln_status = str(result.get("status", "")).strip().lower()
                if not wln_status:
                    wln_status = str(result.get("message", "")).strip().lower()

                if wln_status in success_states:
                    update_withdraw_status(withdraw_id, 2)
                    wln_success_ids.append(withdraw_id)
                elif wln_status in failed_states:
                    update_withdraw_status(withdraw_id, 3)
                    wln_failed_ids.append(withdraw_id)
                else:
                    wln_pending_count += 1

        except Exception:
            if method in ("bappaventure", "ba"):
                ba_pending_count += 1
            elif method in ("wellness", "wln"):
                wln_pending_count += 1
            continue

    if not ba_success_ids and not ba_failed_ids and not wln_success_ids and not wln_failed_ids:
        await progress_message.edit_text(
            "✅ *Check Status Completed*\n"
            f"*Processed:* {processed_count}/{total_rows}\n"
            "Step 3/3: Final summary sent below.",
            parse_mode="Markdown"
        )
        await update.message.reply_text(
            "ℹ️ No final status update (success/failed) found yet.\n"
            f"*Checked:* {checked_count}\n"
            f"*Pending:* {ba_pending_count + wln_pending_count}\n"
            f"*BappaVenture Pending:* {ba_pending_count}\n"
            f"*Wellness Pending:* {wln_pending_count}",
            parse_mode="Markdown"
        )
        return

    async def send_copy_blocks(title, ids):
        if not ids:
            return
        safe_name = title.lower().replace(" ", "_")
        await send_ids_txt(
            update.message,
            ids,
            f"{safe_name}.txt",
            f"{title} ({len(ids)})"
        )

    async def send_gateway_report(gateway_name, success_ids, failed_ids):
        summary = (
            f"*{gateway_name}*\n"
            f"✅ Success: {len(success_ids)}\n"
            f"❌ Failed: {len(failed_ids)}"
        )
        await update.message.reply_text(summary, parse_mode="Markdown")
        await send_copy_blocks(f"{gateway_name} Success IDs", success_ids)
        await send_copy_blocks(f"{gateway_name} Failed IDs", failed_ids)

    if ba_success_ids or ba_failed_ids:
        await send_gateway_report("BappaVenture", ba_success_ids, ba_failed_ids)

    if wln_success_ids or wln_failed_ids:
        await send_gateway_report("Wellness", wln_success_ids, wln_failed_ids)

    await progress_message.edit_text(
        "✅ *Check Status Completed*\n"
        f"*Processed:* {processed_count}/{total_rows}\n"
        "Step 3/3: Final summary sent below.",
        parse_mode="Markdown"
    )


async def pending_ids(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payout_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return

    processing_rows = get_processing_withdraws()
    if not processing_rows:
        await update.message.reply_text("✅ No pending IDs found.")
        return

    ba_ids = []
    wln_ids = []
    unknown_ids = []

    for withdraw_id, _, payment_method in processing_rows:
        if not withdraw_id:
            continue

        method = str(payment_method or "").strip().lower()
        if method in ("bappaventure", "ba"):
            ba_ids.append(withdraw_id)
        elif method in ("wellness", "wln"):
            wln_ids.append(withdraw_id)
        else:
            unknown_ids.append(withdraw_id)

    async def send_pending_group(gateway_name, ids):
        if not ids:
            return

        summary = (
            f"📌 *{gateway_name} Pending IDs*\n\n"
            f"*Total:* {len(ids)}"
        )
        await update.message.reply_text(summary, parse_mode="Markdown")
        safe_gateway = gateway_name.lower().replace(" ", "_")
        await send_ids_txt(
            update.message,
            ids,
            f"{safe_gateway}_pending_ids.txt",
            f"{gateway_name} pending IDs"
        )

    if ba_ids:
        await send_pending_group("BappaVenture", ba_ids)

    if wln_ids:
        await send_pending_group("Wellness", wln_ids)

    if unknown_ids:
        await send_pending_group("Unknown Gateway", unknown_ids)


def process_csv_and_save(csv_path):
    total_ids = 0
    with open(csv_path, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            total_ids += 1
            data = {
                "withdraw_request_id": row["Withdraw Request Id"],
                "beneficiary_name": row["Benificiary Name"],
                "account_number": row["Benificiary Account number"],
                "ifsc_code": row["IFSC Code"],
                "amount": float(row["Amount"]),
                "status": 0,
                "order_id": "",
                "payment_method": ""
            }

            insert_withdraw(data)

    return total_ids


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception while processing update", exc_info=context.error)
    try:
        if update and getattr(update, "effective_chat", None):
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Something went wrong while processing your request. Please try again."
            )
    except Exception:
        pass

# Run bot
init_db()
app = ApplicationBuilder().token(BOT_TOKEN).build()

conv_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(button_handler)],
    states={
        ASK_PAYOUT_ORDER_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payout_order_id)
        ],
        ASK_PAYIN_ORDER_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payin_order_id)
        ],
        ASK_SEARCH_WITHDRAW_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_withdraw_id)
        ],
    },
    fallbacks=[],
)

sendwithdraw_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("sendwithdraw", sendwithdraw_start)],
    states={
        ASK_SEND_WITHDRAW_IDS: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sendwithdraw_ids)
        ],
        ASK_SEND_WITHDRAW_GATEWAY: [
            CallbackQueryHandler(handle_sendwithdraw_gateway, pattern=r"^sendwd_gateway:")
        ],
    },
    fallbacks=[],
)

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler(["pendingwithdraws", "pendingwithdraw"], pending_withdraws))
app.add_handler(CommandHandler("pendingids", pending_ids))
app.add_handler(CommandHandler("checkstatus", checkstatus))
app.add_handler(sendwithdraw_conv_handler)
app.add_handler(conv_handler)
app.add_error_handler(error_handler)

print("🤖 Bot is running...")
app.run_polling()
