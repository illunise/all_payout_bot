import os
import asyncio
import time
import requests
import random
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
    mark_withdraw_processing,
)
from bappaVenture import BA_check_payout_status, BA_check_payin_status, BA_create_payout_order
from wellness import (
    wln_check_payin_status,
    wln_check_payout_payment_status,
    wln_create_payout_payment,
)

from downloader import download_withdraw_csv
from config import *

# ================= CONSTANT =======================

ASK_WITHDRAW_ID = 1
ASK_MERCHANT_ID = 2
ASK_ORDER_ID = 3
ASK_SEND_WITHDRAW_IDS = 4
ASK_SEND_WITHDRAW_GATEWAY = 5

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

# ==================================================

# 🔐 Check permission
def has_permission(user_id, feature):
    return feature in ADMINS.get(user_id, [])


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

    if has_permission(user_id, "ba_payin_status"):
        keyboard.append(
            [InlineKeyboardButton("Payin Status (BappaVenture)", callback_data="ba_payin_status")]
        )

    if has_permission(user_id, "ba_payout_status"):
        keyboard.append(
            [InlineKeyboardButton("Payout Status (BappaVenture)", callback_data="ba_payout_status")]
        )

    if has_permission(user_id, "wln_payin_status"):
        keyboard.append(
            [InlineKeyboardButton("Payin Status (Wellness)", callback_data="wln_payin_status")]
        )

    if has_permission(user_id, "system_info"):
        keyboard.append(
            [InlineKeyboardButton("System Info 🖥", callback_data="system_info")]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Admin Panel\n\nClick below to download withdraw CSV:",
        reply_markup=reply_markup
    )


# Button handler
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    feature = query.data

    await query.answer()

    if not has_permission(user_id, feature):
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

            process_csv_and_save(csv_path)

            await status.edit_text("📤 Sending file...")

            with open(csv_path, "rb") as csv_file:
                await query.message.reply_document(
                    document=csv_file,
                    filename=os.path.basename(csv_path)
                )

            await status.edit_text("✅ CSV Sent Successfully!")

        except Exception as e:
            await status.edit_text(f"❌ Error:\n{str(e)}")

    elif feature == "ba_payin_status":
        await query.edit_message_text("📝 Please enter Merchant ID:")
        return ASK_MERCHANT_ID

    elif feature == "ba_payout_status":
        await query.edit_message_text("📝 Please enter Withdraw ID:")
        return ASK_WITHDRAW_ID

    elif feature == "wln_payin_status":
        await query.edit_message_text("📝 Please enter Order ID:")
        return ASK_ORDER_ID

    # =============================
    # Feature: System Info
    # =============================
    elif feature == "system_info":
        info = f"""
🖥 System Info
-----------------
Working Dir: {os.getcwd()}
Files: {len(os.listdir())}
        """
        await query.edit_message_text(info)

async def handle_withdraw_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payout_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    raw_id = update.message.text.strip()

    if raw_id.startswith("IND-"):
        withdraw_id = raw_id
    else:
        withdraw_id = f"IND-{raw_id}"

    await update.message.reply_text(f"🔍 Checking payout status for ID: {withdraw_id}")

    result = BA_check_payout_status(withdraw_id)

    status = result.get("msg", {}).get("status")

    orderid = result.get("msg", {}).get("orderid")

    bank_acc = result.get("msg", {}).get("account_no")
    ifsc = result.get("msg", {}).get("ifsccode")
    amount = result.get("msg", {}).get("amount")
    bankname = result.get("msg", {}).get("bankname")

    print(result)

    msg = f"*Order ID : {orderid}*\n\n"
    msg += f"==============================\n\n"
    msg += f"*Bank Name:* {bankname}\n"
    msg += f"*IFSC Code:* {ifsc}\n"
    msg += f"*Bank Account No:* {bank_acc}\n"
    msg += f"*Amount:* {amount}\n\n"
    msg += f"==============================\n\n"


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

async def handle_merchant_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "ba_payin_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    merchant_id = update.message.text.strip()

    sent_message = await update.message.reply_text(f"🔍 Checking payin status for Merchant ID: {merchant_id}")

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            BA_check_payin_status,
            merchant_id
        )

        print(result)

        status = result.get("status")
        txn_id = result.get("transactionid")
        amount = result.get("amount")
        utr = result.get("utr")
        txn_datetime = result.get("date")

        msg = f"*Transaction ID : {txn_id}*\n\n"
        msg += "==============================\n\n"
        msg += f"*UTR:* {utr}\n"
        msg += f"*Amount:* ₹{amount}\n"
        msg += f"*Date:* {txn_datetime}\n\n"
        msg += "==============================\n\n"

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

async def handle_order_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not has_permission(user_id, "wln_payin_status"):
        await update.message.reply_text("⛔ You don't have permission.")
        return ConversationHandler.END

    order_id = update.message.text.strip()

    sent_message = await update.message.reply_text(
        f"🔍 Checking Wellness payin status for Order ID: {order_id}"
    )

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            wln_check_payin_status,
            order_id
        )

        print(result)

        status = result.get("data", {}).get("status")
        amount = result.get("data", {}).get("amount")
        utr = result.get("data", {}).get("utr")
        txn_datetime = result.get("data", {}).get("datetime")
        txn_id = result.get("data", {}).get("order_id")

        msg = f"*Order ID : {order_id}*\n\n"
        msg += "==============================\n\n"
        msg += f"*Transaction ID:* {txn_id}\n"
        msg += f"*UTR:* {utr}\n"
        msg += f"*Amount:* ₹{amount}\n"
        msg += f"*Date:* {txn_datetime}\n\n"
        msg += "==============================\n\n"

        if status == "Success":
            msg += "*Status: ✅ Success*\n"
        elif status == "Failed":
            msg += "*Status: ❌ Failed*\n"
        elif status == "Pending":
            msg += "*Status: ⏱ Pending*\n"
        else:
            msg += "*Status: ⚠️ Unknown*\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"❌ Error:\n{str(e)}")

    await sent_message.delete()

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
        "📝 Send withdraw IDs in new lines.\nExample:\nWD-111\nWD-222\nWD-333"
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
        f"Found {len(withdraw_ids)} IDs.\nSelect gateway:",
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
        f"⏳ Creating payouts for {len(withdraw_ids)} withdraw IDs via {payment_method}..."
    )

    rows = get_withdraws_by_ids(withdraw_ids)
    row_map = {}
    for row in rows:
        row_map[row[0]] = row

    success_items = []
    failed_items = []

    # Load numbers & emails once
    try:
        numbers_list = load_file_lines("datas/mobile_numbers.txt")
        emails_list = load_file_lines("datas/gmail_ids.txt")
    except Exception as e:
        await query.message.reply_text(f"❌ Setup error: {str(e)}")
        context.user_data.pop("sendwithdraw_ids", None)
        return ConversationHandler.END

    for idx, wd_id in enumerate(withdraw_ids, start=1):
        row = row_map.get(wd_id)
        if not row:
            failed_items.append(f"{wd_id} -> Not found in DB")
            continue

        _, beneficiary_name, account_number, ifsc_code, amount, status, _, _ = row

        if status not in (0, 1):
            failed_items.append(f"{wd_id} -> Status {status}, skipped")
            continue

        try:
            bank_name = get_bank_name_from_ifsc(ifsc_code)

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
                response = BA_create_payout_order(
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
                    continue

                if response.get("error"):
                    failed_items.append(f"{wd_id} -> BA API error: {response.get('error')}")
                    continue

                msg_data = response.get("msg", {})
                if not isinstance(msg_data, dict):
                    failed_items.append(f"{wd_id} -> BA invalid msg format")
                    continue

                ba_status = str(msg_data.get("status", "")).strip()
                if ba_status not in ("0", "1"):
                    failed_items.append(f"{wd_id} -> BA rejected: {response}")
                    continue

                order_id = msg_data.get("orderid") or response.get("orderid") or request_order_id

            elif selected_gateway == "wln":
                request_order_id = wd_id if wd_id.startswith("WLN-") else f"WLN-{wd_id}"
                payout_id = f"PORD_{int(time.time() * 1000)}"
                response = wln_create_payout_payment(
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
                    continue

                if response.get("error"):
                    failed_items.append(f"{wd_id} -> WLN API error: {response.get('error')}")
                    continue

                if response.get("status") is not True:
                    failed_items.append(f"{wd_id} -> WLN rejected: {response}")
                    continue

                gateway_status = response.get("gateway", {}).get("gateway_status")
                if gateway_status not in ("Completed", "Pending"):
                    failed_items.append(f"{wd_id} -> WLN gateway failed: {response}")
                    continue

                order_id = response.get("payout_id") or response.get("order_id")

            else:
                failed_items.append(f"{wd_id} -> Invalid gateway selected")
                continue

            if not order_id:
                failed_items.append(f"{wd_id} -> Missing order ID in API response")
                continue

            mark_withdraw_processing(wd_id, order_id, payment_method)
            success_items.append(f"{wd_id} -> {order_id}")

        except Exception as e:
            failed_items.append(f"{wd_id} -> {str(e)}")

    result_parts = [
        f"✅ Gateway: {payment_method}",
        f"Total input: {len(withdraw_ids)}",
        f"Success: {len(success_items)}",
        f"Failed: {len(failed_items)}",
    ]

    if success_items:
        result_parts.append("\nSuccessful creations:")
        result_parts.extend(success_items[:50])
        if len(success_items) > 50:
            result_parts.append(f"...and {len(success_items) - 50} more")

    if failed_items:
        result_parts.append("\nFailed:")
        result_parts.extend(failed_items[:20])
        if len(failed_items) > 20:
            result_parts.append(f"...and {len(failed_items) - 20} more")

    await query.message.reply_text("\n".join(result_parts))
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
                "❌ Invalid limit. Use: /pendingwithdraws <amount>\nExample: /pendingwithdraws 5000"
            )
            return

    pending_rows = get_pending_withdraws()

    if not pending_rows:
        await update.message.reply_text("✅ No pending withdraws found.")
        return

    def build_id_chunks(ids, max_chars=3000):
        chunks = []
        current_chunk = []
        current_len = 0

        for wd_id in ids:
            line_len = len(wd_id) + 1
            if current_chunk and current_len + line_len > max_chars:
                chunks.append(current_chunk)
                current_chunk = []
                current_len = 0

            current_chunk.append(wd_id)
            current_len += line_len

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    async def send_copy_blocks(ids, summary_text):
        await update.message.reply_text(summary_text, parse_mode="Markdown")

        if not ids:
            return

        chunks = build_id_chunks(ids)
        for chunk_ids in chunks:
            copy_block = "\n".join(chunk_ids)
            await update.message.reply_text(f"`{copy_block}`", parse_mode="Markdown")

        if len(chunks) > 1:
            full_copy = "'\n" + "\n".join(ids) + "\n'"
            file_data = BytesIO(full_copy.encode("utf-8"))
            file_data.name = "pending_withdraw_ids.txt"
            await update.message.reply_document(
                document=file_data,
                caption="Full ID list in one file."
            )

    if limit is None:
        ids = [wd_id for wd_id, _, _ in pending_rows if wd_id]
        summary = f"📝 *Pending Withdraw IDs*\nTotal: {len(ids)}"
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
        f"Selected: {len(selected)}\n"
        f"Total Amount: ₹{running_total:.2f}\n"
        f"Skipped due to limit: {skipped}"
    )
    await send_copy_blocks(selected, summary)


def process_csv_and_save(csv_path):
    with open(csv_path, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
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

# Run bot
init_db()
app = ApplicationBuilder().token(BOT_TOKEN).build()

conv_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(button_handler)],
    states={
        ASK_WITHDRAW_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_withdraw_id)
        ],
        ASK_MERCHANT_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_merchant_id)
        ],
        ASK_ORDER_ID: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_order_id)
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
app.add_handler(sendwithdraw_conv_handler)
app.add_handler(conv_handler)

print("🤖 Bot is running...")
app.run_polling()
