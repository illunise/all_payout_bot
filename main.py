import os
import asyncio
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
from database import insert_withdraw, init_db, get_pending_withdraws
from bappaVenture import BA_check_payout_status, BA_check_payin_status
from wellness import wln_check_payin_status, wln_check_payout_payment_status

from downloader import download_withdraw_csv
from config import *

# ================= CONSTANT =======================

ASK_WITHDRAW_ID = 1
ASK_MERCHANT_ID = 2
ASK_ORDER_ID = 3

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

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler(["pendingwithdraws", "pendingwithdraw"], pending_withdraws))
app.add_handler(conv_handler)

print("🤖 Bot is running...")
app.run_polling()
