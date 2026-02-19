BOT_TOKEN = "8598932456:AAHCnZkqfYPJvtkwpyDM7TQtl-wObAF2kH0"

ADMIN_ID = 6169351376

ADMINS = {
    6169351376: ["download_csv", "ba_payout_status", "ba_payin_status", "wln_payin_status", "system_info"],
    8412466614: ["download_csv"],
}

# =============== BAPPAVENTURES ===========
BA_BASE_URL = "https://bappaventures.com"
BA_PAYOUT_STATUS_URL = BA_BASE_URL + "/api/merchantpayouthistory"
BA_PAYIN_STATUS_URL = BA_BASE_URL + "/api/payinstatus"
BA_PAYOUT_ORDER_URL = BA_BASE_URL + "/api/single_transaction"

BA_MERCHANT_ID = "INDIANPAY00INDIANPAY00163"
BA_MERCHANT_TOKEN = "tcu3fMgf8O0T5VT0raOGxiT7VwXZYgOS"

TIMEOUT = 30


# =============== WELLNESS ===========
WLN_BASE_URL = "https://wellnessgrow.in"
WLN_STATUS_PAYIN_CHECK_ENDPOINT = "/api/v1/collection-status-check"
WLN_STATUS_PAYOUT_CHECK_ENDPOINT = "/api/v1/payout-status-check"
WLN_CREATE_PAYOUT_ENDPOINT = "/api/v1/payout"

WLN_MERCHANT_ID = "MID940677"
WLN_API_KEY = "pk_59df0cb68d80ba2d9256b71d85ec719d"
WLN_SECRET_KEY = "sk_9fc46a97e9328563fd03e4b03b2c508ed95b54e7f631166c"
