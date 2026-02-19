from config import *
import requests
from typing import Dict

def BA_check_payout_status(orderid: str) -> Dict:
    try:
        r = requests.get(
            BA_PAYOUT_STATUS_URL,
            params={
                "merchantid": BA_MERCHANT_ID,
                "token": BA_MERCHANT_TOKEN,
                "orderid": orderid,
                "limit": 1
            },
            timeout=TIMEOUT
        )
        return _safe_json(r)
    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "error_type": "timeout",
            "error": f"Payout status API timed out after {TIMEOUT} seconds."
        }
    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "error_type": "request_exception",
            "error": str(exc)
        }

def _safe_json(response: requests.Response) -> Dict:
    try:
        return response.json()
    except ValueError:
        return {
            "status": response.status_code,
            "error": "Invalid JSON",
            "raw": response.text
        }

def BA_check_payin_status(orderid: str) -> Dict:
    try:
        response = requests.get(
            BA_PAYIN_STATUS_URL,
            params={
                "order_id": orderid
            },
            timeout=TIMEOUT
        )
        return _safe_json(response)
    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "error_type": "timeout",
            "error": f"Pay-in API timed out after {TIMEOUT} seconds."
        }
    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "error_type": "request_exception",
            "error": str(exc)
        }

def BA_create_payout_order(orderid: str, acc_no: str, ifsc: str, amount: int, bankname: str, name: str, contact: str, email: str) -> Dict:
    try:
        response = requests.get(
            BA_PAYOUT_ORDER_URL,
            params={
                "merchant_id": BA_MERCHANT_ID,
                "merchant_token": BA_MERCHANT_TOKEN,
                "account_no": acc_no,
                "ifsccode": ifsc,
                "amount": amount,
                "bankname": bankname,
                "remark": "Payment",
                "orderid": orderid,
                "name": name,
                "contact": contact,
                "email": email
            },
            timeout=TIMEOUT
        )
        return _safe_json(response)
    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "error_type": "timeout",
            "error": f"Pay-in API timed out after {TIMEOUT} seconds."
        }
    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "error_type": "request_exception",
            "error": str(exc)
        }