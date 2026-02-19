from config import *
import requests
import json
import base64
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

def BA_create_payout_order(orderid: str,
                           acc_no: str,
                           ifsc: str,
                           amount: int,
                           bankname: str,
                           name: str,
                           contact: str,
                           email: str) -> Dict:

    try:
        # 🔹 Step 1: Prepare payload (ALL values must be strings)
        payload = {
            "merchant_id": str(BA_MERCHANT_ID),
            "merchant_token": str(BA_MERCHANT_TOKEN),
            "account_no": str(acc_no),
            "ifsccode": str(ifsc),
            "amount": str(amount),  # must be string
            "bankname": str(bankname),
            "remark": "Payment",
            "orderid": str(orderid),
            "name": str(name),
            "contact": str(contact),
            "email": str(email)
        }

        # 🔹 Step 2: Convert to JSON string
        json_payload = json.dumps(payload)

        # 🔹 Step 3: Encode to Base64
        encoded_payload = base64.b64encode(json_payload.encode("utf-8")).decode("utf-8")

        # 🔹 Step 4: Send in "salt"
        response = requests.post(
            BA_PAYOUT_ORDER_URL,
            json={   # Send JSON body
                "salt": encoded_payload
            },
            timeout=30
        )

        return response.json()

    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "error_type": "timeout",
            "error": "Payout API timed out."
        }

    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "error_type": "request_exception",
            "error": str(exc)
        }

    except Exception as e:
        return {
            "status": "error",
            "error_type": "unknown",
            "error": str(e)
        }