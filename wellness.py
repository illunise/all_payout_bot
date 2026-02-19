import requests
from config import *

def wln_check_payin_status(order_id: str) -> dict:
    """
    Check status of a payment using the original order_id.
    Returns the JSON response or raises on errors.
    """
    url = WLN_BASE_URL + WLN_STATUS_PAYIN_CHECK_ENDPOINT

    payload = {
        "merchant_id": WLN_MERCHANT_ID,
        "api_key": WLN_API_KEY,
        "secret_key": WLN_SECRET_KEY,
        "order_id": order_id,
    }

    headers = {
        "Content-Type": "application/json"
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

    data = resp.json()

    if not data.get("status", False):
        # For 404 or other logical errors, API returns status=false with error
        raise RuntimeError(f"API error: {data}")

    return data

def wln_check_payout_payment_status(payout_id: str) -> dict:
    """
    Check status of a payment using the original order_id.
    Returns the JSON response or raises on errors.
    """

    url = WLN_BASE_URL + WLN_STATUS_PAYOUT_CHECK_ENDPOINT

    payload = {
        "merchant_id": WLN_MERCHANT_ID,
        "api_key": WLN_API_KEY,
        "secret_key": WLN_SECRET_KEY,
        "payout_id": payout_id,
    }

    headers = {
        "Content-Type": "application/json"
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

    data = resp.json()

    if not data.get("status", False):
        # For 404 or other logical errors, API returns status=false with error
        raise RuntimeError(f"API error: {data}")

    return data


import requests
from typing import Dict

def wln_create_payout_payment(
    order_id: str,
    payout_id: str,
    amount: float,
    account_number: str,
    ifsc_code: str,
    bank_name: str,
    name: str,
    email: str,
) -> Dict:
    """
    Create a payout request.
    Returns parsed JSON response.
    Raises RuntimeError on HTTP/API errors.
    """

    url = f"{WLN_BASE_URL}{WLN_CREATE_PAYOUT_ENDPOINT}"

    payload = {
        "merchant_id": WLN_MERCHANT_ID,
        "api_key": WLN_API_KEY,
        "secret_key": WLN_SECRET_KEY,
        "order_id": order_id,
        "payout_id": payout_id,
        "amount": float(amount),  # ensure numeric
        "account_number": account_number,
        "ifsc_code": ifsc_code,
        "bank_name": bank_name,
        "bene_name": name,
        "email": email,
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error while creating payout: {str(e)}")

    # {
    #     "status": true,
    #     "message": "Fund transfer initiated successfully",
    #     "order_id": "WLN-WD-34498-0000-337",
    #     "payout_id": "PORD_1771505679478",
    #     "amount": 400,
    #     "charge": 19.5,
    #     "gst": 3.51,
    #     "total_debit": 423.01,
    #     "gateway_ref": "SMPPO1771505679586",
    #     "gateway": {
    #         "status": true,
    #         "message": "Fund transfer initiated successfully",
    #         "gateway_ref": "SMPPO1771505679586",
    #         "gateway_status": "Completed",
    #         "raw_response": {
    #             "success": true,
    #             "message": "Fund transfer initiated successfully",
    #             "txnId": "SMPPO1771505679586",
    #             "status": "Completed",
    #             "amount": "400.00",
    #             "custRefNo": "WLN-WD-34498-0000-337",
    #             "beneficiary": {
    #                 "name": "bhautik",
    #                 "accountNumber": "91190100003356",
    #                 "ifscCode": "BARB0DBAKHI",
    #                 "bankName": "BANK OF BARODA"
    #             }
    #         }
    #     }
    # }

    # Check HTTP status
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

    # Parse JSON safely
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Invalid JSON response: {resp.text}")

    # Check main API status
    if not data.get("status"):
        raise RuntimeError(f"API Error: {data.get('message', 'Unknown error')}")

    # Optional: Validate gateway status
    gateway_status = (
        data.get("gateway", {})
        .get("gateway_status")
    )

    if gateway_status not in ["Completed", "Pending"]:
        raise RuntimeError(f"Gateway Failed: {data}")

    return data
