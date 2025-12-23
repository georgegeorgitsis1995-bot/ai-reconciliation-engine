import hmac
import hashlib

def normalize_payment_code(code: str) -> str:
    return code.strip().upper()

def payment_code_token(code: str, token_key: str) -> str:
    """
    Deterministic token for matching across collections.
    Same input + same key => same output.
    """
    norm = normalize_payment_code(code)
    digest = hmac.new(
        key=token_key.encode("utf-8"),
        msg=norm.encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()
    return digest
