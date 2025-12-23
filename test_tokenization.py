print("START test_tokenization")

import os
from dotenv import load_dotenv
from security.tokenize import payment_code_token

load_dotenv()

token_key = os.getenv("TOKEN_KEY")
print("TOKEN_KEY exists:", bool(token_key))

if not token_key:
    raise RuntimeError("TOKEN_KEY missing from .env")

code = "RF94907738000300007643365"

t1 = payment_code_token(code, token_key)
t2 = payment_code_token(code, token_key)

print("Token 1:", t1)
print("Token 2:", t2)
print("Same token:", t1 == t2)

print("END test_tokenization")
