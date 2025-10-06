# send_telegram.py
import requests
import sys

TOKEN   = "8429976502:AAGApe-HkCmmgEEcbwzEqPZnZzwBsyyobFM" 
CHAT_ID = 302113556                                        

def send_message(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": CHAT_ID, "text": text})
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    msg = " ".join(sys.argv[1:]) or "Ciao da bot 👋"
    print(send_message(msg))
