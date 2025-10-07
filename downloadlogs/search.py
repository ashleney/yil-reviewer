import re
import sys
import requests
import time
from config import headers

url = "https://discord.com/api/v9/guilds/563730522736689153/messages/search"


offset = 0
pattern = re.compile(r"https://mahjongsoul\.game\.yo-star\.com/\?paipu=[\w-]+")  # regex for the URL

while True:
    params = {
        "channel_id": "1394370118318030888",
        "content": "mahjongsoul.game.yo-star.com",
        "sort_by": "timestamp",
        "sort_order": "desc",
        "offset": offset
    }

    print(params, file=sys.stderr)
    response = requests.get(url, headers=headers, params=params)
    print(response.text, file=sys.stderr)
    response.raise_for_status()
    data = response.json()

    messages = data["messages"]
    if not messages:
        break

    for message in messages:
        for url_match in pattern.findall(message[0]["content"]):
            print(url_match)

    offset += 25
    time.sleep(2)
