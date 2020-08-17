import requests
import json
import urllib


class TelegramHelper:
    def __init__(self):
        self.TOKEN = open('files/token', 'r').read()
        self.URL = "https://api.telegram.org/bot{}/".format(self.TOKEN)

    def get_url(self, url):
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            return ''
        content = response.content.decode("utf8")
        return content

    def get_json_from_url(self, url):
        content = self.get_url(url)
        if content == '':
            return
        js = json.loads(content)
        return js

    def get_updates(self, offset=None):
        url = self.URL + "getUpdates"
        if offset:
            url += "?offset={}".format(offset)
        url += "?timeout={}".format(100)
        js = self.get_json_from_url(url)
        return js

    def get_last_update_id(self, updates):
        update_ids = []
        for update in updates["result"]:
            update_ids.append(int(update["update_id"]))
        return max(update_ids)

    def send_message(self, text, chat_id, reply_markup=None, silent=None, disable_web_preview=None):
        text = urllib.parse.quote_plus(text)
        url = self.URL + "sendMessage?text={}&chat_id={}&parse_mode=Markdown".format(text, chat_id)
        if reply_markup:
            url += "&reply_markup={}".format(reply_markup)
        if silent:
            url += f"&disable_notification={silent}"
        if disable_web_preview:
            url += f"&disable_web_page_preview={disable_web_preview}"
        self.get_url(url)

    def build_keyboard(self, items):
        keyboard = []
        columns = 2
        tmp = []
        for i in items:
            tmp.append(i)
            if columns / len(tmp) == 1:
                keyboard.append(tmp)
                tmp = []
        if tmp:
            keyboard.append(tmp)
        reply_markup = {"keyboard": keyboard, "one_time_keyboard": True}
        return json.dumps(reply_markup)

    def remove_keyboard(self, boolean):
        remove_keyboard_reply = {"remove_keyboard": boolean}
        return json.dumps(remove_keyboard_reply)
