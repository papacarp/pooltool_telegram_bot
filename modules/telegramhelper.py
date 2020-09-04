import requests
import json
import urllib
import io

import modules.common as c


class TelegramHelper:
    def __init__(self):
        self.TOKEN = open('files/token', 'r').read()
        self.URL = "https://api.telegram.org/bot{}/".format(self.TOKEN)
        self.s = requests.session()

    def get_url(self, url):
        try:
            response = self.s.get(url)
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

    def send_message(self, text, chat_id, reply_markup=None, silent=None, disable_web_preview=True):
        if c.DEBUG:
            millis = c.get_current_time_millis()

        text = urllib.parse.quote_plus(text)
        url = self.URL + "sendMessage?text={}&chat_id={}&parse_mode=Markdown".format(text, chat_id)
        if reply_markup:
            url += "&reply_markup={}".format(reply_markup)
        if silent:
            url += f"&disable_notification={silent}"
        if disable_web_preview:
            url += f"&disable_web_page_preview={disable_web_preview}"
        self.get_url(url)

        if c.DEBUG:
            print(f"Sending message: {c.get_current_time_millis() - millis}")

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

    def send_image_remote_file(self, img_url, chat_id, photo_name):
        url = f"{self.URL}sendPhoto"
        remote_image = requests.get(img_url)
        photo = io.BytesIO(remote_image.content)
        photo.name = photo_name
        files = {'photo': photo}
        data = {'chat_id': chat_id}
        r = requests.post(url, files=files, data=data)

    def send_image(self, image, chat_id):
        if c.DEBUG:
            millis = c.get_current_time_millis()

        url = f"{self.URL}sendPhoto"
        files = {'photo': image}
        data = {'chat_id': chat_id}
        r = requests.post(url, files=files, data=data)
        print(r.status_code, r.reason, r.content)

        if c.DEBUG:
            print(f"Sending photo: {c.get_current_time_millis() - millis}")