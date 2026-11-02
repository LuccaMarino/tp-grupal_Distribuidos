import requests
from common.middleware import GroupMember, GroupSender
from common import message_protocol
import logging
import os
import base64
import signal
import time

MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_GROUP = os.environ["OUTPUT_GROUP"]
MAX_RETRIES = int(os.environ.get("MAX_RETRIES"))
RETRY_DELAY = float(os.environ.get("RETRY_DELAY"))
ID = os.environ["ID"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
USER_AGENT = "WikipediaAnalysisBot/1.0 (contact: pciliberto@fi.uba.ar)"
RATE_LIMITING_DELAY = 0.125  # 8 requests per second

class ImageDownloader:
    def __init__(self):
        self.output_group = GroupSender(MOM_HOST, OUTPUT_GROUP, GATEWAY_AMOUNT)
        self.input_group_member = GroupMember(MOM_HOST, INPUT_GROUP, int(ID))
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, _signum, _frame):
        logging.info("Received SIGTERM signal")
        self.input_group_member.stop_consuming_threadsafe()

    def _close(self):
        for resource in (self.input_group_member,self.output_group,):
            try:
                resource.close()
            except Exception as e:
                logging.error(e)

    def download_image(self, url)-> bytes:
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers={"User-Agent": USER_AGENT},timeout=15,)
                response.raise_for_status()
                return True, response.content
            except requests.HTTPError as e:
                status_code = e.response.status_code
                if status_code not in {429, 500, 502, 503, 504}:
                    logging.error("ERROR for URL %s: HTTP status code %d", url, status_code)
                    return False, "ERROR: HTTP Error"
                if status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else max(3.0, RETRY_DELAY * (2 ** attempt))
                else:
                    delay = RETRY_DELAY * (2 ** attempt)
                if attempt == (MAX_RETRIES - 1):
                    logging.error("Max retries exceeded for URL %s", url)
                    return False, "ERROR: HTTP max retries exceeded"
                delay = RETRY_DELAY * (2 ** attempt)
            except requests.Timeout:
                if attempt == (MAX_RETRIES - 1):
                    logging.error("Max retries exceeded for URL %s", url)
                    return False, "ERROR: HTTP max retries exceeded"
                delay = RETRY_DELAY * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay) 
        return False, "ERROR: HTTP max retries exceeded"

    def consume_eof(self, client_id, sender):
        logging.info("Received EOF message")
        sender_id = f"{ID},{sender}"
        eof_query_result = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q2, sender_id)
        self.output_group.broadcast(eof_query_result)
        logging.info("Sent EOF message to output group")

    def consume_image(self, client_id, fields):
        logging.info("Received image download request")
        try:
            for name, url, image_url in fields:
                success, image_content = self.download_image(image_url)
                if success:
                    image_content_b64 = base64.b64encode(image_content).decode("ascii")
                else:
                    image_content_b64 = image_content
                self.output_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q2, [[name, url, image_content_b64]]))
                time.sleep(RATE_LIMITING_DELAY)
        except requests.HTTPError as e:
            logging.warning("Downloading image failed for %s (%s): %s", url, image_url, e)
        except Exception:
            logging.exception("Error processing image download request")

    def consume_message(self, message, ack, _nack):
        try:
            msg_type, client_id, fields = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.EOF:
                sender = fields[0]
                self.consume_eof(client_id, sender)
            elif msg_type == message_protocol.internal.MsgType.DATA:
                self.consume_image(client_id, fields)
            else:
                logging.error("Unexpected message type for image download request")
                raise ValueError("Unexpected message type for image download request")
            ack()
        except Exception:
            logging.exception("Error processing image download request")
            _nack()

    def start(self):
        try:
            self.input_group_member.start_consuming(self.consume_message)
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting image downloader service...")
    image_downloader = ImageDownloader()
    image_downloader.start()

if __name__ == "__main__":
    main()