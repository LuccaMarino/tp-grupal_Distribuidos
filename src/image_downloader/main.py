import requests
from common.middleware import GroupMember, GroupSender
from common import message_protocol
import logging
import os
import base64
import signal

MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_GROUP = os.environ["OUTPUT_GROUP"]
ID = os.environ["ID"]
GATEWAY_AMMOUNT = int(os.environ["GATEWAY_AMMOUNT"])
USER_AGENT = (
    "ImageDownloaderTP/"
    "0.1 (FIUBA Sistemas Distribuidos)"
)

class ImageDownloader:
    def __init__(self):
        self.output_group = GroupSender(MOM_HOST, OUTPUT_GROUP, GATEWAY_AMMOUNT)
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
        response = requests.get(url, headers={"User-Agent": USER_AGENT},timeout=15,)
        response.raise_for_status()
        return response.content

    def consume_eof(self, client_id):
        logging.info("Received EOF message")
        eof_query_result = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q2, ID)
        self.output_group.broadcast(eof_query_result)
        logging.info("Sent EOF message to output group")

    def consume_image(self, client_id, fields):
        logging.info("Received image download request")
        try:
            for name, url, image_url in fields:
                image_content = self.download_image(image_url)
                image_content_b64 = base64.b64encode(image_content).decode("ascii")
                self.output_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q2, [name, url, image_content_b64]))
                logging.info(f"Downloaded image size: {len(image_content)} bytes" + f" for article {name} ({url})")
        except requests.HTTPError as e:
            logging.warning("Downloading image failed for %s (%s): %s", url, image_url, e)
        except Exception:
            logging.exception("Error processing image download request")

    def consume_message(self, message, ack, _nack):
        try:
            msg_type, client_id, fields = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.EOF:
                self.consume_eof(client_id)
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