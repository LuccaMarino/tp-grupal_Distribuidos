import requests
from common.middleware import GroupMember, GroupSender
from common import message_protocol
import logging
import os
import base64

MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_GROUP = os.environ["OUTPUT_GROUP"]
ID = os.environ["ID"]
GATEWAY_AMMOUNT = int(os.environ["GATEWAY_AMMOUNT"])
USER_AGENT = (
    "ImageDownloaderTP/"
    "0.1 (FIUBA Sistemas Distribuidos)"
)


def download_image(url)-> bytes:
    response = requests.get(url, headers={"User-Agent": USER_AGENT},timeout=15,)
    response.raise_for_status()
    return response.content


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting image downloader service...")
    output_group = GroupSender(MOM_HOST, OUTPUT_GROUP, GATEWAY_AMMOUNT)
    input_group = GroupMember(MOM_HOST, INPUT_GROUP, ID)
    def consume_image(message, ack, _nack):
        logging.info("Received image download request")
        try:
            msg_type, client_id, fields = message_protocol.internal.deserialize(message)
            if msg_type != message_protocol.internal.MsgType.DATA:
                logging.error("Unexpected message type for image download request")
                raise ValueError("Unexpected message type for image download request")
            for name, url, image_url in fields:
                image_content = download_image(image_url)
                image_content_b64 = base64.b64encode(image_content).decode("ascii")
                output_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q2, [name, url, image_content_b64]))
                logging.info(f"Downloaded image size: {len(image_content)} bytes" + f" for article {name} ({url})")
        except requests.HTTPError as e:
            logging.warning("Downloading image failed for %s (%s): %s", url, image_url, e)
        except Exception:
            logging.exception("Error processing image download request")
        ack()
    input_group.start_consuming(consume_image)

if __name__ == "__main__":
    main()