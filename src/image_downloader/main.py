import requests
from common.middleware import GroupMember, GroupSender
from common import message_protocol
import logging
import os


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
    def consume_image(message, ack, nack):
        logging.info("Received image download request")
        try:
            client_id, name, url, image_url = message_protocol.internal.deserialize(message)
            image_content = download_image(image_url)
            output_group.send(message_protocol.internal.serialize([client_id, name, url, image_content]))
            logging.info(f"Downloaded image size: {len(image_content)} bytes" + f" for article {name} ({url})")
        except requests.HTTPError as e:
            logging.warning("Downloading image failed for %s (%s): %s", url, image_url, e)
            ack()
        except Exception:
            logging.exception("Error processing image download request")
            nack()
        else:
            ack()
    input_group.start_consuming(consume_image)

if __name__ == "__main__":
    main()