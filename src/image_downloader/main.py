import requests
from common import middleware, message_protocol
from datetime import datetime
import logging
import os


MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


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
    #output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
    def consume_image(message, ack, nack):
        logging.info("Received image download request")
        try:
            client_id, name, url, image_url = message_protocol.internal.deserialize(message)
            image_content = download_image(image_url)
            state = {"status": "ok", "image_bytes": image_content, "error": None}
            logging.info(f"Downloaded image size: {len(image_content)} bytes" + f" for article {name} ({url})")
            ack()
        except requests.HTTPError as e:
            status_code = e.response.status_code
            logging.warning("Downloading Image Error %s: %s", url,image_url)
            state = {"status": "error","error": "HTTP 404 Not Found","image_bytes": None}
        finally:
            #podria no enviarse nada si hay error
            # output_queue.send(message_protocol.internal.serialize(client_id, state))
            ack()
    input_queue.start_consuming(consume_image)

if __name__ == "__main__":
    main()