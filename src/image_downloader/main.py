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

# User Agent is used to identify the bot to the server. It is important to use a descriptive User Agent that includes contact information, so that the server administrators can reach out if there are any issues with the bot's behavior. Here is an example of a User Agent that includes the bot's name and contact email (one of the authors of this code was chosen).
USER_AGENT = "WikipediaAnalysisBot/1.0 (contact: pciliberto@fi.uba.ar)" 

STATUS_CODES_TO_RETRY = {429, 500, 502, 503, 504}
TOO_MANY_REQUESTS_STATUS_CODE = 429
RATE_LIMITING_DELAY = 0.125  # 8 requests per second
MAX_IMAGES = 4

class ImageDownloader:
    """
    The ImageDownloader class is responsible for consuming image download requests from the input group, downloading the requested images, and sending the downloaded images to the output group. It handles different message types, including EOF and DATA messages, and implements retry logic for failed downloads.
    """
    def __init__(self):
        """
        Initializes the ImageDownloader instance by setting up the necessary group sender and group member for image download requests.
        """
        self.output_group = GroupSender(MOM_HOST, OUTPUT_GROUP, GATEWAY_AMOUNT)
        self.input_group_member = GroupMember(MOM_HOST, INPUT_GROUP, int(ID))
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, _signum, _frame):
        """
        Handles the SIGTERM signal by stopping the input group member from consuming messages.
        """
        logging.info("Received SIGTERM signal")
        self.input_group_member.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the resources used by the ImageDownloader instance, including the input group member and the output group sender.
        """
        for resource in (self.input_group_member,self.output_group,):
            try:
                resource.close()
            except Exception as e:
                logging.error(e)

    def download_image(self, url)-> bytes:
        """
        Downloads an image from the given URL and returns the image data as bytes. It implements retry logic for failed downloads, with exponential backoff for certain HTTP status codes (429, 500, 502, 503, 504). If the download is successful, it returns a tuple (True, image_data). If the download fails after the maximum number of retries, it returns a tuple (False, error_message).
        """
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers={"User-Agent": USER_AGENT},timeout=15,)
                response.raise_for_status()
                return True, response.content
            except requests.HTTPError as e:
                status_code = e.response.status_code
                if status_code not in STATUS_CODES_TO_RETRY:
                    logging.error("ERROR for URL %s: HTTP status code %d", url, status_code)
                    return False, "ERROR: HTTP Error"
                if status_code == TOO_MANY_REQUESTS_STATUS_CODE:
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else max(3.0, RETRY_DELAY * (2 ** attempt))
                else:
                    delay = RETRY_DELAY * (2 ** attempt)
                if attempt == (MAX_RETRIES - 1):
                    logging.error("Max retries exceeded for URL %s", url)
                    return False, "ERROR: HTTP max retries exceeded"
            except requests.Timeout:
                if attempt == (MAX_RETRIES - 1):
                    logging.error("Max retries exceeded for URL %s", url)
                    return False, "ERROR: HTTP max retries exceeded"
                delay = RETRY_DELAY * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay) 
        return False, "ERROR: HTTP max retries exceeded"

    def consume_eof(self, client_id, sender):
        """
        Handles the EOF message by sending a query result with the EOF flag set.
        """
        logging.info("Received EOF message")
        sender_id = f"{ID},{sender}"
        eof_query_result = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q2, sender_id)
        self.output_group.broadcast(eof_query_result)
        logging.info("Sent EOF message to output group")

    def consume_image(self, client_id, fields):
        """
        Handles the image download request by downloading the requested images and sending the downloaded images to the output group. It implements rate limiting to avoid overwhelming the server with requests. If the download is successful, it sends a query result with the downloaded images. If the download fails, it logs a warning message.
        """
        try:
            images = []
            for [name, url, image_url] in fields:
                success, image_content = self.download_image(image_url)
                if success:
                    image_content_b64 = base64.b64encode(image_content).decode("ascii")
                else:
                    image_content_b64 = image_content
                images.append([name, url, image_content_b64])
                time.sleep(RATE_LIMITING_DELAY)
                if len(images) >= MAX_IMAGES:
                    self.output_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q2, images))
                    images = []
            self.output_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q2, images))
        except requests.HTTPError as e:
            logging.warning("Downloading image failed for %s (%s): %s", url, image_url, e)
        except Exception as e:
            logging.exception("Error processing image download request for client %s: %s", client_id, e)

    def consume_message(self, message, ack, _nack):
        """
        Consumes a message from the input group, deserializes it, and processes it based on its type (EOF or DATA).
        """
        try:
            msg_type, client_id, fields = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.EOF:
                sender = fields[0]
                self.consume_eof(client_id, sender)
            elif msg_type == message_protocol.internal.MsgType.DATA:
                if len(fields) != 1 or not isinstance(fields[0], list):
                    raise ValueError("Expected a list of image download records")
                if len(fields[0]) > 1:
                    raise ValueError("Expected at most one image per download message")
                self.consume_image(client_id, fields[0])
            else:
                raise ValueError(f"Unexpected message type for image download request: {msg_type}")
            ack()
        except Exception as e:
            logging.exception("Error processing image download request for client %s: %s", client_id, e)
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