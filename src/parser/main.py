import json

from common import message_protocol
from common.middleware import GroupMember, GroupSender
from common.middleware.middleware import MessageMiddlewareCloseError
from datetime import datetime, timezone
import logging
import os
import signal

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_Q1 = os.environ["OUTPUT_Q1"]
OUTPUT_Q2 = os.environ["OUTPUT_Q2"]
OUTPUT_ABSTRACT = os.environ["OUTPUT_ABSTRACT"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
SUM_AMOUNT = int(os.environ["WORD_SUM_AMOUNT"])
IMAGE_DOWNLOADER_AMOUNT = int(os.environ["IMAGE_DOWNLOADER_AMOUNT"])


class Parser:
    """
    The Parser class is responsible for consuming messages from the input group, processing the articles, and sending the parsed articles to the appropriate output groups. It handles different message types, including EOF and DATA messages, and extracts relevant fields from the articles for further processing.
    """
    def __init__(self):
        """
        Initializes the Parser instance by setting up the necessary group senders and group member for different queries.
        """
        self.q1_group = GroupSender(MOM_HOST, OUTPUT_Q1, GATEWAY_AMOUNT)
        self.q2_group = GroupSender(MOM_HOST, OUTPUT_Q2, IMAGE_DOWNLOADER_AMOUNT)
        self.abstract_group = GroupSender(MOM_HOST, OUTPUT_ABSTRACT, SUM_AMOUNT)
        self.input_group_member = GroupMember(MOM_HOST, INPUT_GROUP, int(ID))
        self.q2_sent = 0
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, _signum, _frame):
        """
        Handles the SIGTERM signal by stopping the input group member from consuming messages.
        """
        logging.info("Received SIGTERM signal")
        self.input_group_member.stop_consuming_threadsafe()

    def _close(self):
        """
        Closes the resources used by the Parser instance, including the input group member and the output group senders.
        """
        for resource in (self.input_group_member, self.q1_group, self.q2_group, self.abstract_group):
            try:
                resource.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def extract_sections(self, sections_raw):
        """
        Extracts section names from the raw sections data in an article. It returns a set of section names in lowercase, ignoring any sections without a name or with invalid JSON format.
        """
        section_names = set()
        if sections_raw is None or (isinstance(sections_raw, str) and not sections_raw.strip()):
            return section_names
        try:
            sections_raw = json.loads(sections_raw)
        except Exception as e:
            logging.error(f"Error parsing sections JSON: {e}")
            return section_names
        for section in sections_raw:
            if section.get("name") is not None:
                section_names.add(section["name"].strip().lower())
        return section_names

    def parse_date_modified(self, value):
        """
        Parses the date_modified value from an article and returns a datetime object in UTC timezone.
        """
        if value is None or value == 0 or value == "":
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                pass
        elif isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        logging.warning("Ignoring invalid date_modified value of type %s", type(value).__name__)
        return None

    def extract_article_fields(self, article: dict) -> dict:
        """
        Extracts relevant fields from an article dictionary and returns a new dictionary containing the extracted fields. The extracted fields include `date_modified`, `year`, `language`, `abstract`, `image_url`, `name`, `url`, and `biography` status.
        """
        result = {}
        date_modified = self.parse_date_modified(article.get("date_modified"))
        if date_modified is not None:
            result["date_modified"] = date_modified.isoformat()
            result["year"] = date_modified.year
        result["language"] = article.get("lang")
        if "abstract" in article:
            result["abstract"] = article["abstract"]
        
        raw_image = article.get("image")
        if raw_image is not None:
            result["image_url"] = raw_image
        else:
            result["image_url"] = None
        result["name"] = article.get("name")
        result["url"] = article.get("url")
        sections = self.extract_sections(article.get("sections"))
        if "biography" in sections or "biographie" in sections:
            result["biography"] = True
        else:
            result["biography"] = False
        return result

    def consume_article(self,article, q1, q2, q3):
        """
        Consumes an article and extracts relevant fields for different queries. It appends the extracted data to the corresponding query lists (q1, q2, q3) based on specific conditions.
        """
        try:
            article_fields = self.extract_article_fields(article)
            if article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("language") == "en" and article_fields.get("date_modified"):
                q1.append([article_fields["url"], article_fields["date_modified"]])
            if article_fields["biography"] == True and article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("image_url"):
                q2.append([article_fields["name"], article_fields["url"], article_fields["image_url"]])
            if article_fields.get("language") and article_fields.get("year") is not None and article_fields.get("url") and article_fields.get("abstract"):
                q3.append([article_fields.get("language"), article_fields.get("year"), article_fields.get("url"), article_fields.get("abstract")])
        except Exception:
            logging.exception("Error processing article message")

    def consume_eof(self, client_id):
        """
        Consumes an EOF message from the input group and broadcasts the EOF message to the output groups (q1, q2, abstract).
        """
        try:
            eof_query_result = message_protocol.internal.serialize_query_eof(client_id, message_protocol.internal.Query.Q1, ID)
            self.q1_group.broadcast(eof_query_result)
            eof_message = message_protocol.internal.serialize_eof(client_id, ID)
            self.q2_group.broadcast(eof_message)
            self.abstract_group.broadcast(eof_message)
            logging.info("Sent EOF message to abstract group")
        except Exception:
            logging.exception("Error processing EOF message")
    
    def consume_message(self, message, ack, nack):
        """
        Consumes a message from the input group, deserializes it, and processes it based on its type (EOF or DATA). It extracts relevant fields from articles and sends the parsed data to the appropriate output groups.
        """
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.EOF:
                self.consume_eof(client_id)
            elif msg_type == message_protocol.internal.MsgType.DATA:
                if len(args) != 1 or not isinstance(args[0], list):
                    raise ValueError("Expected a list of articles in the parser input message")
                q1_results = []
                q2_results = []
                abstract_results = []
                for article in args[0]:
                    if not isinstance(article, dict):
                        raise ValueError("Expected each parser input article to be an object")
                    self.consume_article(article, q1_results, q2_results, abstract_results)
                if q1_results:
                    self.q1_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q1, q1_results))
                if q2_results:
                    for image_record in q2_results:
                        self.q2_group.send(message_protocol.internal.serialize_data(client_id, [image_record]))
                if abstract_results:
                    self.abstract_group.send(message_protocol.internal.serialize_data(client_id, abstract_results))
            else:
                raise ValueError(f"Unexpected parser input message type: {msg_type}")
            ack()
        except Exception as e:
            logging.exception("Error decoding parser input message for client %s: %s", client_id, e)
            nack()

    def start(self):
        try:
            self.input_group_member.start_consuming(self.consume_message)
        finally:
            self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting parser service...")
    parser = Parser()
    parser.start()
    return 0

if __name__ == "__main__":
    main()