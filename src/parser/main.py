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

    def __init__(self):
        self.q1_group = GroupSender(MOM_HOST, OUTPUT_Q1, GATEWAY_AMOUNT)
        self.q2_group = GroupSender(MOM_HOST, OUTPUT_Q2, IMAGE_DOWNLOADER_AMOUNT)
        self.abstract_group = GroupSender(MOM_HOST, OUTPUT_ABSTRACT, SUM_AMOUNT)
        self.input_group_member = GroupMember(MOM_HOST, INPUT_GROUP, int(ID))
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, _signum, _frame):
        logging.info("Received SIGTERM signal")
        self.input_group_member.stop_consuming_threadsafe()

    def _close(self):
        for resource in (self.input_group_member, self.q1_group, self.q2_group, self.abstract_group):
            try:
                resource.close()
            except MessageMiddlewareCloseError as e:
                logging.error(e)

    def extract_sections(self, sections_raw):
        section_names = set()
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
        #print(result)
        return result

    def consume_article(self, client_id, article):
        try:
            article_fields = self.extract_article_fields(article)
            if article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("language") == "en" and article_fields.get("date_modified"):
                self.q1_group.send(message_protocol.internal.serialize_query_result(client_id, message_protocol.internal.Query.Q1, [article_fields["url"], article_fields["date_modified"]]))
            if article_fields["biography"] == True and article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("image_url"):
                logging.info(f"SENDING biography article to q2: {article_fields['name']} ({article_fields['url']})")
                self.q2_group.send(message_protocol.internal.serialize_data(client_id, [article_fields["name"], article_fields["url"], article_fields["image_url"]]))
            if article_fields.get("language") and article_fields.get("year") is not None and  article_fields.get("url") and article_fields.get("abstract"):
                self.abstract_group.send(message_protocol.internal.serialize_data(client_id, [[article_fields.get("language"), article_fields.get("year"), article_fields.get("url"), article_fields.get("abstract")]]))      
        except Exception:
            logging.exception("Error processing article message")

    def consume_eof(self, client_id):
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
        try:
            msg_type, client_id, args = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.EOF:
                self.consume_eof(client_id)
            elif msg_type == message_protocol.internal.MsgType.DATA:
                if len(args) != 1 or not isinstance(args[0], list):
                    raise ValueError("Expected a list of articles in the parser input message")
                for article in args[0]:
                    if not isinstance(article, dict):
                        raise ValueError("Expected each parser input article to be an object")
                    self.consume_article(client_id, article)
            else:
                raise ValueError(f"Unexpected parser input message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("Error decoding parser input message")
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