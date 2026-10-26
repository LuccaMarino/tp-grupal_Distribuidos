import json

from common import middleware, message_protocol
from common.middleware import GroupMember, GroupSender
from datetime import datetime, timezone
import logging
import os

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_GROUP = os.environ["INPUT_GROUP"]
OUTPUT_Q1 = os.environ["OUTPUT_Q1"]
OUTPUT_Q2 = os.environ["OUTPUT_Q2"]
OUTPUT_ABSTRACT = os.environ["OUTPUT_ABSTRACT"]
GATEWAY_AMMOUNT = int(os.environ["GATEWAY_AMMOUNT"])
SUM_AMMOUNT = int(os.environ["WORD_SUM_AMMOUNT"])
IMAGE_DOWNLOADER_AMMOUNT = int(os.environ["IMAGE_DOWNLOADER_AMMOUNT"])

def extract_sections(sections_raw):
    section_names = set()
    try:
        sections_raw = json.loads(sections_raw)
    except Exception as e:
        logging.error(f"Error parsing sections JSON: {e}")
        return section_names
    return section_names

def parse_date_modified(value):
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

def extract_article_fields(article: dict) -> dict:
    result = {}
    date_modified = parse_date_modified(article.get("date_modified"))
    if date_modified is not None:
        result["date_modified"] = date_modified.isoformat()
        result["year"] = date_modified.year
    result["language"] = article.get("lang")
    if "abstract" in article:
        result["abstract"] = article["abstract"]
    result["image"] = article.get("image")
    result["name"] = article.get("name")
    result["url"] = article.get("url")
    sections = extract_sections(article.get("sections", []))
    if "biography" in sections or "biographie" in sections:
        result["biography"] = True
    else:
        result["biography"] = False
    #print(result)
    return result

def consume_article(client_id, article, q1, q2, abstracts):
    try:
        article_fields = extract_article_fields(article)
        if article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("language") == "en" and article_fields.get("date_modified"):
            #q1.send(message_protocol.internal.serialize([client_id, article_fields["url"], article_fields["date_modified"]]))
            pass
        if article_fields.get("biography") and article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("image"):
            logging.info(f"SENDING biography article to q2: {article_fields['name']} ({article_fields['url']})")
            #q2.send(message_protocol.internal.serialize([client_id, article_fields["name"], article_fields["url"], article_fields["image_url"]]))
        if article_fields.get("language") and article_fields.get("year") is not None and  article_fields.get("url") and article_fields.get("abstract"):
            #abstracts.send(message_protocol.internal.serialize([client_id, article_fields]))
            pass
        return True
    except Exception as e:
        logging.exception("Error processing article message")
        return False

def consume_eof(client_id, abstracts):
    try:
        eof_message = message_protocol.internal.serialize([client_id, ID])
        #abstracts.send(eof_message)
        return True
    except Exception as e:
        logging.exception("Error processing EOF message")
        return False

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting parser service...")
    q1_group = GroupSender(MOM_HOST, OUTPUT_Q1, GATEWAY_AMMOUNT)
    q2_group = GroupSender(MOM_HOST, OUTPUT_Q2, IMAGE_DOWNLOADER_AMMOUNT)
    abstract_group = GroupSender(MOM_HOST, OUTPUT_ABSTRACT, SUM_AMMOUNT)
    input_group_member = GroupMember(MOM_HOST, INPUT_GROUP, int(ID))

    def consume_message(message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if not isinstance(fields, list):
                logging.error("Expected a list in the parser input message")
            if len(fields) == 1:
                succeeded = consume_eof(fields[0], abstract_group)
            elif len(fields) == 2:
                succeeded = consume_article(fields[0], fields[1], q1_group, q2_group, abstract_group)
            else:
                logging.error(f"Unexpected parser input message with {len(fields)} fields")
                raise ValueError("Unexpected parser input message")
            if succeeded:
                ack()
            else:
                nack()
        except Exception:
            logging.exception("Error decoding parser input message")
            nack()
    input_group_member.start_consuming(consume_message)

if __name__ == "__main__":
    main()