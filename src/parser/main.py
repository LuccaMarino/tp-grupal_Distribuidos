from common import middleware, message_protocol
from datetime import datetime, timezone
import logging
import os

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_Q1 = os.environ["OUTPUT_Q1"]
OUTPUT_Q2 = os.environ["OUTPUT_Q2"]
OUTPUT_ABSTRACT = os.environ["OUTPUT_ABSTRACT"]

def extract_sections(sections):
    section_names = set()
    for section in sections:
        if isinstance(section, dict) and "name" in section:
            section_names.add(section["name"].lower())
        elif isinstance(section, str):
            section_names.add(section.lower())
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
    if "in_language" in article:
        result["language"] = article["in_language"].get("identifier")
    if "abstract" in article:
        result["abstract"] = article["abstract"]
    image = article.get("image")
    if isinstance(image, dict):
        result["image_url"] = image.get("content_url")
    elif isinstance(image, str):
        result["image_url"] = image
    result["name"] = article.get("name")
    result["url"] = article.get("url")
    sections = extract_sections(article.get("sections", []))
    if "biography" in sections or "biographie" in sections:
        result["biography"] = True
    return result

def consume_article(client_id, article, q1, q2, abstracts):
    try:
        article_fields = extract_article_fields(article)
        if article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("language") == "en" and article_fields.get("date_modified"):
            #q1.send(message_protocol.internal.serialize([client_id, article_fields["url"], article_fields["date_modified"]]))
            pass
        if article_fields.get("biography") and article_fields.get("year") is not None and article_fields["year"] <= 2020 and article_fields.get("image_url"):
            logging.info(f"Sending biography article to q2: {article_fields['name']} ({article_fields['url']})")
            q2.send(message_protocol.internal.serialize([client_id, article_fields["name"], article_fields["url"], article_fields["image_url"]]))
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
    q1_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_Q1)
    q2_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_Q2)
    abstract_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_ABSTRACT)
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def consume_message(message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if not isinstance(fields, list):
                logging.error("Expected a list in the parser input message")
            if len(fields) == 1:
                succeeded = consume_eof(fields[0], abstract_queue)
            elif len(fields) == 2:
                succeeded = consume_article(fields[0], fields[1], q1_queue, q2_queue, abstract_queue)
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
    input_queue.start_consuming(consume_message)

if __name__ == "__main__":
    main()