
import json
import os
import re
import hashlib
import logging
import urllib.request
import urllib.parse
import mimetypes
import base64
from datetime import datetime, timezone
from pathlib import PurePath

import boto3

REGION = "eu-west-1"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
secrets = boto3.client("secretsmanager", region_name=REGION)

QUEUE_ENV = {
    "statement": "STATEMENT_QUEUE_URL",
    "po_invoice": "POINVOICE_QUEUE_URL",
    "non_po_invoice": "NONPOINVOICE_QUEUE_URL",
}

S3_CLASSIFICATION = {
    "statement": "soainvoices",
    "po_invoice": "poinvoices",
    "non_po_invoice": "nonpoinvoices",
}


def graph_request(url, token):
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        return json.loads(response.read().decode("utf-8"))


def get_graph_access_token():
    secret_arn = os.environ["GRAPH_SECRET_ARN"]
    secret_value = secrets.get_secret_value(
        SecretId=secret_arn
    )["SecretString"]

    try:
        parsed = json.loads(secret_value)
        if isinstance(parsed, dict):
            client_secret = (
                parsed.get("client_secret")
                or parsed.get("GRAPH_CLIENT_SECRET")
                or parsed.get("value")
            )
        else:
            client_secret = secret_value
    except json.JSONDecodeError:
        client_secret = secret_value

    if not client_secret:
        raise ValueError("Graph client secret is empty")

    tenant = os.environ["GRAPH_TENANT_ID"]
    client_id = os.environ["GRAPH_CLIENT_ID"]

    token_url = (
        f"https://login.microsoftonline.com/{tenant}"
        "/oauth2/v2.0/token"
    )
    form = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }).encode()

    request = urllib.request.Request(
        token_url,
        data=form,
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode())["access_token"]


def read_mailbox(token):
    mailbox = urllib.parse.quote(
        os.environ["GRAPH_MAILBOX"], safe=""
    )
    query = urllib.parse.urlencode({
        "$top": "10",
        "$select": "id,subject,receivedDateTime,hasAttachments",
        "$orderby": "receivedDateTime desc",
    })

    data = graph_request(
        f"{GRAPH_BASE}/users/{mailbox}/messages?{query}", token
    )

    return {
        "status": "mailbox-read-success",
        "messages_found": len(data.get("value", [])),
        "messages": [
            {
                "id": m.get("id"),
                "subject": m.get("subject"),
                "receivedDateTime": m.get("receivedDateTime"),
                "hasAttachments": m.get("hasAttachments"),
            }
            for m in data.get("value", [])
        ],
    }


def get_message_base(message_id):
    mailbox = urllib.parse.quote(
        os.environ["GRAPH_MAILBOX"], safe=""
    )
    encoded_id = urllib.parse.quote(message_id, safe="")
    return f"{GRAPH_BASE}/users/{mailbox}/messages/{encoded_id}"


def get_attachments(message_id, token):
    """Fetch attachment metadata across all Graph result pages."""
    url = f"{get_message_base(message_id)}/attachments?$top=100"
    attachments = []

    while url:
        page = graph_request(url, token)
        attachments.extend(page.get("value", []))
        url = page.get("@odata.nextLink")

    return attachments


def safe_filename(name):
    name = PurePath(name or "attachment.bin").name
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] or "attachment.bin"


def inspect_attachments(event, token):
    message_id = event.get("message_id")
    if not message_id:
        raise ValueError("message_id is required")

    attachments = get_attachments(message_id, token)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message_id": message_id,
            "attachment_count": len(attachments),
            "attachments": [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "contentType": item.get("contentType"),
                    "size": item.get("size"),
                    "isInline": item.get("isInline"),
                    "type": item.get("@odata.type"),
                    "has_content_bytes": bool(
                        item.get("contentBytes")
                    ),
                }
                for item in attachments
            ],
        }),
    }


def process_message(event, token):
    """
    Process one selected email with explicit classification per attachment.

    Example:
    {
      "action": "process_message",
      "message_id": "GRAPH_MESSAGE_ID",
      "attachment_types": {
        "SOA Riyadh Cables.pdf": "statement",
        "INV26-0175.pdf": "po_invoice"
      }
    }

    Unmapped attachments are skipped. The function does not guess
    a document's type from its filename.
    """
    message_id = event.get("message_id")
    attachment_types = event.get("attachment_types")

    if not message_id:
        raise ValueError("message_id is required")

    if not isinstance(attachment_types, dict) or not attachment_types:
        raise ValueError(
            "attachment_types must map filenames to statement, "
            "po_invoice, or non_po_invoice"
        )

    normalized_types = {}
    for supplied_name, document_type in attachment_types.items():
        filename = safe_filename(supplied_name)
        if document_type not in QUEUE_ENV:
            raise ValueError(
                f"Invalid document_type for {filename}: {document_type}"
            )
        normalized_types[filename] = document_type

    message = graph_request(get_message_base(message_id), token)

    if not message.get("hasAttachments"):
        return {
            "status": "no_attachments",
            "message_id": message_id,
        }

    attachments = get_attachments(message_id, token)
    bucket = os.environ["S3_BUCKET_NAME"]

    received = message.get("receivedDateTime")
    try:
        date_folder = datetime.fromisoformat(
            received.replace("Z", "+00:00")
        ).date().isoformat()
    except (ValueError, AttributeError):
        date_folder = datetime.now(timezone.utc).date().isoformat()

    results = []
    skipped = []

    for attachment in attachments:
        if attachment.get("@odata.type") != (
            "#microsoft.graph.fileAttachment"
        ):
            logger.info(
                "Skipping non-file attachment type: %s",
                attachment.get("@odata.type"),
            )
            continue

        if attachment.get("isInline"):
            continue

        filename = safe_filename(attachment.get("name"))
        document_type = normalized_types.get(filename)

        if not document_type:
            skipped.append({
                "filename": filename,
                "reason": "No explicit classification supplied",
            })
            continue

        content = attachment.get("contentBytes")
        if not content:
            skipped.append({
                "filename": filename,
                "reason": "Missing contentBytes",
            })
            continue

        file_bytes = base64.b64decode(content, validate=True)
        attachment_id = attachment.get("id", filename)

        transaction_id = hashlib.sha256(
            f"{message_id}:{attachment_id}".encode()
        ).hexdigest()[:24]

        classification = S3_CLASSIFICATION[document_type]
        key = (
            f"Raw-Invoice-Uploads/{classification}/"
            f"{date_folder}/{transaction_id}/{filename}"
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_bytes,
            ContentType=(
                attachment.get("contentType")
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            ),
            Metadata={
                "source-message-id-hash": hashlib.sha256(
                    message_id.encode()
                ).hexdigest(),
                "attachment-id-hash": hashlib.sha256(
                    attachment_id.encode()
                ).hexdigest(),
                "document-type": document_type,
            },
        )

        payload = {
            "transaction_id": transaction_id,
            "document_type": document_type,
            "s3_bucket": bucket,
            "s3_key": key,
            "filename": filename,
        }

        sent = sqs.send_message(
            QueueUrl=os.environ[QUEUE_ENV[document_type]],
            MessageBody=json.dumps(payload),
        )

        results.append({
            "filename": filename,
            "document_type": document_type,
            "s3_key": key,
            "sqs_message_id": sent["MessageId"],
        })

    return {
        "status": "completed",
        "message_id": message_id,
        "attachments_processed": len(results),
        "attachments": results,
        "skipped": skipped,
    }


def lambda_handler(event, context):
    event = event or {}
    action = event.get("action", "health")

    if action == "health":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "mailbox-reader-lambda-running"
            }),
        }

    if action == "read_mailbox":
        token = get_graph_access_token()
        result = read_mailbox(token)
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }

    if action == "inspect_attachments":
        token = get_graph_access_token()
        return inspect_attachments(event, token)

    if action == "process_message":
        token = get_graph_access_token()
        result = process_message(event, token)
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }

    if action == "publish":
        document_type = event.get("document_type")
        if document_type not in QUEUE_ENV:
            raise ValueError("Unsupported document_type")

        required = ["transaction_id", "s3_key", "filename"]
        missing = [
            key for key in required if not event.get(key)
        ]
        if missing:
            raise ValueError(
                f"Missing required fields: {missing}"
            )

        sent = sqs.send_message(
            QueueUrl=os.environ[QUEUE_ENV[document_type]],
            MessageBody=json.dumps({
                "transaction_id": event["transaction_id"],
                "document_type": document_type,
                "s3_bucket": os.environ["S3_BUCKET_NAME"],
                "s3_key": event["s3_key"],
                "filename": event["filename"],
            }),
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Message sent to SQS",
                "message_id": sent["MessageId"],
            }),
        }

    raise ValueError(f"Unsupported action: {action}")
