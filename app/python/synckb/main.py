import json, logging, os, boto3, copy

levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
logger = logging.getLogger()
try:
    logger.setLevel(levels.get(os.getenv('LOG_LEVEL', 'info').lower()))
except KeyError:
    logger.setLevel(logging.INFO)


bedrock_agent_client = boto3.client('bedrock-agent')
sqs_client = boto3.client('sqs')

queue_url = os.getenv('QUEUE_URL')


def sqs_send_message(message_body, delay_seconds=0):
    if not queue_url:
        logger.error("No queue URL provided")
        return
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
        DelaySeconds=delay_seconds
    )


def sqs_delete_message(receipt_handle):
    if not queue_url:
        logger.error("No queue URL provided")
        return
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


def get_message_by_id(message_id, records):
    for record in records:
        if record["messageId"] == message_id:
            return record


def get_receipt_handle_by_message_id(message_id, records):
    return get_message_by_id(message_id, records)["receiptHandle"]


def get_message_body_by_message_id(message_id, records):
    return get_message_by_id(message_id, records)["body"]


def get_matching_kbs(resource_prefix=""):
    kbs = []
    response = bedrock_agent_client.list_knowledge_bases(
        maxResults=100,
    )
    for kb in response["knowledgeBaseSummaries"]:
        if kb["name"].startswith(resource_prefix):
            kbs.append(kb)

    while "nextToken" in response:
        response = bedrock_agent_client.list_knowledge_bases(
            maxResults=100,
            nextToken=response["nextToken"]
        )
        for kb in response["knowledgeBaseSummaries"]:
            if kb["name"].startswith(resource_prefix):
                kbs.append(kb)

    return kbs


def get_matching_data_sources(kb_id: str):
    data_sources = []
    response = bedrock_agent_client.list_data_sources(
        knowledgeBaseId=kb_id,
        maxResults=100,
    )
    for ds in response["dataSourceSummaries"]:
        data_sources.append(ds)

    while "nextToken" in response:
        response = bedrock_agent_client.list_data_sources(
            knowledgeBaseId=kb_id,
            maxResults=100,
            nextToken=response["nextToken"]
        )
        for ds in response["dataSourceSummaries"]:
            data_sources.append(ds)

    return data_sources


def get_data_source_details(ds_id: str, kb_id: str):
    response = bedrock_agent_client.get_data_source(
        dataSourceId=ds_id,
        knowledgeBaseId=kb_id
    )
    return response['dataSource']


def start_ingestion_job(ds_id: str, kb_id: str):
    bedrock_agent_client.start_ingestion_job(
        dataSourceId=ds_id,
        knowledgeBaseId=kb_id,
        description="Triggered by data source update"
    )


PREFIX_CONFIG = None


def add_prefix_config(prefix: str, kb_id: str, ds_id: str):
    if PREFIX_CONFIG is None:
        raise RuntimeError(
            "PREFIX_CONFIG not initialized. Make sure to invoke get_prefix_config() first."
        )
    if prefix in PREFIX_CONFIG:
        PREFIX_CONFIG[prefix].append({
            "kb_id": kb_id,
            "ds_id": ds_id
        })
    else:
        PREFIX_CONFIG[prefix] = [{
            "kb_id": kb_id,
            "ds_id": ds_id
        }]


def get_prefix_config():
    global PREFIX_CONFIG
    if PREFIX_CONFIG is None:
        PREFIX_CONFIG = {}
        for kb in get_matching_kbs(os.getenv('RESOURCE_PREFIX')):
            for ds in get_matching_data_sources(kb["knowledgeBaseId"]):
                ds_details = get_data_source_details(ds["dataSourceId"], kb["knowledgeBaseId"])
                if "dataSourceConfiguration" in ds_details:
                    if "s3Configuration" in ds_details["dataSourceConfiguration"]:
                        if "inclusionPrefixes" in ds_details["dataSourceConfiguration"]["s3Configuration"]:
                            if len(ds_details["dataSourceConfiguration"]["s3Configuration"]["inclusionPrefixes"]) == 0:
                                logger.debug("%s: Empty list of inclusing prefixes found", ds_details["dataSourceId"])
                                add_prefix_config("/", ds_details["knowledgeBaseId"], ds_details["dataSourceId"])
                            else:
                                logger.debug("%s: %s", ds_details["dataSourceId"], ds_details["dataSourceConfiguration"]["s3Configuration"]["inclusionPrefixes"])
                                for prefix in ds_details["dataSourceConfiguration"]["s3Configuration"]["inclusionPrefixes"]:
                                    add_prefix_config(prefix, ds_details["knowledgeBaseId"], ds_details["dataSourceId"])
                        else:
                            logger.debug("%s: No inclusion prefixes found", ds_details["dataSourceId"])
                            add_prefix_config("/", ds_details["knowledgeBaseId"], ds_details["dataSourceId"])
    return PREFIX_CONFIG


def get_changed_keys(records):
    changed_keys = {}
    messages = 0
    for record in records:
        records = 0
        message_body = record['body']
        messages += 1
        body = json.loads(message_body)
        for message in body["Records"]:
            records += 1
            bucket = message["s3"]["bucket"]["name"]
            changed_key = message["s3"]["object"]["key"]
            logger.info("Messages: %d, Records: %d, Bucket: %s, Key: %s", messages, records, bucket, changed_key)
            if changed_key.endswith("/"):
                logger.debug("Key '%s' is a folder, skipping", changed_key)
                continue
            changed_keys[changed_key] = record['messageId']
    return changed_keys


def get_ingestion_jobs_to_execute_for_non_root_keys(changed_keys, prefix_config):
    ij_to_execute = {}
    processed_changed_keys = {}
    for prefix in prefix_config:
        if prefix == "/":
            continue
        for changed_key, message_id in changed_keys.items():
            if changed_key.startswith(prefix):
                if changed_key not in processed_changed_keys:
                    processed_changed_keys[changed_key] = message_id
                for ij in prefix_config[prefix]:
                    ij_id = f'{ij["kb_id"]}_{ij["ds_id"]}'
                    if ij_id not in ij_to_execute:
                        ij_to_execute[ij_id] = {}
                    ij_to_execute[ij_id][changed_key] = message_id
    return ij_to_execute, processed_changed_keys


def get_todo_changed_keys(changed_keys, processed_changed_keys):
    todo_changed_keys = {}
    for changed_key, message_id in changed_keys.items():
        if changed_key not in processed_changed_keys:
            todo_changed_keys[changed_key] = message_id
    return todo_changed_keys


def get_ingestion_jobs_to_execute_for_root(prefix_config, ingestion_jobs_to_execute, todo_changed_keys):
    if len(todo_changed_keys) > 0:
        for ij in prefix_config["/"]:
            ij_id = f'{ij["kb_id"]}_{ij["ds_id"]}'
            if ij_id not in ingestion_jobs_to_execute:
                ingestion_jobs_to_execute[ij_id] = {}
            for todo_changed_key, message_id in todo_changed_keys.items():
                ingestion_jobs_to_execute[ij_id][todo_changed_key] = message_id


def get_valid_event_records(event):
    logger.info("Number of overall messages in this batch: %d", len(event['Records']))
    logger.debug(event)

    if "Records" not in event:
        logger.warning(event)
        logger.warning("No Records found in event. Skipping...")
        return None

    messages = 0
    event_records = copy.deepcopy(event["Records"])
    for record in event["Records"]:
        message_body = record['body']
        messages += 1
        body = json.loads(message_body)
        if "Records" not in body:
            if "Event" in body:
                logger.warning("Message %s is event %s. Removing this message...", messages, body["Event"])
            else:
                logger.warning("Message %s is an unexpected event. Removing this message...", messages)
            logger.info("Removing message %s from queue.", record["messageId"])
            sqs_delete_message(record["receiptHandle"])
            del event_records[messages - 1]
    logger.info("Number of valid messages in this batch: %d", len(event_records))
    return event_records


def lambda_handler(event, context):
    event_records = get_valid_event_records(event)

    if not event_records or len(event_records) == 0:
        logger.warning("No valid Records in event. Skipping...")
        return

    changed_keys = get_changed_keys(event_records)

    logger.debug("Changed keys: %s", changed_keys)

    if not changed_keys:
        logger.info("No relevant changed keys (files) found in event. Skipping...")
        return

    pc = get_prefix_config()
    logger.debug("Prefix config: %s", pc)

    ij_to_execute, processed_changed_keys = get_ingestion_jobs_to_execute_for_non_root_keys(changed_keys, pc)

    todo_changed_keys = get_todo_changed_keys(changed_keys, processed_changed_keys)
    logger.debug("Changed Keys processed (in subfolders): %s", processed_changed_keys)
    logger.debug("Changed Keys still to do (in root): %s", todo_changed_keys)

    get_ingestion_jobs_to_execute_for_root(pc, ij_to_execute, todo_changed_keys)

    logger.debug("Ingestion Jobs to execute: %s", ij_to_execute)

    for ij, keys in ij_to_execute.items():
        ij_data = ij.split("_")
        kb_id = ij_data[0]
        ds_id = ij_data[1]

        try:
            logger.info('Starting ingestion job for KB %s and DS %s triggered by changed objects: %s', kb_id, ds_id, list(keys.keys()))
            start_ingestion_job(ds_id, kb_id)
            for key, message_id in keys.items():
                logger.info("Removing message %s from queue.", message_id)
                sqs_delete_message(get_receipt_handle_by_message_id(message_id, event_records))
        except bedrock_agent_client.exceptions.ConflictException:
            logger.info("Ingestion job already running for KB %s and DS %s", kb_id, ds_id)
            # pushing messages back to SQS
            for key, message_id in keys.items():
                logger.info("Delay processing of message %s for 60 seconds in queue.", message_id)
                sqs_send_message(get_message_body_by_message_id(message_id, event_records), 60)
