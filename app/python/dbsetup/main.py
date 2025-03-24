import json, boto3, logging, os
from string import Template
import cfnresponse

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

rds_client = boto3.client('rds-data')
secrets_client = boto3.client('secretsmanager')

master_sql_templates = [
    {"Template": Template("CREATE EXTENSION IF NOT EXISTS vector;"), "FailOnError": True},
    {"Template": Template("CREATE SCHEMA IF NOT EXISTS $schema;"), "FailOnError": True},
    {"Template": Template("CREATE ROLE $user WITH PASSWORD '$password' LOGIN;"), "FailOnError": False},
    {"Template": Template("GRANT ALL ON SCHEMA $schema TO $user;"), "FailOnError": True},
]

table_sql_templates = [
    {"Template": Template("CREATE TABLE IF NOT EXISTS $schema.$table ($primary_key_field uuid PRIMARY KEY, $vector_field vector($embedding_size), $text_field text, $metadata_field json);"), "FailOnError": True},
    {"Template": Template("CREATE INDEX IF NOT EXISTS idx_$table ON $schema.$table USING hnsw (embedding vector_cosine_ops) WITH (ef_construction=256);"), "FailOnError": True}
]


def get_field_mapping(rp):
    if "FieldMapping" not in rp:
        raise KeyError("FieldMapping not found in ResourceProperties")
    if isinstance(rp['FieldMapping'], str):
        field_mapping = json.loads(rp['FieldMapping'])
    elif isinstance(rp['FieldMapping'], dict):
        field_mapping = rp['FieldMapping']
    else:
        raise TypeError(f"FieldMapping must be a string or a dictionary, but is {type(rp['FieldMapping'])}")

    if not field_mapping["primary_key_field"]:
        raise ValueError("primary_key_field cannot be empty")

    return field_mapping


def lambda_handler(event, context):
    try:
        if ('RequestType' in event and event['RequestType'] == 'Create') or \
                ('tf' in event and 'action' in event['tf'] and event['tf']['action'] == 'create'):
            rp = event['ResourceProperties']
            db_cluster_arn = rp['DbClusterArn']
            master_secret_arn = rp['MasterSecretArn']
            bedrock_secret_arn = rp['BedrockSecretArn']
            database_name = rp['DatabaseName']
            kb_tables = rp['KnowledgeBaseTables']
            field_mapping = get_field_mapping(rp)

            response = secrets_client.get_secret_value(
                SecretId=bedrock_secret_arn)
            secret = json.loads(response['SecretString'])

            data = {
                "schema": secret.get('schema'),
                "user": secret.get('username'),
                "password": secret.get('password'),
                "embedding_size": secret.get('embedding_size')
            }

            for key, value in field_mapping.items():
                data[key] = value

            for sql_template in master_sql_templates:
                execute_sql(sql_template, data, db_cluster_arn, master_secret_arn, database_name)

            for kb_table in kb_tables.split(','):
                data["table"] = kb_table
                for sql_template in table_sql_templates:
                    execute_sql(sql_template, data, db_cluster_arn, bedrock_secret_arn, database_name)

        send_response_cfn(event, context, cfnresponse.SUCCESS)
        return {
            "message": "Function executed successfully!"
        }
    except Exception as e:
        logger.error("Exception: %s", e)
        send_response_cfn(event, context, cfnresponse.FAILED)
        return {
            "message": "Error executing function",
            "error": str(e),
            "code": "INTERNAL_ERROR"
        }


def execute_sql(
        sql_template: dict,
        data: dict,
        db_cluster_arn: str,
        db_secret_arn: str,
        database_name: str):
    try:
        response = rds_client.execute_statement(
            resourceArn=db_cluster_arn,
            secretArn=db_secret_arn,
            database=database_name,
            sql=sql_template["Template"].substitute(data)
        )
        return response
    except Exception as e:
        if "FailOnError" not in sql_template or sql_template["FailOnError"]:
            logger.error("Error executing SQL: %s", e)
            raise e
        logger.info("Error executing SQL: %s. This error is ok, because 'FailOnError' was set to False for this statement.", e)
        return None


def send_response_cfn(event, context, response_status):
    if 'ResponseURL' not in event:
        return
    response_data = {}
    response_data['Data'] = {}
    cfnresponse.send(event, context, response_status, response_data, "CustomResourcePhysicalID")
