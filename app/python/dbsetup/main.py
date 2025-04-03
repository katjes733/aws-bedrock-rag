import json, boto3, logging, os, time
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
    {"Template": Template("CREATE SCHEMA IF NOT EXISTS ${schema};"), "FailOnError": True},
    {"Template": Template("CREATE ROLE ${user} WITH PASSWORD '${password}' LOGIN;"), "FailOnError": False},
    {"Template": Template("GRANT ALL ON SCHEMA ${schema} TO ${user};"), "FailOnError": True},
]

table_sql_templates = [
    {"Template": Template("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${primary_key_field} uuid PRIMARY KEY, ${vector_field} vector(${embedding_size}), ${text_field} text, ${metadata_field} json);"), "FailOnError": True},
    {"Template": Template("CREATE INDEX IF NOT EXISTS idx_${table}_${vector_field} ON ${schema}.${table} USING hnsw (${vector_field} vector_cosine_ops) WITH (ef_construction=256);"), "FailOnError": True},
    {"Template": Template("CREATE INDEX IF NOT EXISTS idx_${table}_${text_field} ON ${schema}.${table} USING gin (to_tsvector('simple', ${text_field}));"), "FailOnError": True}
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


def get_kb_configs(rp):
    kb_configs = []
    if "KbConfigs" not in rp:
        raise KeyError("KbConfigs not found in ResourceProperties")
    if not isinstance(rp['KbConfigs'], list):
        raise TypeError(f"KbConfigs must be a list, but is {type(rp['KbConfigs'])}")
    for kb_config in rp['KbConfigs']:
        if not isinstance(kb_config, dict):
            raise TypeError(f"Each element in KbConfigs must be a dictionary, but is {type(kb_config)}")
        kb_config['name'] = kb_config['name'].replace('-', '_')
        kb_configs.append(kb_config)

    return kb_configs


def lambda_handler(event, context):
    logger.info("Event: %s", event)
    try:
        if ('RequestType' in event and event['RequestType'] == 'Create') or \
                ('tf' in event and 'action' in event['tf'] and event['tf']['action'] == 'create'):
            rp = event['ResourceProperties']
            db_cluster_arn = rp['DbClusterArn']
            master_secret_arn = rp['MasterSecretArn']
            bedrock_secret_arn = rp['BedrockSecretArn']
            database_name = rp['DatabaseName']
            kb_configs = None
            if 'KbConfigs' in rp:
                kb_configs = get_kb_configs(rp)
            else:
                kb_tables = rp['KnowledgeBaseTables']
                embedding_size = rp['EmbeddingSize']
            field_mapping = get_field_mapping(rp)

            response = secrets_client.get_secret_value(
                SecretId=bedrock_secret_arn)
            secret = json.loads(response['SecretString'])

            data = {
                "schema": secret.get('schema'),
                "user": secret.get('username'),
                "password": secret.get('password')
            }

            for key, value in field_mapping.items():
                data[key] = value

            for sql_template in master_sql_templates:
                execute_sql(sql_template, data, db_cluster_arn, master_secret_arn, database_name)

            if kb_configs:
                for kb_config in kb_configs:
                    data["table"] = kb_config['name']
                    data["embedding_size"] = kb_config['embedding_size']
                    for sql_template in table_sql_templates:
                        execute_sql(sql_template, data, db_cluster_arn, bedrock_secret_arn, database_name)
            else:
                for kb_table in kb_tables.split(','):
                    data["table"] = kb_table
                    data["embedding_size"] = embedding_size
                    for sql_template in table_sql_templates:
                        execute_sql(sql_template, data, db_cluster_arn, bedrock_secret_arn, database_name)

        send_response_cfn(event, context, cfnresponse.SUCCESS)
        return {
            "message": "Function executed successfully!"
        }
    except Exception as e:
        logger.error("Exception: %s", e)
        send_response_cfn(event, context, cfnresponse.FAILED)
        if 'ResponseURL' not in event:
            raise e
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
    while True:
        try:
            response = rds_client.execute_statement(
                resourceArn=db_cluster_arn,
                secretArn=db_secret_arn,
                database=database_name,
                sql=sql_template["Template"].substitute(data)
            )
            return response
        except rds_client.exceptions.DatabaseResumingException:
            logger.info("Database is resuming, waiting for 5 seconds...")
            time.sleep(5)
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
