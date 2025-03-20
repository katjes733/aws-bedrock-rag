import logging, os, boto3, time
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


def delete_all_objects(event):
    bucket_name = event['ResourceProperties']['BucketName']
    if event['RequestType'] == 'Delete':
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter():
            logger.info("Deleting object with key: %s ...", obj.key)
            s3.Object(bucket.name, obj.key).delete()
            logger.debug("Deleted object with key: %s", obj.key)


def sleep(event):
    """
    Sleep function. Sleeps no more than 60 seconds.

    Args:
        event: The CFN event.
    """
    sleep_time = min(int(event['ResourceProperties']['SleepTime']), 60)
    if event['RequestType'] == 'Create':
        logger.info("Sleeping for %s seconds", sleep_time)
        time.sleep(sleep_time)
        logger.info("Waking after %s seconds", sleep_time)


def create_folders(event):
    bucket_name = event['ResourceProperties']['BucketName']
    folder_names = event['ResourceProperties']['FolderNames']
    if event['RequestType'] == 'Create':
        s3 = boto3.resource('s3')
        for folder_name in folder_names.split(','):
            fixed_folder_name = folder_name if folder_name.endswith('/') else f'{folder_name}/'
            logger.info("Creating folder with key: %s ...", fixed_folder_name)
            s3.Object(bucket_name, fixed_folder_name).put()
            logger.debug("Created folder with key: %s", fixed_folder_name)


def lambda_handler(event, context):
    try:
        if event['ResourceProperties']['Function'] == 'DeleteAllObjects':
            delete_all_objects(event)
        elif event['ResourceProperties']['Function'] == 'CreateFolders':
            create_folders(event)
        elif event['ResourceProperties']['Function'] == 'Sleep':
            sleep(event)
        else:
            logger.warning("No implementation for function [%s]. Skipping...", event['ResourceProperties']['Function'])

        send_response_cfn(event, context, cfnresponse.SUCCESS)
    except Exception as e:
        logger.error("Exception: %s", e)
        send_response_cfn(event, context, cfnresponse.FAILED)


def send_response_cfn(event, context, response_status):
    response_data = {}
    response_data['Data'] = {}
    cfnresponse.send(event, context, response_status, response_data, "CustomResourcePhysicalID")