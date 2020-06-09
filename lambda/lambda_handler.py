import base64
import json
import logging
import os
import re
import uuid
import arrow
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
request_time_pattern = 'D/MMM/YYYY:HH:mm:ss Z'
nginx_log_pattern = '$remote_addr - $remote_user [$request_time] "$method $uri $http_version" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
regex = ''.join('(?P<' + g + '>.*?)' if g else re.escape(c)
    for g, c in re.findall(r'\$(\w+)|(.)', nginx_log_pattern))

s3_client = boto3.client("s3")
_BUCKET = os.environ.get("S3_BUCKET_NAME", "KAKAOBANK_LOG")
_PREFIX_PATH = os.environ.get("S3_SAVE_PREFIX", "nginx/v1-logs")


def _parse_nginx_log(log_str):
    """
        RAW Nginx Log를 파싱 하는 로직 수행
    """
    m = re.match(regex, log_str)
    return m.groupdict()


def _save_log(file_key, results):
    """
        S3에 데이터를 저장 하는 로직 수행. 
    """
    s3_upload_result = s3_client.put_object(Bucket=_BUCKET, Key=file_key, Body="\n".join(results))
    return s3_upload_result is not None and s3_upload_result.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) == 200


def _build_save_path(log_time):
    """
        로그가 발생된 시간을 기반으로 S3 Object Key를 생성한다.
    """
    return f"{_PREFIX_PATH}/year={log_time.format('YYYY')}" \
           f"/month={log_time.format('MM')}" \
           f"/day={log_time.format('DD')}" \
           f"/{log_time.format('YYYY-MM-DD-HH:mm')}.json"


def lambda_handler(event, context):
    """
        실제 로그가 수집되는 메인 진입부
    """
    result = {}
    for recored in event.get("Records"):
        try:
            payload = json.loads(base64.b64decode(recored.get("kinesis").get("data")).decode("UTF-8"))
            if payload is None or payload.get("message") is None:
                logger.error(f"Failed Parse Event LOG : {base64.b64decode(recored.get('kinesis').get('data')).decode('UTF-8')}" )
                continue
            
            item = _parse_nginx_log(payload.get("message"))
            log_time = arrow.get(item["request_time"], request_time_pattern)
            item["request_time"] = log_time.isoformat()
            collect_key = _build_save_path(log_time)
            if result.get(collect_key) is None:
                result[collect_key] = []
            result[collect_key].append(json.dumps(item))       
                
        except Exception as e:
            logger.exception(e)


    for file_key, values in result.items():
        if _save_log(file_key=f"{file_key}.{uuid.uuid4()}" ,results=values) is False:
            logger.error(f"Failed Save Log key={file_key}, values={values}")
        else:
            logger.debug(f"Success Save Log key={file_key}, values={values}")
    
    return event