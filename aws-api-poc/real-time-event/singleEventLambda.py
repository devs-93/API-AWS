import json
import boto3


def putDataToKinesis(payloadData):
    kinesis = boto3.client("kinesis")
    response = kinesis.put_record(
        StreamName="firstStream",
        Data=json.dumps(payloadData),
        PartitionKey="AdjustAsNeeded"
    )
    return response


def paramFlattenFunction(paramArray, resObj):
    count = 0
    for rows in paramArray:
        resObj['key_' + str(count)] = rows['key']
        resObj['string_value_' + str(count)] = rows['value']['string_value']
        resObj['int_value_' + str(count)] = rows['value']['int_value']
        resObj['float_value_' + str(count)] = rows['value']['float_value']
        resObj['double_value_' + str(count)] = rows['value']['double_value']
        count = count + 1
    return resObj


def lambda_handler(event, context):
    resObj = {}
    # parse query string param
    if event['httpMethod'] == "POST":
        resObj['PartitionKey'] = event['body']['PartitionKey']
        resObj['device'] = event['body']['device']
        resObj['v'] = event['body']['v']
        resObj['user_id'] = event['body']['user_id']
        resObj['client_ts'] = event['body']['client_ts']
        resObj['sdk_version'] = event['body']['sdk_version']
        resObj['os_version'] = event['body']['os_version']
        resObj['manufacturer'] = event['body']['manufacturer']
        resObj['platform'] = event['body']['platform']
        resObj['session_id'] = event['body']['session_id']
        resObj['session_num'] = event['body']['session_num']
        resObj['limit_ad_tracking'] = event['body']['limit_ad_tracking']
        resObj['logon_gamecenter'] = event['body']['logon_gamecenter']
        resObj['logon_gameplay'] = event['body']['logon_gameplay']
        resObj['jailbroken'] = event['body']['jailbroken']
        resObj['android_id'] = event['body']['android_id']
        resObj['googleplus_i'] = event['body']['googleplus_i']
        resObj['facebook_id'] = event['body']['facebook_id']
        resObj['gender'] = event['body']['gender']
        resObj['birth_year'] = event['body']['birth_year']
        resObj['build'] = event['body']['build']
        resObj['engine_version'] = event['body']['engine_version']
        resObj['ios_idfv'] = event['body']['ios_idfv']
        resObj['connection_type'] = event['body']['connection_type']
        resObj['ios_idfa'] = event['body']['ios_idfa']
        resObj['google_aaid'] = event['body']['google_aaid']
        resObj['eventName'] = event['body']['eventName']
        resObj['metric'] = event['body']['metric']
        resObj['date'] = event['body']['date']
        try:
            payloadData = paramFlattenFunction(event['body']['params'], resObj)
        except Exception as e:
            return {'statusCode': 200, 'body': json.dumps(str(e))}
        res = putDataToKinesis(payloadData)
        return {'statusCode': 200, 'body': res}
    elif event['httpMethod'] == "GET":
        return {'statusCode': 200, 'body': json.dumps(str("GET method found , POST required"))}
    else:
        return {'statusCode': 200, 'body': json.dumps(str("no appropriate method found"))}
