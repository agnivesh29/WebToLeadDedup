import boto3
# @TO_DO - change table name to dedup table

has_pagination = True

records = list()


def process_records(f, items):
    # for items in records:
    for item in items:
        hashkey = item['Hashkey']['S']
        msg_body = item['MessageBody']['S']
        timestamp = item['Timestamp']['S']
        f.write('{},{},{}\n'.format(timestamp, hashkey, msg_body))


with open('./missedrecords/missedrecords_06072018.csv', 'w') as f:
    table_name = 'Web2LeadMessages-backup'  # 'Web2LeadMessageStaging' #'Web2LeadMessages-backup'#'Web2LeadMessageStaging'

    client = boto3.client('dynamodb')
    response = client.scan(
        ExpressionAttributeNames={
             '#TS': 'Timestamp',
             # 'HKEY': 'Hashkey',
        },
        ExpressionAttributeValues={
                    ':start': {
                        'S': '2018-07-06 16:18:00.000000',
                    },
                    ':end': {
                        'S': '2018-07-07 00:00:00.000000',
                    }
                },
        #ProjectionExpression='Hashkey,MessageBody,#TS',
        FilterExpression='#TS >= :start AND #TS <= :end',
        TableName=table_name
    )

    print(response)

    records.append(response['Items'])

    #process_records(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = client.scan(
            ExpressionAttributeNames={
                '#TS': 'Timestamp',
                # 'HKEY': 'Hashkey',
            },
            ExpressionAttributeValues={
                ':start': {
                        'S': '2018-07-06 16:18:00.000000',
                    },
                ':end': {
                    'S': '2018-07-07 00:00:00.000000',
                }
            },
            # ProjectionExpression='Hashkey,MessageBody,#TS',
            FilterExpression='#TS >= :start AND #TS <= :end',
            TableName=table_name,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        print(response)
        records.extend(response['Items'])
        process_records(f, response['Items'])

    print('>>>>>>>>>>{}\n<<<<<<<<<'.format(records))



