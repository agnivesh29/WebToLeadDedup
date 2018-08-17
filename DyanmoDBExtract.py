import boto3

# from boto3.dynamodb.conditions import Key, Attr
# import json

client = boto3.client('dynamodb')
response = client.query(
    IndexName='createddate-createddatetime-index',
    # KeyConditionExpression=Key('createddate').eq(cutoffdate),
    ExpressionAttributeValues={
        ':v1': {
            'S': '2018-06-17',
        },
    },
    ProjectionExpression='hashkey',
    KeyConditionExpression='createddate = :v1',
    TableName='Web2LeadMessageDedup-WithTTL'
)

items = response['Items']

print(response)

last_eval_key = response['LastEvaluatedKey']

# resource = boto3.resource('dynamodb')
# table = resource.Table('Web2LeadMessageDedup-WithTTL')

# cutoffdate='2018-06-18'

# response = table.query(
# IndexName='createddate-createddatetime-index',
# KeyConditionExpression=Key('createddate').eq(cutoffdate),
# # ExpressionAttributeValues={
# # ':v1': {
# # 'S': '2018-06-18',
# # },
# # },
# ProjectionExpression='hashkey',
# # KeyConditionExpression='createddate = :v1',
# # TableName='Web2LeadMessageDedup-WithTTL'
# #ProjectionExpression='SongTitle'
# )

# items = response['Items']

# print(response)

with open('./dynamodb_extract_17JUN.csv', 'w') as f:
    for item in items:
        # print(item)
        # break
        # item_map = json.loads(item)
        f.write('{}\n'.format(item['hashkey']['S']))

    response = client.query(
        IndexName='createddate-createddatetime-index',
        # KeyConditionExpression=Key('createddate').eq(cutoffdate),
        ExpressionAttributeValues={
            ':v1': {
                'S': '2018-06-17',
            },
        },
        ProjectionExpression='hashkey',
        KeyConditionExpression='createddate = :v1',
        TableName='Web2LeadMessageDedup-WithTTL',
        ExclusiveStartKey=last_eval_key
    )

    items = response['Items']
    print('second call {}'.format(response))
    for item in items:
        # print(item)
        # break
        # item_map = json.loads(item)
        f.write('{}\n'.format(item['hashkey']['S']))
