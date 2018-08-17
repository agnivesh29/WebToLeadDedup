import boto3
# import csv

client = boto3.client('dynamodb')
day_to_extract = '2018-07-10'
table_name = 'Web2LeadMessage-Backup-WithTTL'  #'Web2LeadMessageDedup-WithTTL' #'Web2LeadMessage-Backup-WithTTL' #''
has_pagination = True

hash_key_set = set()
dedup_table_messages = list()


def get_query_args(table_name):
    table_detail_map = {
        'Web2LeadMessageDedup-WithTTL': {
            'IndexName': 'createddate-createddatetime-index',
            'ExpressionAttributeValues': {
                ':v1': {
                    'S': day_to_extract,
                }
            },
            'ProjectionExpression': 'hashkey, msgbody',
            'KeyConditionExpression': 'createddate = :v1',
            'TableName': table_name

        },
        'Web2LeadMessage-Backup-WithTTL': {
            'ExpressionAttributeValues': {
                ':v1': {
                    'S': day_to_extract,
                }
            },
            'ProjectionExpression': 'hashkey, msgbody',
            'KeyConditionExpression': 'createddate = :v1',
            'TableName': table_name
        }
    }
    return table_detail_map[table_name]


def get_pagination_condition(last_evaluated_key):
    pagination_condition_arg = {
        'ExclusiveStartKey': last_evaluated_key
    }
    return pagination_condition_arg


def query_dynamodb(**kwargs):
    response = client.query(**kwargs)
    return response


def main_handler():
    with open('DynamoDBExtract' + table_name + day_to_extract + '_hashkey_msgbody.csv', 'w') as file_pointer:
        query_args = get_query_args(table_name)
        query_result = query_dynamodb(**query_args)
        handle_response(query_result, file_pointer)
        while 'LastEvaluatedKey' in query_result:
            pagination_condition = get_pagination_condition(query_result['LastEvaluatedKey'])
            query_args_with_exclusive_start = {**query_args, **pagination_condition}
            print('Updated query condition {}'.format(query_args_with_exclusive_start))
            query_result = query_dynamodb(**query_args_with_exclusive_start)
            handle_response(query_result, file_pointer)
            print('########################################################################')
            # print(query_result)

        write_to_csv()


def handle_response(response, file_pointer):

    # if action == 'create':
    #     with open('DynamoDBExtract'+day_to_extract+'.csv', 'w') as file_pointer:
    #         write_to_csv(file_pointer, response)
    # elif action == 'append':
    #     print('action = {} query response {}'.format(action, response))
    #     with open('DynamoDBExtract'+day_to_extract+'.csv', 'a') as file_pointer:
    #         write_to_csv(file_pointer, response)
    counter = 0
    dynamodb_items = response['Items']
    for item in dynamodb_items:
        counter = counter + 1
        # print(item)
        hash_key_set.add(item['hashkey']['S'])
        dedup_table_messages.append(item['msgbody']['S'])
        create_csv_with_hashkey_msgbody(file_pointer, item)
    print('>>>>>>> Item Count {}'.format(counter))
        # file_pointer.write('{}\n'.format(item['hashkey']['S']))


def write_to_csv():
    # dynamodb_items = response['Items']
    # print(dynamodb_items)
    # for item in dynamodb_items:
    #     file_pointer.write('{}\n'.format(item['hashkey']['S']))
    print('************ Unique hashkeys {}'.format(len(hash_key_set)))
    with open('DynamoDBExtract' + table_name + day_to_extract + '_hashkey.csv', 'w') as file_pointer:
        for hash_key in hash_key_set:
            file_pointer.write('{}\n'.format(hash_key))
    with open('DynamoDBExtract' + table_name + day_to_extract + '_msgbody.csv', 'w') as file_pointer:
        for msg in dedup_table_messages:
            file_pointer.write('{}\n'.format(msg))


def create_csv_with_hashkey_msgbody(file_pointer, item):
    hash_key_str = item['hashkey']['S']
    msg_body = item['msgbody']['S']
    file_pointer.write('{},{}\n'.format(hash_key_str,msg_body))


if __name__ == "__main__":
    main_handler()


# print(query_args)
# query_result = query_dynamodb(**query_args)
# print(query_result)
# print('##############################')
# print(query_result['LastEvaluatedKey'])

# pagination_condition = get_pagination_condition('test-condition')

# while has_pagination:
#     if 'LastEvaluatedKey' in query_result:
#         has_pagination = True
#         last_item = query_result['LastEvaluatedKey']
#     else:
#         pass

