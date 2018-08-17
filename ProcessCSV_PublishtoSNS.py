import csv
import boto3
import time
import json

sns_client = boto3.client('sns')


def publish_to_sns(msg):
    response = sns_client.publish(
        TopicArn='arn:aws:sns:ap-southeast-2:354382532205:LeadsCreationRequested',  #'arn:aws:sns:ap-southeast-2:354382532205:test-topic-Staging',  #'',  #'', #'',  #'arn:aws:sns:ap-southeast-2:354382532205:test-topic-Staging',      #'arn:aws:sns:ap-southeast-2:354382532205:LeadsCreationRequested',
        Message=msg,
        Subject='LeadCreationRequested'
    )
    return response


with open('./missedrecords/missedrecords_06072018.csv', 'r') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    first_msg = 0
    for row in spamreader:
        # print('\noriginal row {}'.format(row))
        # print('first msg count ={}'.format(first_msg))
        if first_msg > 4000:
            break
        msg = ''

        if first_msg % 5 == 0:
            time.sleep(1)

        counter = 0
        for msg_body in row:
            if counter < 2:
                counter = counter + 1
                continue
            # print(msg_body)
            if msg == '':
                msg = msg + msg_body
            else:
                msg = msg + ',' + msg_body
            # print('###### {}'.format(msg))
        #print(msg.replace('\n', ''))
        # print(''.join(msg))
        print('Time={} counter={} msg={}\n'.format(time.ctime(), first_msg, msg))
        response = publish_to_sns(msg)
        print('publish response @@@@@@@@@@@@@ {}\n'.format(response))

        if first_msg < 10:
            pass
            #msg = ','.join(msg)
            # response = publish_to_sns(msg)
            # print('publish response @@@@@@@@@@@@@ {}'.format(response))
        first_msg = first_msg + 1
        # first_msg = first_msg + 1
        #p_response = publish_to_sns(json.dumps(row))
        #print(p_response)
        #counter = counter + 1
        #print(', '.join(row))
        #print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n')


#if __name__ == "__main__":
#    pass

