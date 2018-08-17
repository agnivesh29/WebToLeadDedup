import hashlib
import csv


def gen_hash_key(message):
    hash_key = hashlib.md5(message.encode()).hexdigest()
    return hash_key


counter = 0
hash_key_set = set()
file_name = 'prod_message_records_10jul'
# with open('./prod_message_records_02jul.csv', 'w') as f1:
# with open('./message_records_18June.csv', 'r') as f:
# message=f.readline()
# while(message!=''):
# counter = counter+1
# print('counter={} reading from file {}'.format(counter,message))
# f1.write(gen_hash_key(message)+'\n')
with open(file_name + '_hashkey.csv', 'w') as file_write:
    with open('./'+file_name+'.csv', 'r') as csvfile:
        spamreader = csv.reader(csvfile)  # Dialect.doublequote , quotechar='|' delimiter=' '
        for row in spamreader:
            counter = counter + 1
            msg_body = ''.join(row)
            # print(msg_body)
            hash_key = gen_hash_key(msg_body)
            # check for duplicate value.
            counter = 0
            if hash_key in hash_key_set:
                print('{} Duplicate found hash key {}'.format(counter,hash_key))
                counter = counter + 1
                # break

            hash_key_set.add(hash_key)
            # f1.write(hash_key + '\n')

    print('Total messages={} Unique messages={}'.format(counter, len(hash_key_set)))

    for msg_hash_key in hash_key_set:
        file_write.write('{}\n'.format(msg_hash_key)) #'{}\n'.format(hash_key)
