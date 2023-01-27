import json
import csv
import os

directory_in_str = '/Users/userName/downloads/logs' #the directory where the json files are.
directory = os.fsencode(directory_in_str)
data_file = open(directory_in_str + '/CSV/output.csv' , 'w') #the relative location of csv output. better to be a different folder than json files
csv_writer = csv.writer(data_file, delimiter = '😎')
csv_writer.writerow(['id', 'sent_at', 'message']) # a list of column names in string
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    pre, ext = os.path.splitext(filename)
    if ext != '.json':
        continue

    with open(directory_in_str + '/' + filename) as json_file:
        data = json.load(json_file)

    count = 0
    for datum in data:
        # need to implement below based on the snowflake table structure#########
        atrribute = datum['attributes']
        attributes = atrribute['attributes']
        # some field may not apply for all logs

        if 'subfieldB' in attributes['fieldA']:
            description = attributes['fieldA']['subfieldB']
        else:
            print(attributes['fieldA'])
            description = None

        # this needs to match the column/header name; however, it wont fail if it doesnt match
        csv_writer.writerow(
            [datum['id'], atrribute['timestamp'], description])
        #########################################################################
        count += 1
    print('converted ' + str(count) + ' for ' + filename)
data_file.close()