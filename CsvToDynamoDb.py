import numpy as np
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource("dynamodb");

table = dynamodb.Table('Result')

#code to upload csv file rows to dynamoDb
#loding csv file
file=np.loadtxt('/home/hardik/F0-v6/output.csv',delimiter=',',dtype=str)
headings=file[0]

#number of files
n=file.__len__()
#number of attributes
m=file[0].__len__()


for i in range(1, n - 1):
    list = {}
    for j in range(0, m):
        list[headings[j]] = file[i][j]
    print list
    response = table.put_item(
        Item=list)
    print("PutItem succeeded: "+list['File_Path'])
    print(json.dumps(response, indent=4, cls=DecimalEncoder))
