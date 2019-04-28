from __future__ import print_function # Python 2/3 compatibility
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from datetime import timedelta, date
import time
import json
import decimal

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days + 1)):
        yield start_date + timedelta(n)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Pipeline')

then = time.time()

try:
    response = table.get_item(
        Key={
            'plant_id': 1,
            'date': '2018-04-23'
        }
    )
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    item = response['Item']
    print("GetItem succeeded:")
    print(json.dumps(item, indent=4, cls=DecimalEncoder))

now = time.time()

print("It took: ", now-then, " seconds to get data for one day at plant 1")

###########################

then = time.time()

start_date = date(2018, 1, 1)
end_date = date(2018, 2, 1)
count = 0

try:
    for single_date in daterange(start_date, end_date):
        response = table.get_item(
            Key={
                'plant_id': 1,
                'date': single_date.strftime('%Y-%m-%d')
            }
        )
        count += 1
except ClientError as e:
    print(e.response['Error']['Message'])

now = time.time()

print("It took: ", now-then, " seconds to get ", count, " records of data for one month at plant 1")


###########################

then = time.time()

start_date = date(2018, 3, 1)
end_date = date(2018, 4, 1)

try:
    plant_filer = Key('plant_id').eq(1)
    date_filter = Key('date').between(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'));
    response = table.scan(
        FilterExpression=plant_filer & date_filter
    )
except ClientError as e:
    print(e.response['Error']['Message'])

now = time.time()

print("It took: ", now-then, " seconds to scan for ", len(response['Items']), " records of data for one month at plant 1")


###########################

then = time.time()

start_date = date(2018, 3, 1)
end_date = date(2018, 4, 1)

try:
    date_filter = Key('date').between(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'));
    response = table.scan(
        FilterExpression=date_filter
    )
except ClientError as e:
    print(e.response['Error']['Message'])

now = time.time()

print("It took: ", now-then, " seconds to scan for ", len(response['Items']), " records of data for one month at plants 1, 2, and 3")


###########################

then = time.time()

start_date = date(2013, 1, 1)
end_date = date(2018, 1, 1)

try:
    plant_filer = Key('plant_id').eq(1)
    date_filter = Key('date').between(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'));
    response = table.scan(
        FilterExpression=plant_filer & date_filter
    )
except ClientError as e:
    print(e.response['Error']['Message'])

now = time.time()

print("It took: ", now-then, " seconds to scan for ", len(response['Items']), " records of data for 5 years at plant 1")
