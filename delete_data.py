from __future__ import print_function # Python 2/3 compatibility
import boto3
from botocore.exceptions import ClientError
from datetime import timedelta, date
import time

then = time.time()

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Pipeline')

plant_ids = [1, 2, 3]
start_date = date(2012, 1, 1)
end_date = date(2019, 1, 1)
count = 0

for plant_id in plant_ids:
    for single_date in daterange(start_date, end_date):
        try:
            response = table.delete_item(
               Key={
                   'plant_id': plant_id,
                   'date': single_date.strftime("%Y-%m-%d")
                }
            )
            count += 1
        except ClientError as e:
            print('Error with ', plant_id, 'and', single_date)

now = time.time()

print("It took: ", now-then, " seconds to delete", count, " records")
