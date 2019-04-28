from __future__ import print_function # Python 2/3 compatibility
import boto3
import random
from datetime import timedelta, date
import time

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

then = time.time()

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Pipeline')

plant_ids = [1, 2, 3]
start_date = date(2012, 1, 1)
end_date = date(2019, 1, 1)
count = 0

for plant_id in plant_ids:
    for single_date in daterange(start_date, end_date):
        response = table.put_item(
           Item={
               'plant_id': plant_id,
               'date': single_date.strftime("%Y-%m-%d"),
               'scheduled_qty': random.randint(1000, 100000),
               'metered_qty': random.randint(1000, 100000)
            }
        )
        count += 1

now = time.time()

print("It took: ", now-then, " seconds to load", count, " records")
