import boto3
from datetime import datetime
import json
import decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Pipeline')
agg_table = dynamodb.Table('PipelineAgg')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            plant_id = int(record['dynamodb']['NewImage']['plant_id']['N'])
            date = datetime.strptime(record['dynamodb']['NewImage']['date']['S'], '%Y-%m-%d')
            scheduled_qty = record['dynamodb']['NewImage']['scheduled_qty']['N']
            metered_qty = record['dynamodb']['NewImage']['metered_qty']['N']

            # read the aggregated record, if it exists
            try:
                response = agg_table.get_item(
                    Key={
                        'plant_id': plant_id,
                        'date': date.strftime('%Y-%m')
                    }
                )

                if 'Item' in json.dumps(response, indent=4, cls=DecimalEncoder):
                    scheduled_qty = ((int(response['Item']['scheduled_qty']) * int(date.day - 1)) + int(
                        scheduled_qty)) / int(date.day)
                    metered_qty = ((int(response['Item']['metered_qty']) * int(date.day - 1)) + int(metered_qty)) / int(
                        date.day)
            except ClientError as e:
                print('this is ok')

            # update the aggregate with a new value
            response = agg_table.put_item(
                Item={
                    'plant_id': plant_id,
                    'date': date.strftime('%Y-%m'),
                    'scheduled_qty': int(scheduled_qty),
                    'metered_qty': int(metered_qty)
                }
            )
        print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
    return 'Successfully processed {} records.'.format(len(event['Records']))


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)