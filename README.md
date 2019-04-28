## Instructions for using repo
1. Use `create_data.py` to load 7 years of data for 3 fictitious locations.
2. Use `read_data.py` to get a sense of different queries you can construct and their respective durations.
3. Use `aggregate_data.py` as the Lambda code to aggregate days to months using DynamoDB streams
   - Keep in mind that DynamoDB Streams work in batch sizes of 100 and at the time of writing this is not configuratble.
4. Use `delete_data.py` to remove your DynamoDB data
5. Manually delete DynamoDB and Lambda resources.

## todo:
- [ ] Create DynamoDB and Lambda resources through CloudFormation
