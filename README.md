SegmentDynamoDbPipeline
=======================

An extremely simple worker process for taking Segment.io data via Iron.io and sending to AWS DynamoDB.


to upload

```
iron_worker upload SegmentDynamoDbPipeline
```

to upload with a config file, retries, and maximum concurrency limit
```
iron_worker upload SegmentDynamoDbPipeline --worker-config config.json --retries 3 --max-concurrency 10
```

queue a task via cli with optional parameters
```
iron_worker queue SegmentDynamoDbPipeline --priority 1 --cluster mem1 --timeout 3600
```
