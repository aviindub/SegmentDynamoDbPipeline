var iron_mq = require('iron_mq');
var worker = require('node_helper');
var AWS = require("aws-sdk");

var imq = new iron_mq.Client({
    token: worker.config.IRON_MQ_TOKEN,
    project_id: worker.config.IRON_MQ_PROJECT_ID,
    host: worker.config.IRON_MQ_HOST
});
var queue = imq.queue(worker.config.IRON_MQ_QUEUE_NAME);

AWS.config.update({
    accessKeyId: worker.config.AWS_ACCESS_KEY_ID,
    secretAccessKey: worker.config.AWS_SECRET_ACCESS_KEY,
    region: worker.config.AWS_REGION
});
var dd = new AWS.DynamoDB();

var messageToItem = function(message) {
    message_parsed = JSON.parse(message);
    var timestamp = new Date(message_parsed.timestamp).valueOf().toString();
    var item = {
        "os_id": { "S": message_parsed.messageId },
        "timestamp": { "N": timestamp },
        "ts_prefix" : { "N": timestamp.substring(0, 4) },
        "message": { "S": message }
    };
    return item;
};

var dynamoDbPutMessage = function(message) {
    var params = {
        "TableName": worker.config.SEGMENT_RAW_DATA_TABLE_NAME,
        "Item": messageToItem(message.body)
    }
    dd.putItem(params, function(error, data) {
        if (data && Object.keys(data).length > 0) {
            console.log("got data back from putItem:", data);
        }
        if (error) {
            console.log("error from putItem:", error);
            queue.msg_release(message.id, { reservation_id: message.reservation_id }, function(error, response) {
                if (error) {
                    console.log("error releasing message. message id:", message.id, "error:", error);
                }
            });
        }
        else {
            console.log("dynamodb putitem successful. message ID:", params.Item.os_id.S);
            queue.del(message.id, { reservation_id: message.reservation_id }, function(error, response) {
                if (error) {
                    console.log("error deleting message. message id:", message.id, "error:", error);
                }
            });
        }
    });
};

var getAndPut = function(i) {
    var options = {n: 100, timeout: 180};
    queue.get_n(options, function(error, messages) {
        if (error) {
            console.log("error getting messages from queue:", error);
        }
        if (messages) {
            console.log("successfully got", messages.length.toString(), "messages.");
            messages.forEach(function(message) {
               dynamoDbPutMessage(message);
            });
        }
        if (i - 1 > 0) {
            getAndPut(i - 1);
        }
    });
}

queue.info(function(error, results) {
    if (error) {
        console.log("error getting queue info:", error);
    }
    else if (results) {
        var i = Math.ceil(results.size / 100);
        if (results.size % 100 > 90) { i++; }
        console.log('queue contains', results.size, 'messages. performing', i, 'iterations.');
        getAndPut(i);
    }
});
