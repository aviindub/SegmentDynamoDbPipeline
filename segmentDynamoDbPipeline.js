var iron_mq = require('iron_mq');
var worker = require('node_helper');
var AWS = require("aws-sdk");
var Guid = require("guid");

var imq = new iron_mq.Client();
var queue = imq.queue(worker.config.IRON_MQ_QUEUE_NAME); // "segment"

AWS.config.update({
    accessKeyId: worker.config.AWS_ACCESS_KEY_ID,
    secretAccessKey: worker.config.AWS_SECRET_ACCESS_KEY,
    region: worker.config.AWS_REGION
});
var dd = new AWS.DynamoDB();

var messageToItem = function(message) {
    message_parsed = JSON.parse(message);
    var item = {
        "os_id": {"S": Guid.raw()},
        "timestamp": {"N": new Date(message_parsed.timestamp).valueOf().toString()},
        "message": {"S": message}
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
            queue.msg_release(message.id, {}, function(error, response) {
                if (error) {
                    console.log("error releasing message. message id:", message.id, "error:", error);
                }
            });
        }
        else {
            console.log("dynamodb putitem successful. message ID:", params.Item.os_id.S);
            queue.del(message.id, function(error, response) {
                if (error) {
                    console.log("error deleting message. message id:", message.id, "error:", error);
                }
            });
        }
    });
};

var options = {n: 100};
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
});
