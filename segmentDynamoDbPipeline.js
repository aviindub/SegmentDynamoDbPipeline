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

// console.log("params:", worker.params);
// console.log("config:", worker.config);
// console.log("task_id:", worker.task_id);

var messageToItem = function(message) {
    message_parsed = JSON.parse(message);
    // console.log("type of params part deux:", typeof message);
    // console.log("message parsed:", typeof message_parsed);
    // console.log("message.timestamp is", message_parsed.timestamp);
    var item = {
        "os_id": {"S": Guid.raw()},
        "timestamp": {"N": new Date(message_parsed.timestamp).valueOf().toString()},
        // "message": {"S": JSON.stringify(message)}
        "message": {"S": message}
    };
    return item;
};

var dynamoDbPutMessage = function(message) {
    var params = {
        "TableName": worker.config.SEGMENT_RAW_DATA_TABLE_NAME,
        "Item": message
    }
    dd.putItem(params, function(err, data) {
        if (data && Object.keys(data).length > 0) {
            console.log("got data back from putItem:", data);
        }
        if (err) {
            console.log("error from putItem:", err);
            // console.log("message that errored:", message);
        }
        else {
            console.log("dynamodb putitem successful. message ID:", message.os_id.S);
            message.delete();
        }
    });
};

//debug
// if (worker.params) {
//     console.log("type of params:", typeof worker.params);
//     dynamoDbPutMessage(worker.params);
// }
// console.log("typeof imq:", typeof imq);
// console.log("imq:", imq);
// imq.queues(options, function(error, body) {
//     if (error) {
//         console.log("error getting queues:", error);
//     }
//     if (body) {
//         console.log("queues body:", body);
//     }
// });
//end debug

// var options = {n: 100};
var options = {n: 5};
queue.get_n(options, function(error, messages) {
    if (error) {
        console.log("error getting messages from queue:", error);
    }
    if (messages) {
        console.log("successfully got", messages.length.toString(), "messages");
        messages.forEach(function(message) {
           dynamoDbPutMessage(messageToItem(message.body));
        });
        // console.log("type of messages:", typeof messages);
        // console.log("type of messages[0]:", typeof messages[0]);
        // console.log("and a message:", messages[0]);
    }
});
