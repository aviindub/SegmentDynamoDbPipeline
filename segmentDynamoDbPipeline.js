var iron_mq = require('iron_mq');
var worker = require('node_helper');
var AWS = require("aws-sdk");
var Guid = require("guid");

var imq = new iron_mq.Client();
var queue = imq.queue("my_queue");

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
    // message_parsed = JSON.parse(message);
    // console.log("type of params part deux:", typeof message);
    var item = {
        "os_id": {"S": Guid.raw()},
        "timestamp": {"N": new Date(message.timestamp).valueOf().toString()},
        "message": {"S": JSON.stringify(message)}
    };
    return item;
};

var dynamoDbPutMessage = function(message) {
    var params = {
        "TableName": worker.config.SEGMENT_RAW_DATA_TABLE_NAME,
        "Item": messageToItem(message)
    }
    dd.putItem(params, function(err, data) {
        if (data) {
            //what is this?
            console.log("got data back from putItem:", data);
        }
        if (err) {
            console.log(err);
            // process.exit();
        }
        else {
            console.log("no error. exiting.");
            message.delete();
            // process.exit();
        }
    });
};

//debug
if (worker.params) {
    //console.log("type of params:", typeof worker.params);
    dynamoDbPutMessage(worker.params);
}
//end debug

var options = {n: 100};
queue.get_n(options, function(error, messages) {
    if (error) {
        console.log("error getting messages from queue:", error);
    }
    if (messages) {
        messages.forEach(function(message) {
            dynamoDbPutMessage(message);
        });
    }
});
