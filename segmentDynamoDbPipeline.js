var iron_mq = require('iron_mq');
var AWS = require("aws-sdk");
var DOC = require("dynamodb-doc");

var imq = new iron_mq.Client();
var queue = imq.queue("my_queue");
var docClient = new DOC.DynamoDB();

AWS.config.update({
    accessKeyId: worker.config.AWS_ACCESS_KEY_ID,
    secretAccessKey: worker.config.AWS_SECRET_ACCESS_KEY,
    region: worker.config.AWS_REGION
});

var worker = require('node_helper');

console.log("params:", worker.params);
//console.log("config:", worker.config);
console.log("task_id:", worker.task_id);


var options = {n: 100};
queue.get_n(options, function(error, messages) {
    messages.forEach(function(message) {
        // do we need to insert a GUID or something on the message?
        docClient.putItem(JSON.parse(message.body), function(err, data) {
            if (err) {
                //log error
            }
            else {
                message.delete();
            }
            if (data) {
                //what is this?
            }
        });
    });
});

