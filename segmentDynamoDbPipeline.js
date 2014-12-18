var iron_mq = require('iron_mq');
var imq = new iron_mq.Client();
var queue = imq.queue("my_queue");
var AWS = require("aws-sdk");
var DOC = require("dynamodb-doc");


var worker = require('node_helper');

console.log("params:", worker.params);

console.log("config:", worker.config);

console.log("task_id:", worker.task_id);



AWS.config.update({region: "us-west-1"});

var docClient = new DOC.DynamoDB();

var options = {n: 100};
queue.get_n(options, function(error, messages) {
    messages.forEach(function(message) {
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

