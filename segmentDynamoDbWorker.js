var iron_mq = require('iron_mq');
var imq = new iron_mq.Client();
var queue = imq.queue("my_queue");
var AWS = require("aws-sdk");
var DOC = require("dynamodb-doc");

AWS.config.update({region: "us-west-1"});

var docClient = new DOC.DynamoDB();

var options = {n: 100};
queue.get_n(options, function(error, messages) {
    messages.forEach(function(message) {
        docClient.putItem(JSON.parse(message.body), function(err, data) {
            if (err) {
                //log error
            }
            if (data) {
                //what is this?
            }
        });
    })
});

