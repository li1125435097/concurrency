var express = require('express');
var router = express.Router();
var redis = require('redis');
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var kafkaClient = new kafka.KafkaClient();
var producer = new Producer(kafkaClient);

var topicsToCreate = [{
    topic: 'topic1',
    partitions: 1,
    replicationFactor: 2
  },
  {
    topic: 'topic2',
    partitions: 5,
    replicationFactor: 3,
    // Optional set of config entries
    configEntries: [
      {
        name: 'compression.type',
        value: 'gzip'
      },
      {
        name: 'min.compaction.lag.ms',
        value: '50'
      }
    ],
    // Optional explicit partition / replica assignment
    // When this property exists, partitions and replicationFactor properties are ignored
    replicaAssignment: [
      {
        partition: 0,
        replicas: [3, 4]
      },
      {
        partition: 1,
        replicas: [2, 1]
      }
    ]
  }];
kafkaClient.createTopics(topicsToCreate,(error, result) => {
    console.log('话题建立成功-----------------')
  })
var count = 0;
console.log(123)
router.get('/', function (req, res) {
    console.log('count=' + count++);
    var fn = function (optionalClient) {
        if (optionalClient == 'undefined' || optionalClient == null) {
            var client = redis.createClient();
        }else{
            var client = optionalClient;
        }
        client.on('error', function (er) {
            console.trace('Here I am');
            console.error(er.stack);
            client.end(true);
        });
        client.watch("counter");
        client.get("counter", function (err, reply) {
            if (parseInt(reply) > 0) {
                var multi = client.multi();
                multi.decr("counter");
                multi.exec(function (err, replies) {
                    if (replies == null) {
                        console.log('should have conflict')
                        fn(client);
                    } else {
                        var payload = [
                            {
                                topic: 'CAR_NUMBER',
                                messages: 'buy 1 car',
                                partition: 0
                            }
                        ];
                        producer.send(payload, function (err, data) {
                            console.log(data,err,'----------------------');
                        });
                        res.send(replies);
                        client.end(true);
                    }
                });
            } else {
                console.log("sold out!");
                res.send("sold out!");
                client.end(true);
            }
        })
    };
    fn();
});

module.exports = router;