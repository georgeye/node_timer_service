var Timer_Client = require('../timer_client').Timer_Client, assert = require('assert'),
    queue_utils = require('../../lib/queue_utils');
var redis = require('redis'),
    redis_client = redis.createClient(6379, "127.0.0.1");
    redis_client.on('ready', function() {
        this.del("expiration_channel");
        this.del("events_queue");
        this.del("push_queue");
        start();
    });

    redis_client.on('error', function(err) {
        console.log("xxxx error");
    });


function start() {
    console.log('xxxxx start testing');
    var timer = new Timer_Client("127.0.0.1", 6379, "push");
    timer.on('error', function(err) {
        console.log('timer error');
    });
    timer.on('ready', function() {
        basic_test(timer); // create 3
        setTimeout(function() {
            test_creation(timer); }, 1*1000);  // create 100K
        setTimeout(function() {
            test_creation_with_cancel(timer)}, 10*1000);  //create 100k and cancel 100k
    });

}

function basic_test(timer) {

    // user defined timer id
    var id = timer.create_timer_with_id("my_id", 60, "");
    assert.equal(id, "my_id");
    
    // auto timer id
    id = timer.create_timer(60, "");
    redis_client.get("service:" + timer.service + ":timer:" + id, function(err, reply) {
        assert.equal(reply, 1);
    });

    //test user defined timer id with payload
    id = timer.create_timer_with_id("my_id1", 60, "my_payload");
    redis_client.get("payload:" + id, function(err, reply) {
        assert.equal("my_payload", reply);
    });

    timer.cancel(id);
    redis_client.get("payload:" + id, function(err, reply) {
        assert.notEqual("my_payload", reply);
    });
    redis_client.get("service:" + timer.service + ":timer:" + id, function(err, reply) {
        assert.notEqual(1, reply);
    });
    
    //auto timer id with payload
    id = timer.create_timer(60, "my_payload");
    redis_client.get("payload:" + id, function(err, reply) {
        assert.equal("my_payload", reply);
    });
}

function test_creation(timer) {
    console.log("going to create 100k timers...");
    //create 100000 timers
    for(var i = 0; i < 100000; i++) {
        timer.create_timer(60, "");
    }
}

function test_creation_with_cancel(timer) {
    console.log("going to create 100k timers and then cancel them...");
    //create 100000 timers
    for(var i = 0; i < 100000; i++) {
        var id = timer.create_timer(60, "");
        timer.cancel(id);
    }
    setTimeout(function() {
          checkTotal(timer.service, 3+100000, true); }, 160*1000);
}

function checkTotal(service, total, terminate) {
    console.log("checking total timer events");
    redis_client.llen(queue_utils.get_consumer_queue(service),  function(err, reply) {
        assert.equal(total, reply);
        if(terminate) {
            console.log("All test passed");
            process.exit(0);
        }
    });
}
