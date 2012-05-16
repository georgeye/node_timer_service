var Queue_Client = require('../queue_client').Queue_Client, assert = require('assert');
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
    var queue = new Queue_Client("127.0.0.1", 6379, "push");
    queue.on('error', function(err) {
        console.log('timer error');
    });
    queue.on('ready', function() {
        basic_test(queue); 
        setTimeout(function() {
        enqueue_test(queue, 10);}, 10*1000);
        setTimeout(function() {
        dequeue_and_complete_test(queue, 10);}, 30*1000);
        setTimeout( function() {
            check_result(queue); }, 60*1000);
    });
}

function basic_test(queue) {
    var id = queue.queue("mypayload");
    queue.next(function(err, id_, meta) {
        assert.equal(err, null);
        assert.equal(id, id_);
        assert.equal(meta, "mypayload");
        setTimeout(function() {
        queue.complete(id);}, 100);
    });
}

function enqueue_test(queue, num) {
    setTimeout(function() {
        enqueue(queue, num);
        num--;
        if(num >0)
        enqueue_test(queue, num);
        }, 100);
}

function enqueue(queue, i) {
    queue.queue("payload" + i);
}

function dequeue_and_complete_test(queue, num) {
    if(num <= 0) return;
    queue.next(function(err, id, payload) {
        console.log("get " + id + " with payload=" + payload);
        assert.equal(err, null);
        assert.equal(payload, "payload" + num);
        setTimeout(function() {
        queue.complete(id);
        dequeue_and_complete_test(queue, num-1); }, 10);
    });
}

function check_result(queue) {
    redis_client.llen(queue.service + "_queue", function(err, len) {
        assert.equal(err, null);
        assert.equal(len, 0);
        console.log("all test passed\n");
        process.exit(0);
    });
}
