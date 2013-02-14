var uuid = require('node-uuid');
var redis = require('redis');
var EventEmitter = require('events').EventEmitter,
    util = require('util'),
    queue_utils = require('../lib/queue_utils');

/** constructor
    input:
      server: name of redis server
      port: listen port of redis server
      serverName: server name for this queue client
*/
function Queue_Client(server, port, serviceName) {
    this.server = server;
    this.port = port;
    if(!serviceName) this.service = "";
    else  this.service = serviceName;
    EventEmitter.call(this);
    var redis_client = redis.createClient(port, server);
    var redis_work = redis.createClient(port, server);
    var self = this;
    this.ready_count = 0;
    redis_client.on("ready", function() {
        self.ready_count++;
        if(self.ready_count == 2) {
            self.ready_count = 0;
            self.emit('ready');
        }
    });
    redis_client.on("error", function(err) {
        self.emit('error', err);
    });
    redis_work.on("error", function(err) {
        self.emit('error', err);
    });
    redis_work.on("ready", function() {
        self.ready_count++;
        if(self.ready_count == 2) {
            self.ready_count = 0;
            self.emit('ready');
        }
    });
    this.redis_client = redis_client;
    this.redis_work = redis_work;
}

util.inherits(Queue_Client, EventEmitter);

Queue_Client.prototype.quit = function() {
  this.redis_client.quit();
  this.redis_work.quit();
}
/** enqueue element
   input: 
   payload - meta data associated with event
   return id - event id, cleint to use this id to call complete() when this event is handled successfully
*/
Queue_Client.prototype.queue = function(payload) {
    return this.queue_with_id(uuid(), payload);
}

Queue_Client.prototype.queue_with_service = function(service, payload) {
    return this.queue_with_id_service(uuid(), service, payload);
}

Queue_Client.prototype.queue_with_id_service = function(id, service, payload) {
    if(!payload) return "error";
    key = "service:" + service + ":timer:" + id + ":payload:" + payload;
    this.redis_work.set("payload:" + id, payload);
    this.redis_work.lpush(queue_utils.get_consumer_queue(service), key);
    return id;
}
    
Queue_Client.prototype.queue_with_id = function(id, payload) {
    return this.queue_with_id_service(id, this.service, payload);
}

/**
    callback = function(err, id, payload)
*/
Queue_Client.prototype.next = function(callback) {
  self = this;
    this.redis_client.brpoplpush(queue_utils.get_consumer_queue(this.service), queue_utils.get_retry_queue(), 0, function(err, reply) {
        if(err) callback(err, null, null);
        else {
            var key = reply, payload = "";
            if(reply.indexOf(":payload:") > 0) {
                var items = reply.split(":payload:");
                key = items[0],
                payload = items[1];
            }
            var tokens = key.split(":");
            if(tokens.length == 4) {
                self.redis_work.get(tokens[3] + ":status", function(err, res)
                {
                  if(!err && res == "done") {
                    self.complete(tokens[3]);
                    //console.log("ignore completed event: " + "service:" + self.service + ":timer:" + tokens[3]);
                    self.next(callback);
                  }
                  else {
                    callback(err, tokens[3], payload);
                  }
                });
            }
            else {
                callback("error", "", "");
            }
        }
    });
}
/* confime that an element has been handled successfully,
   so that this element can be removed from the system.
*/
Queue_Client.prototype.complete = function(id) {
    var self = this;
    //set status for the event, so that won't be rescheduled in the future
    self.redis_work.set(id + ":status", "done");
    self.redis_work.expire(id + ":status", 30*60); //auto cleanup in half an hour
    this.redis_work.del("service:" + this.service + ":timer:" + id);
    this.redis_work.del("service:" + this.service + ":timer:" + id + ":num_retry");
    this.redis_work.del("payload:" + id);
}

/* report error on handling event, expect to schedule this event again in the interval future
   input:
   id - event id
   payload - meta data associated with event
   interval - in seconds
*/
Queue_Client.prototype.error = function(id, payload, interval) {
    var key = "service:" + this.service + ":timer:" + id;
    this.redis_work.set(key, 1);
    this.redis_work.expire(key, interval);
    if(payload)
        this.redis_work.set("payload:" + id, payload);
}

Queue_Client.prototype.setTimeout = function(interval) {
    this.redis_work.set(this.service + "_retry_interval", interval);
}

exports.Queue_Client = Queue_Client;
