var uuid = require('node-uuid');
var redis = require('redis');
var EventEmitter = require('events').EventEmitter,
    util = require('util'), queue_utils = require('../lib/queue_utils');

/** constructor
    input:
      server: name of redis server
      port: listen port of redis server
      serverName: server name for this timer client
*/

function Timer_Client(server, port, serviceName, queue_by_hour) {
    this.server = server;
    this.port = port;
    this.service = serviceName;
    EventEmitter.call(this);
    var redis_client = redis.createClient(port, server);
    var self = this;
    redis_client.on("ready", function() {
        self.emit('ready');
    });
    redis_client.on("error", function(err) {
        self.emit('error');
    });
    this.queue_by_hour = false;
    if(queue_by_hour) this.queue_by_hour = true;
    this.redis_client = redis_client;
}

util.inherits(Timer_Client, EventEmitter);

/** create a timer
    id : timer id (UUID), if empty, a uuid will return
    data : opaque user data, will be sent back if timer fired, use empty string if there is no user data
    duration : expire in duration seconds
    return: UUID() to identify a timer
*/
Timer_Client.prototype.create_timer = function(duration, data) {
    var id = uuid();
    return this.create_timer_with_id(id, duration,  data);
}
Timer_Client.prototype.create_timer_with_id = function(id, duration, data) {
    var key = composeKey(this.service, id, "");
    return this.create_timer_with_key(key, duration, data);
}

Timer_Client.prototype.create_timer_with_key = function(key, duration, data) {
    if (duration <= 0) return "error";
    var id = get_timer_id(key);
    var timer_meta = get_timer_meta(duration, this.queue_by_hour);
    this.redis_client.zadd(timer_meta.qname, timer_meta.score, key);
    this.redis_client.set("timer_queue:" + key, timer_meta.qname);
    if(data != "") {
        this.redis_client.set("payload:" + id, data);
    }
    return id;
}

// cancel timer with id
Timer_Client.prototype.cancel = function(id) {
    var self = this;
    var key = composeKey(self.service, id, "");
    this.redis_client.del("payload:" + id);
    self.redis_client.get("timer_queue:" + key, function(err, res)
        {
            if(!err) {
                self.redis_client.zrem(res, key); 
            }
        });
    self.redis_client.del("timer_queue:" + key);
}
function composeKey(service, id, meta) {
    if(meta == "") {
        return "service:" + service + ":timer:" + id;
    }
    else {
        return "service:" + service + ":timer:" + id + ":payload:" + meta;
    }
}

function get_timer_meta(duration, queue_by_hour) {
    var d = new Date();
    if(queue_by_hour) {
        return {qname: "timer_" + (d.getUTCMonth() + 1) + d.getUTCDate() + d.getUTCHours(), score: parseInt(d.getTime()/1000, 10) + duration};
    }
    else {
        return {qname: "timer_" + (d.getUTCMonth() + 1) + d.getUTCDate(), score: parseInt(d.getTime()/1000, 10) + duration};
    }
}

function get_timer_id(key) {
    var index = key.indexOf("timer:");
    if(index > 0) {
      return key.substr(index + 7);
    }
    else return key;
}
exports.Timer_Client = Timer_Client;
