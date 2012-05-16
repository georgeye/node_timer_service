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

function Timer_Client(server, port, serviceName) {
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
    if (duration <= 0) return "error";
    if(id == "") return "error";
    var key = composeKey(this.service, id, "");
    this.redis_client.set(key, 1);
    this.redis_client.expire(key, duration);
    if(data != "") {
        this.redis_client.set("payload:" + id, data);
    }
    return id;
}

// cancel timer with id
Timer_Client.prototype.cancel = function(id) {
    var self = this;
    this.redis_client.get("payload:" + id, function(err, reply) {
        if (!err) {
            if(reply != null) {
                self.redis_client.lrem(queue_utils.get_consumer_queue(self.service), 0, composeKey(self.service, id, reply));
            }
            else {
                self.redis_client.lrem(queue_utils.get_consumer_queue(self.service), 0, composeKey(self.service, id, ""));
            }
        }
    });
    this.redis_client.del(composeKey(self.service, id, ""));
    this.redis_client.del("payload:" + id);
}
function composeKey(service, id, meta) {
    if(meta == "") {
        return "service:" + service + ":timer:" + id;
    }
    else {
        return "service:" + service + ":timer:" + id + ":payload:" + meta;
    }
}
exports.Timer_Client = Timer_Client;
