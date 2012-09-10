var utils = require('./utils');
var redis = require("redis");
var config = require('./config');
var queue_utils = require('./lib/queue_utils');

//initialized logging (winston)
utils.initLog(config.logFile, config.logLevel);
var restart_in_process = false, num_errors = 0, retry_interval_query = 0, stats = {'expired_events': 0};
var clients = {};
var MAX_RETRY = 5;
if(config.max_retry) MAX_RETRY = config.max_retry;
start();

function start() {
    utils.logInfo("Starting ...\n");
    var timer_client = redis.createClient(config.redis_server_port, config.redis_server_name);
    var work_client = redis.createClient(config.redis_server_port, config.redis_server_name);
    var retry_client = redis.createClient(config.redis_server_port, config.redis_server_name);
    clients.timer = timer_client;
    clients.work = work_client;
    clients.retry = retry_client;
    restart_in_process = false;
    for(var key in clients) {
        clients[key].on("error", function(err) {
            utils.logError("error:" + err);
            handle_error();
        });
        clients[key].on("end", function() {
            utils.logDebug("connection closed, just ignore, redis will retry\n");
        });
    }
    work_client.on("ready", function() {
        save_retry_value();
    });
    retry_client.on("ready", function() {
        get_retry_msg(this);
    });
    timer_client.on("ready", function () {
         get_timer_msg(this);
    });
}
// key is in format of timer:ID
function process_expired_event(key) {
    update_stats();
    var items = key.split(":");
    if(items.length < 3) return;  // ignore bad key
    // get meta data associated with the ID
    clients.work.get("payload:" + items[3], function(err, reply) {
       if (err) {
           utils.logError("Get error: " + err);
           handle_error();
       } else if(reply != null ){
           utils.logDebug("Get meta data for " + items[3] + "as :" + reply);
           push_to_consumer_queue(clients.work, key, items[1], reply);
       }
       else {
           utils.logDebug("find no meta data for " + items[3]);
           push_to_consumer_queue(clients.work, key, items[1], "");
       }
    })
}
function restart() {
    if(!restart_in_process) {
        utils.logInfo("restarting in 10 seconds ...\n");
        for(var key in clients) {
            clients[key].quit();
            clients[key].closing = true;
          //  clients[key].removeAllListeners('error');
        }
        setTimeout(function() { start();}, 10000);
        restart_in_process = true;
    }
}

function push_to_consumer_queue(work_client, key, service_name,  metadata) {
    var msg = key;
    if(metadata != "") {
        msg += ":payload:" + metadata;
    }
    utils.logInfo("publish event: " + msg + " for consumer\n");
    work_client.lpush(queue_utils.get_consumer_queue(service_name), msg);
}
// message in format of timer:ID::metadata
// this functio is to schedule an event timer:ID 
function schedule_for_retry(work_client, message) {
    var key = message;
    var id = "";
    if(message.indexOf(":payload:") >= 0) {
        var items = message.split(":payload:");
        if(items.length == 2) {
            key = items[0];
        }
        else {
            return;
        }
    }
    var tokens = key.split(":timer:");
    if(tokens.length == 2) id = tokens[1];
    else return;
    work_client.get(id + ":status", function(err, value) {
      if(!err && value == "done") {
        utils.logDebug("This event was completed, no more retry");
      }
      else if(!err) {
        work_client.get(key + ":num_retry", function(err, value) {
          if(err) {
              utils.logError("Failed to get num_retry for:" + key + ", err=" + err);
          }
          else if(value && parseInt(value) > MAX_RETRY) {
              utils.logError("Exceeded max retry count, ignore event:" + key);
              work_client.del(key);
              work_client.del(key + ":num_retry");
              work_client.del("payload:" + id);
          }
          else {
              utils.logInfo("schedule " + key + " for retry\n");
              //schedle for retry
              work_client.set(key, "retry");
              new_retry = value ? parseInt(value) + 1 : 1;
              work_client.set(key + ":num_retry", new_retry);
              get_retry_interval(key.split(":")[1], function(err, value) {
                if(err) {
                  work_client.expire(key, config.default_retry_interval*Math.pow(2, new_retry - 1));
                }
                else {
                  if(value) {work_client.expire(key, value*Math.pow(2, new_retry-1));}
                  else {work_client.expire(key, config.default_retry_interval*Math.pow(2, new_retry - 1));}
                }
              });
            }
          });
        }
    });
}

function get_timer_msg(timer_client) {
    timer_client.blpop("expiration_channel", 0, function(err, reply) {
       if (err) {
           utils.logError("Get error on blpop: " + err);
           handle_error();
       } else if(reply != null ){
           if(reply[1].indexOf("timer:") > 0) {
               utils.logDebug("Get timer event: " + reply[1]);
               process_expired_event(reply[1]);
           }
       }
       get_timer_msg(timer_client);
    });
}

function get_retry_msg(retry_client) {
    retry_client.brpop(queue_utils.get_retry_queue(""), 0, function(err, reply) {
       if (err) {
           utils.logError("Get error on brpop: " + err);
           handle_error();
       } else if(reply != null ){
           utils.logDebug("Get retry key: " + reply[1]);
           schedule_for_retry(clients.work, reply[1]);
       }
       get_retry_msg(retry_client);
    });
}

function handle_error() {
    num_errors ++;
    if(num_errors == 100) { // use restart to slow down the flooding of reconnection in redis code
        num_errors = 0;
        restart();
    }
}

function get_retry_interval(service, callback) {
    key = service + "_retry_interval";
    retry_interval_query++;
    if(retry_interval_query == 100) { // to reduce redis query volume
        retry_interval_queuy = 0;
        clients.work.get(key, function(err, value) {
            if(!err) config[key] = value; // save to config
            callback(err, value);
        });
    }
    else {
      if(config[key]) {
          callback(null, config[key]);
      }
      else {
          callback(null, config.default_retry_interval);
      }
    }
}
        
function save_retry_value() {
    for (var key in config) {
        if(typeof key == "string" && key.indexOf("retry_interval") > 0) {
            console.log("save key/value of " + key + "/" + config[key] + " to redis");
            clients.work.set(key, config[key]);
        }
    }
}
process.on('SIGUSR2', function() {
    utils.logInfo("stats expired_events=" + stats.expired_events);
    //console.log("stats expired_events=" + stats.expired_events);
});

function update_stats() {
    stats.expired_events++;
    if(stats.expired_events == 999999) 
        stats.expired_events = 0;
}
