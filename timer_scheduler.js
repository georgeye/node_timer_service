var redis = require("redis");
var config = require("./config");
var utils = require('./utils');

utils.initLog(config.logFile, config.logLevel);
var MAX_EXPIRE_ENTRY = 100;
var DEFAULT_TIMER_INTERVAL = 20;
var queue_daily = true, queue_by_hour = false;
if(config.queue_daily == false) queue_daily = false;
if(config.queue_by_hour) queue_by_hour = true;
var work_client = redis.createClient(config.redis_server_port, config.redis_server_name);

setInterval(function() { handle_timeout(); }, DEFAULT_TIMER_INTERVAL);
function handle_timeout() {
  if(work_client.command_queue_length > 10 || work_client.offline_queue_length > 10) {
    utils.logWarn("too many pending requests, slow down retry");
  }
  else {
    var queues = get_queues(queue_daily, queue_by_hour);
    for(var i = 0; i < queues.length; i++) {
      utils.logDebug("going to fetch expired event from " + queues[i]);
      expire_events(queues[i]);
    }
  }
}

exports.handle_timeout = handle_timeout;

function expire_events(qname) {
  var d = new Date();
  var score = parseInt(d.getTime()/1000, 10);
  work_client.zrangebyscore(qname, 0, score, "limit", 0, MAX_EXPIRE_ENTRY, function(err, items) {
    if(!err) {
      for (var i=0; i < items.length; i++) {
        utils.logInfo("event=" + items[i] + " expired, publishing to expiration_channel");
        work_client.rpush("expiration_channel", items[i]);
        work_client.zrem(qname, items[i]);
      }
    }
  });
}

function get_queues(queue_daily, queue_by_hour) {
  var results = [];
  var d = new Date();
  if(queue_daily) {
    results.push(get_queue_name(d));
    results.push(get_queue_name(new Date(d.getTime() - 3600*24*1000)));
  }
  if(queue_by_hour) {
    results.push(get_queue_name_by_hour(d));
    results.push(get_queue_name_by_hour(new Date(d.getTime() - 3600*1000)));
  }
  return results;
}

function get_queue_name(time) {
  return "timer_" + (time.getUTCMonth() + 1) + time.getUTCDate();
}

function get_queue_name_by_hour(time) {
  return "timer_" + (time.getUTCMonth() + 1) + time.getUTCDate() + time.getUTCHours();
}

