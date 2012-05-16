var config = {};

config.logFile = './log/trace.log';
config.logLevel = 4;
config.redis_server_port = 6379;
config.redis_server_name = '127.0.0.1';
config.default_retry_interval = 10; // in 10 seconds
config.push_retry_interval = 30; // in 10 seconds
module.exports = config;
