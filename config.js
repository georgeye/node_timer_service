var config = {};

config.logFile = 'trace.log';
config.logLevel = 4;
config.redis_server_port = 6379;
config.redis_server_name = '127.0.0.1';
config.default_retry_interval = 10; // in 10 seconds
config.att_convert_retry_interval = 60; // in 60 seconds for transmog
config.no_retry_services = ['value'];
module.exports = config;
