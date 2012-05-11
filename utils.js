var winston = require('winston');
var config = require('./config');

exports.initLog = function(name) {
  winston.add(winston.transports.File, { filename: name, maxsize: 1000000});
  winston.remove(winston.transports.Console);
}
exports.logInfo = function(log) {
  if(config.logLevel >= 4) {
    var current = new Date();
    winston.info(current + ' PID:' + process.pid + ' ' + log);
  }
}

exports.logDebug = function(log) {
  if(config.logLevel >=5) {
    var current = new Date();
    winston.debug(current + ' PID:' + process.pid + ' ' + log);
  }
}

exports.logError = function(log) {
  if(config.logLevel >= 2) {
    var current = new Date();
    winston.error(current + ' PID:' + process.pid + ' ' + log);
  }
}

