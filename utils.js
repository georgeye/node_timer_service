var winston = require('winston');

var logLevel = 5;
exports.initLog = function(name, logLevel_) {
  winston.add(winston.transports.File, { filename: name, maxsize: 1000000});
  winston.remove(winston.transports.Console);
  logLevel = logLevel_;
}
exports.logInfo = function(log) {
  if(logLevel >= 4) {
    var current = new Date();
    winston.info(current + ' PID:' + process.pid + ' ' + log);
  }
}

exports.logDebug = function(log) {
  if(logLevel >=5) {
    var current = new Date();
    winston.debug(current + ' PID:' + process.pid + ' ' + log);
  }
}

exports.logWarn = function(log) {
  if(logLevel >= 3) {
    var current = new Date();
    winston.warn(current + ' PId:' + process.pid + ' ' + log);
  }
}

exports.logError = function(log) {
  if(logLevel >= 2) {
    var current = new Date();
    winston.error(current + ' PID:' + process.pid + ' ' + log);
  }
}

