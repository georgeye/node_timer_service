 var config = require('../config');
 var utils = require('../utils');
 var assert = require("assert");
 assert.equal(true, utils.is_forever_retry_service(config, "tt_archive"));
 assert.equal(false, utils.is_forever_retry_service(config, "something"));
 console.log("done the test");
