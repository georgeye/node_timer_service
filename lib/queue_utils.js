var default_queue = "events_queue",
    default_retry_queue = "retry_events_queue";

exports.get_consumer_queue = function(key) {
    return default_queue;
}

exports.get_retry_queue = function(key) {
    return default_retry_queue;
}

