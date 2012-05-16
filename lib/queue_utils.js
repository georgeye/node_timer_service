var default_queue = "events_queue",
    default_retry_queue = "retry_events_queue";

exports.get_consumer_queue = function(service_name) {
    if(service_name != "") return service_name + "_queue";
    else return default_queue;
}

exports.get_retry_queue = function() {
    return default_retry_queue;
}

