var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');


var queue = new Queue();

var r = new Replay('74094ec30f1f4975a98e69699ac5bb61', queue);
r.parseDataAtCheckpoint();

/*
setInterval(function() {
    queue.next();
}, 100);
*/