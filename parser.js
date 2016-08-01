var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');


var queue = new Queue();
queue.fillBuffer();

/*
//var r = new Replay('74094ec30f1f4975a98e69699ac5bb61', queue);
var x = new Replay('3393407c050a477fb5ee948e304867e9', 60032, queue);
//r.parseDataAtCheckpoint();
x.parseDataAtCheckpoint();
*/