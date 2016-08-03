var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');


var queue = new Queue();
queue.fillBuffer();

/*
var x = new Replay('f7baabc985514298925dca87d423ba7b', 0, queue);
x.parseDataAtCheckpoint();
*/