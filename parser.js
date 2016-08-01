var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');


var queue = new Queue();

var r = new Replay('74094ec30f1f4975a98e69699ac5bb61', queue);
var x = new Replay('5eeaf1d0edb24d368136fa1e256efabf', queue);
r.parseDataAtCheckpoint();
x.parseDataAtCheckpoint();

/*
async.parallel([
], function() {

});
*/


/*
setInterval(function() {
    queue.next();
}, 100);
*/