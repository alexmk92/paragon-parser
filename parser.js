var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');

var cleaningUp = false;

var queue = new Queue();
queue.fillBuffer();

// Handle closing here:
process.stdin.resume();//so the program will not close instantly

function exitHandler(options) {
    if (options.cleanup && !cleaningUp) {
        cleaningUp = true;
        queue.stop(function() {
            // clean up any processes which were put on queue afterward (need to look at this but
            // its a temp plaster for now :))
            queue.stop(function() {
                cleaningUp = false;
                console.log('all workers have been stopped'.yellow);
                process.exit();
            });
        });
    }
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {cleanup:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {cleanup:true}));

