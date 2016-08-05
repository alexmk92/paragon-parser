var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');
var cluster = require('cluster');
var config  = require('./conf.js');
var MongoClient = require('mongodb').MongoClient;

var url = 'mongodb://' + config.MONGO_HOST + '/paragon';
var mongodb = null;
var queue   = null;

MongoClient.connect(url, function(err, db) {
    mongodb = db;
});

if(cluster.isMaster) {
    cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        console.log('FORKING A NEW PROCESS'.red);
        cluster.fork();
    });
}

if(cluster.isWorker) {
    var cleaningUp = false;

    if(!queue) queue = new Queue(mongodb);
    queue.fillBuffer();

    // Handle closing here:
    process.stdin.resume();//so the program will not close instantly

    function cleanup() {
        if (!cleaningUp) {
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
    process.on('exit', cleanup);

    //catches ctrl+c event
    process.on('SIGINT', cleanup);

    //catches uncaught exceptions
    process.on('uncaughtException', function(err) {
        console.log('UNCAUGHT EXCEPTION: '.red, err);
        cleanup();
    });
}