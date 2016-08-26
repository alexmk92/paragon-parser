var config  = require('./conf.js');

if(config.PROFILING) {
    require('newrelic');
}

var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');
var cluster = require('cluster');
var MongoClient = require('mongodb').MongoClient;

//var url = 'mongodb://' + config.MONGO_HOST + '/' + config.MONGO_DATABASE;

var url = '';
if(config.MONGO_URI !== null) {
    url = config.MONGO_URI;
} else {
    url = 'mongodb://' + config.MONGO_HOST + '/' + config.MONGO_DATABASE;
}

var mongodb = null;
var queue   = null;
var workers = 1;

// Take command line arguments
process.argv.some(function (val, index) {
    if(index == 2) {
        var param = val.split('=');
        if (param[0] == '--workers') {
            workers = param[1];
            return true;
        }
    }
    return false;
});


process.on('uncaughtException', function (err) {
    console.error((new Date).toUTCString() + ' uncaughtException:', err.message);
    console.error(err.stack);
    process.exit(1);
});

MongoClient.connect(url, function(err, db) {
    mongodb = db;
    if(err) {
        console.log('[MONGODB] Error connecting to MongoDB'.red, err);
    } else {
        
    }
    if(cluster.isMaster) {

        cluster.fork();

        cluster.on('exit', function(worker) {
            console.log('[PARSER] Process %s died. Restarting...', worker.process.pid);
            cluster.fork();
        });
    }

    if(cluster.isWorker) {
        var cleaningUp = false;

        if(!queue) queue = new Queue(mongodb, workers);

        // Handle closing here:
        //process.stdin.resume();//so the program will not close instantly

        // function cleanup() {
        //     if(queue) { queue.killConnections(); }
        //     if (!cleaningUp) {
        //         cleaningUp = true;
        //         /*
        //         queue.stop(function() {
        //             // clean up any processes which were put on queue afterward (need to look at this but
        //             // its a temp plaster for now :))
        //             queue.stop(function() {
        //                 cleaningUp = false;
        //                 console.log('[PARSER] All workers were shut down successfully'.yellow);
        //                 process.exit();
        //             });
        //         });
        //         */
        //     }
        // }
        //
        // //do something when app is closing
        // process.on('exit', cleanup);
        //
        // //catches ctrl+c event
        // process.on('SIGINT', cleanup);
        //
        // //catches uncaught exceptions
        // process.on('uncaughtException', function(err) {
        //     console.log('[PARSER] Uncaught Exception: '.red, err);
        //     cleanup();
        // });
    }
});