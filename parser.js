var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');
var cluster = require('cluster');
var config  = require('./conf.js');
var MongoClient = require('mongodb').MongoClient;

var url = 'mongodb://' + config.MONGO_HOST + '/' + config.MONGO_DATABASE;
var mongodb = null;
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

//catches uncaught exceptions
process.on('uncaughtException', function (err) {
    console.error((new Date).toUTCString() + ' uncaughtException:', err.message);
    console.error(err.stack);
    process.exit(1);
});

// Actual process code here
MongoClient.connect(url, function(err, db) {
    mongodb = db;
    if(cluster.isMaster) {
        // do the initial fork
        cluster.fork();

        cluster.on('exit', function(worker) {
            console.log('[PARSER] Process %s died. Restarting...', worker.process.pid);
            cluster.fork();
        });
    }
});