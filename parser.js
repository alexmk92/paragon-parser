/*
 * Entry point for the parser application, this should be started in PM2 with the command:
 *      > pm2 start parser.yaml
 *  Please ensure that pm2 is installed globally on the box running this software first
 *  by running
 *      > npm install pm2 -g
 *  It is not installed  as a dependency on this package as PM2 can be ran as a container
 *  for many node apps on the same system.
 */

require('dotenv').config();

var QueueClient = require('./src/QueueClient');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');
var cluster = require('cluster');
var MongoClient = require('mongodb').MongoClient;

//var Memcached = require('memcached');
//var memcached = new Memcached('paragongg-queue.t4objd.cfg.use1.cache.amazonaws.com:11211');

var url = '';
if(process.env.MONGO_URI) {
    url = process.env.MONGO_URI;
} else {
    url = 'mongodb://' + process.env.MONGO_HOST + ':' + process.env.MONGO_PORT + '/' + process.env.MONGO_DATABASE;
}

var mongodb = null;
var queue   = null;
var workers = process.env.WORKERS || 1;

// Generate a unique ID for this process so that we can identify it in MySQL
var processId = process.pid + '_' + Math.random().toString(12).substr(2, 8);

/**
 * @connect :
 * ----------
 * Makes a connection to mongo with the URL generated above, this will either be a URL based on a
 * Mongo cluster, or will be a url containing the single HOST:PORT combination.
 *
 * Once we make a successful connection a new Queue object is created if one doesn't already exist,
 * if anything fails here then PM2 will restart the process.
 */

MongoClient.connect(url, function(err, db) {
    mongodb = db;
    if(err) {
        Logger.writeToConsole('[MONGODB] Error connecting to MongoDB'.red, err);
    } else {
        Logger.writeToConsole('[PARSER] Process: '.cyan + processId + ' is building a queue with '.cyan + workers + ' workers'.cyan);
        if(!queue) queue = new QueueClient(mongodb, workers, processId);
    }
});

/**
 * @cleanup :
 * ----------
 * When a SIGNIT event is received by the process, we dispose of any open resources by attaining a
 * lock to memcached and then call @Queue.disposeOfLockedReservedEvents to unreserve any resources
 * allocated to this queue, allowing other processes to work on them.
 *
 * If this function cannot attain the lock from memcached, it will poll memcached every 2.5s
 * until it can run its query.  The lock will dispose itself after 15 seconds if it cannot
 * be released.
 */

function cleanup() {
    if(queue) queue.terminate();
    process.exit(0);
}

// If a process dies, dispose of its reserved events
process.on('SIGINT', function() {
    cleanup();
});
// If a process reaches an uncaught exception dispose of it
process.on('uncaughtException', function(err) {
    Logger.writeToConsole('[PARSER] Uncaught exception in Parser: '.red, err);
    cleanup();
});