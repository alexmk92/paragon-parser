/*
 * Entry point for the parser application, this should be started in PM2 with the command:
 *      > pm2 start parser.yaml
 *  Please ensure that pm2 is installed globally on the box running this software first
 *  by running
 *      > npm install pm2 -g
 *  It is not installed as a dependency on this package as PM2 can be ran as a container
 *  for many node apps on the same system.
 */

require('dotenv').config();

var Queue = require('./src/Queue');
var Logger = require('./src/Logger');
var Replay = require('./src/Replay');
var async = require('async.js');
var colors = require('colors');
var cluster = require('cluster');
var MongoClient = require('mongodb').MongoClient;

var url = '';
if(process.env.MONGO_URI) {
    url = process.env.MONGO_URI;
} else {
    url = 'mongodb://' + process.env.MONGO_HOST + '/' + process.env.MONGO_DATABASE;
}

var mongodb = null;
var queue   = null;
var workers = process.env.WORKERS || 1;

MongoClient.connect(url, function(err, db) {
    mongodb = db;
    if(err) {
        Logger.writeToConsole('[MONGODB] Error connecting to MongoDB'.red, err);
    } else {
        Logger.writeToConsole('[PARSER] Building a queue with '.cyan + workers + ' workers'.cyan);
        if(!queue) queue = new Queue(mongodb, workers);
    }
});