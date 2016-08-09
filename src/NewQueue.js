var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var assert = require('assert');
var fs = require('fs');

var conn = new Connection();
var LOG_FILE = './logs/log.txt';

var Queue = function(db, workers) {
    this.maxWorkers = 40;
};

// Start up the workers
    // Select a job from the queue thats not reserved and not in the future
    // Reserve resource
Queue.prototype.initializeWorkers = function() {
    for(var i = 0; i < this.maxWorkers; i++) {
        var selectQuery = 'SELECT * FROM queue WHERE reserved = false AND scheduled <= NOW() FOR UPDATE';
        var updateQuery = 'UPDATE queue SET reserved=1';

        conn.selectUpdate(selectQuery, updateQuery, function(replay) {
            console.log('replay is: ', replay.replayId);
        });
    }
};

//

module.exports = Queue;
