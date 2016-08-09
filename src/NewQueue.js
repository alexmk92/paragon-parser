var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var assert = require('assert');
var fs = require('fs');

var conn = null;
var LOG_FILE = './logs/log.txt';

var Queue = function(db, workers) {
    this.mongoconn = db; // If null, couldn't connect

    this.maxWorkers = workers || 40;
    conn = new Connection(workers || 40);

    this.initializeWorkers();
};

// Start up the workers
    // Select a job from the queue thats not reserved and not in the future
    // Reserve resource
Queue.prototype.initializeWorkers = function() {
    for(var i = 0; i < this.maxWorkers; i++) {
        this.getNextJob(true);
    }
};

Queue.prototype.getNextJob = function(initializing) {
    if(!initializing) {
        //console.log('[QUEUE] Fetching next item to run on queue...'.cyan);
    }
    var selectQuery = 'SELECT * FROM queue WHERE reserved = false AND scheduled <= NOW() FOR UPDATE';
    var updateQuery = 'UPDATE queue SET reserved=1';

    conn.selectUpdate(selectQuery, updateQuery, function(replay) {
        if(typeof replay !== 'undefined' && replay !== null) {
            this.runTask(new Replay(this.mongoconn, replay.replayId, replay.checkpointTime, replay.attempts, this));
        } else {
            // we dont want to spam requests to get jobs if the queue is empty
            setTimeout(function() {
                this.getNextJob();
            }.bind(this), 5000);
        }
    }.bind(this));
};

Queue.prototype.runTask = function(replay) {
    //console.log('[QUEUE] Running work for Replay: '.green + replay.replayId);
    replay.parseDataAtCheckpoint();
};

/*
 * The replay failed to process for some reason, we remove it from mongo here
 *  as its data could have potentially been corrupted.
 */

Queue.prototype.failed = function(replay) {
    var scheduledDate = new Date(Date.now() + 120000);
    replay.replayJSON = null;
    var query = 'UPDATE queue SET completed = false, checkpointTime = 0, attempts = attempts + 1, priority = 2, scheduled = DATE_ADD(NOW(), INTERVAL 2 MINUTE), reserved = false WHERE replayId = "' + replay.replayId + '"';

    conn.query(query, function(row) {
        if(row.affectedRows !== 0) {
            console.log('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
            Logger.append(LOG_FILE, new Date() + ' The replay with id: ' + replay.replayId + ' failed, its scheduled to re-run at ' + scheduledDate);
            this.deleteFile(replay);
            this.getNextJob(false);
        } else {
            console.log('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, but there was an error when updating it'.red);
            Logger.append('./logs/log.txt', new Date() + ' Failed to update the failure attempt for ' + replay.replayId + '. Used query: ' + query + ', returned:' + JSON.stringify(row));
            this.getNextJob(false);
        }
    }.bind(this));
};

/*
 * Schedules the replay to be revisited at a later date
 */

Queue.prototype.schedule = function(replay, ms) {
    var scheduledDate = new Date(Date.now() + ms);
    //console.log('[QUEUE] Scheduled to run: '.blue + replay.replayId + ' at: '.blue, scheduledDate);
    var query = 'UPDATE queue SET reserved = false, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3 WHERE replayId="' + replay.replayId + '"';
    conn.query(query, function() {
        this.uploadFile(replay, function() {
            this.getNextJob(false);
        }.bind(this));
    }.bind(this));
};

/*
 * Removes a specific item from the queue, set its status to completed, this only happens
 * once a file has been fully processed
 */

Queue.prototype.removeItemFromQueue = function(replay) {
    this.uploadFile(replay, function(err) {
        if(err === null) {
            console.log('[QUEUE] Replay '.green + replay.replayId + ' finished processing and uploaded to mongo successfully!'.green);
            var query = 'UPDATE queue SET completed=true, completed_at=NOW(), live=0 WHERE replayId="' + replay.replayId + '"';
            conn.query(query, function() {});
            this.getNextJob();
        } else {
            console.log('[QUEUE] There was an error when uploading file (this is callback from remove item from queue): '.red + err.message);
            replay.replayJSON = Replay.getEmptyReplayObject();
            this.failed(replay);
        }
    }.bind(this));
};

/*
 * Removes a dead replay from the queue, this replay will never be serviced again
 */

Queue.prototype.removeDeadReplay = function(replay) {
    var query = 'UPDATE queue SET completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
    conn.query(query, function() {
        Logger.append('./logs/failedReplays.txt', 'Replay: ' + replay.replayId + ' was either empty or had been processed before and has been removed from the queue');
        console.log('[QUEUE] Replay '.red + replay.replayId + ' had either already been processed by another queue, or was a dead replay and reported no checkpoints in 6 minutes, removing from queue.'.red);
        this.deleteFile(replay);
        this.getNextJob(false);
    }.bind(this));
};

/*
 * Uploads the replay json file to mongo
 */

Queue.prototype.uploadFile = function(replay, callback) {
    if(replay.replayJSON !== null) {
        try {
            // get a lock on this specific item
            this.mongoconn.collection('matches').update(
                { replayId: replay.replayId },
                { $set: replay.replayJSON },
                { upsert: true},
                function(err, results) {
                    if(err) {
                        callback({ message: 'failed to upload file'});
                    } else {
                        //console.log('[QUEUE] Replay file: '.green + replay.replayId + ' uploaded to mongo!'.green);
                        callback(null);
                    }
                }.bind(this));
        } catch(e) {
            console.log('[MONGO ERROR] in Queue.js when uploading relay: '.red + item.replayId + '.  Error: '.red, e);
            callback({ message: 'failed to upload' });
            //Logger.append('./logs/mongoError.txt', 'Mongo error: ' + JSON.stringify(e));
        }
    } else {
        callback({ message: 'replay JSON was null'})
    }
};

/*
 * Removes a file from mongo
 */

Queue.prototype.deleteFile = function(replay) {
    try {
        // get a lock on this specific item
        this.mongoconn.collection('matches').deleteOne(
            { replayId: replay.replayId },
            function(err, results) {
                if(err) {
                    console.log('[QUEUE] Failed to remove replay file: '.red + replay.replayId + ' from mongo'.red);
                } else {
                    console.log('[QUEUE] Replay: '.yellow + replay.replayId + ' was deleted from mongo successfully.'.yellow);
                }
            }.bind(this));
    } catch(e) {
        console.log('[MONGO ERROR] in Queue.js when uploading replay: '.red + replay.replayId + '.  Error: '.red, e);
        //Logger.append('./logs/mongoError.txt', 'Mongo error: ' + JSON.stringify(e));
    }
};

//

module.exports = Queue;