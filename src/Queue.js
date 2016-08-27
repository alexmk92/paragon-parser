var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var assert = require('assert');

var fs = require('fs');
var Memcached = require('memcached');
var memcached = new Memcached('paragongg-queue.t4objd.cfg.use1.cache.amazonaws.com:11211');

// determines if another worker is already fetching a job
//var fetching = false;
var removing = false;

var Queue = function(db, workers, processId) {
    this.mongoconn = db; // If null, couldn't connect
    this.maxWorkers = workers || 40;
    this.processId = processId;
    this.workers = [];

    this.initializeWorkers();
};

// Start up the workers
Queue.prototype.initializeWorkers = function() {
    for(var i = 0; i < this.maxWorkers; i++) {
        this.getNextJob();
    }
};

Queue.prototype.getNextJob = function() {
    // Check memcache and make sure we aren't running the job to clear dead replays
    memcached.get('clearDeadReservedReplays', function(err, data) {
        if(err || typeof data === 'undefined' || data === null) {
            if(this.workers.length < this.maxWorkers) {
                memcached.add('locked', true, 2, function(err) {
                    if(err) {
                        //console.log('[MEMCACHED ERR] '.red, err);
                        setTimeout(function() {
                            this.getNextJob();
                        }.bind(this), 250);
                    } else {
                        var conn = new Connection();
                        //Logger.writeToConsole('[QUEUE] Fetching next item to run on queue...'.cyan);

                        // Set the priority on the queue back to 0 once we start working it
                        //var selectQuery = 'SELECT * FROM queue WHERE completed = false AND reserved = false AND scheduled <= NOW() ORDER BY priority DESC LIMIT 1 FOR UPDATE';
                        var selectQuery = 'SELECT * FROM queue WHERE completed = false AND ISNULL(reserved_by) AND scheduled <= NOW() LIMIT 1 FOR UPDATE';
                        var updateQuery = 'UPDATE queue SET reserved_by="' + this.processId + '", priority=0';

                        conn.selectUpdate(selectQuery, updateQuery, function(replay) {
                            memcached.del('locked', function(err) {
                                if(err) {
                                    Logger.writeToConsole(err.red);
                                    setTimeout(function() {
                                        this.getNextJob();
                                    }.bind(this), 2000);
                                } else {
                                    Logger.writeToConsole('deleted cache lock'.green);
                                    if(typeof replay !== 'undefined' && replay !== null) {
                                        this.runTask(new Replay(this.mongoconn, replay.replayId, replay.checkpointTime, replay.attempts, this));
                                    } else {
                                        // we dont want to spam requests to get jobs if the queue is empty
                                        setTimeout(function() {
                                            this.getNextJob();
                                        }.bind(this), 250);
                                    }
                                }
                            }.bind(this));
                        }.bind(this));
                    }
                }.bind(this));
            } else {
                Logger.writeToConsole('[QUEUE] Not enough jobs running'.yellow);
                setTimeout(function() {
                    this.getNextJob();
                }.bind(this), 250);
            }
        } else {
            Logger.writeToConsole('[QUEUE] Removing locked replays in memcache');
            setTimeout(function() {
                this.getNextJob();
            }.bind(this), 250);
        }
    }.bind(this));
};

Queue.prototype.runTask = function(replay) {
    var found = false;
    this.workers.some(function(workerId) {
        found = workerId === replay.replayId;
        return found;
    });

    if(!found) {
        this.workers.push(replay.replayId);
        Logger.writeToConsole('[QUEUE] Running work for Replay: '.green + replay.replayId);
        replay.parseDataAtCheckpoint();
    } else {
        Logger.writeToConsole('[QUEUE] Another worker on this box is already running work for replay: '.yellow + replay.replayId + '. Fetching new job.'.yellow);
        this.getNextJob();
    }
};

/*
 * Removes the replay from the workers array
 */

Queue.prototype.workerDone = function(replay) {
    return new Promise(function(resolve, reject) {
        if(!removing) {
            removing = true;
            var index = -1;
            this.workers.some(function(workerId, i) {
                if(workerId === replay.replayId) {
                    index = i;
                    return true;
                }
                return false;
            });
            if(index > -1) {
                this.workers.splice(index, 1);
                Logger.writeToConsole('[QUEUE] Worker '.cyan + (index+1) + ' was successfully removed from the queue, there are '.cyan + this.workers.length + '/'.cyan + this.maxWorkers + ' workers running'.cyan);
            }
            resolve();
        } else {
            // Wait for the lock to release so we can remove a resource
            reject();
        }
    }.bind(this));
};

/*
 * The replay failed to process for some reason, we remove it from mongo here
 *  as its data could have potentially been corrupted.
 */

Queue.prototype.failed = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var scheduledDate = new Date(Date.now() + 120000);
        replay.replayJSON = null;
        var query = 'UPDATE queue SET reserved_by=null, completed = false, checkpointTime = 0, attempts = attempts + 1, priority = 2, scheduled = DATE_ADD(NOW(), INTERVAL 2 MINUTE) WHERE replayId = "' + replay.replayId + '"';

        conn.query(query, function(row) {
            if(typeof row !== 'undefined' && row.affectedRows !== 0) {
                Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
                this.deleteFile(replay);
                this.getNextJob();
            } else {
                Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, but there was an error when updating it'.red);
                this.getNextJob();
            }
        }.bind(this));
    }.bind(this), function() {
        removing = false;
        setTimeout(function() {
            this.workerDone(replay);
        }.bind(this), 50)
    }.bind(this));

};

/*
 * Schedules the replay to be revisited at a later date
 */

Queue.prototype.schedule = function(replay, ms) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var scheduledDate = new Date(Date.now() + ms);
        Logger.writeToConsole('[QUEUE] Scheduled to run: '.blue + replay.replayId + ' at: '.blue, scheduledDate);
        var query = 'UPDATE queue SET reserved_by=null, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            this.uploadFile(replay, function() {
                this.getNextJob();
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        removing = false;
        setTimeout(function() {
            this.workerDone(replay);
        }.bind(this), 50)
    }.bind(this));

};

/*
 * Removes a specific item from the queue, set its status to completed, this only happens
 * once a file has been fully processed
 */

Queue.prototype.removeItemFromQueue = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        this.uploadFile(replay, function(err) {
            if(err === null) {
                Logger.writeToConsole('[QUEUE] Replay '.green + replay.replayId + ' finished processing and uploaded to mongo successfully '.green + '✓');
                var query = 'UPDATE queue SET reserved_by=null, priority=0, completed=true, completed_at=NOW(), live=0, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
                conn.query(query, function() {});
                this.getNextJob();
            } else {
                Logger.writeToConsole('[QUEUE] There was an error when uploading file (this is callback from remove item from queue): '.red + err.message);
                replay.replayJSON = Replay.getEmptyReplayObject();
                this.failed(replay);
            }
        }.bind(this));
    }.bind(this), function() {
        removing = false;
        setTimeout(function() {
            this.workerDone(replay);
        }.bind(this), 50)
    }.bind(this));

};

/*
 * Removes a dead replay from the queue, this replay will never be serviced again
 */

Queue.prototype.removeDeadReplay = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            Logger.writeToConsole('[QUEUE] Replay '.red + replay.replayId + ' had either already been processed by another queue, or was a dead replay and reported no checkpoints in 6 minutes, removing from queue.'.red);
            this.deleteFile(replay);
            this.getNextJob();
        }.bind(this));
    }.bind(this), function() {
        removing = false;
        setTimeout(function() {
            this.workerDone(replay);
        }.bind(this), 50)
    }.bind(this));

};

/*
 * Removes a bot game from the queue
 */

Queue.prototype.removeBotGame = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            Logger.writeToConsole('[QUEUE] Replay removed as it is a bot game for: '.yellow + replay.replayId);
            this.getNextJob();
        }.bind(this));
    }.bind(this), function() {
        removing = false;
        setTimeout(function() {
            this.workerDone(replay);
        }.bind(this), 50)
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
                        Logger.writeToConsole('[QUEUE] Replay file: '.green + replay.replayId + ' uploaded to mongo!'.green);
                        callback(null);
                    }
                }.bind(this));
        } catch(e) {
            this.failed(replay);
            Logger.writeToConsole('[MONGO ERROR] in Queue.js when uploading relay: '.red + replay.replayId + '.  Error: '.red, e);
            callback({ message: 'failed to upload' });
        }
    } else {
        this.failed(replay);
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
                    Logger.writeToConsole('[QUEUE] Failed to remove replay file: '.red + replay.replayId + ' from mongo'.red);
                } else {
                    Logger.writeToConsole('[QUEUE] Replay: '.yellow + replay.replayId + ' was deleted from mongo successfully.'.yellow);
                }
            }.bind(this));
    } catch(e) {
        Logger.writeToConsole('[MONGO ERROR] in Queue.js when uploading replay: '.red + replay.replayId + '.  Error: '.red, e);
    }
};

/*
 * STATIC Unbinds all locked events from the Queue
 */

Queue.disposeOfLockedReservedEvents = function(processId, callback) {
    Logger.writeToConsole('disposing of locked events');
    var conn = new Connection();
    var query = 'UPDATE queue SET reserved_by=null WHERE reserved_by="' + processId + '"';
    conn.query(query, function() {
        Logger.writeToConsole('disposed');
        callback();
    });
};

module.exports = Queue;
