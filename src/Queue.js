var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var assert = require('assert');

var fs = require('fs');
var Memcached = require('memcached');
var memcached = new Memcached(process.env.MEMCACHED_HOST + ':' + process.env.MEMCACHED_PORT);

// Local lock to determine if we are removing from workers array
var removing = false;

/**
 * @Queue :
 * --------
 * Create a new Queue object, only one should be created per process, this queue
 * will reserve jobs with its processId so that each job can be distinguished
 * on the queue.
 *
 * @param {object} db - The mongo db object
 * @param {number} workers - The amount of workers to run on this process
 * @param {string} processId - The id of this process, if the server stops or dies
 * then all of this processes reserved jobs will be released from the queue.
 */

var Queue = function(db, workers, processId) {
    this.mongoconn = db; // If null, couldn't connect
    this.maxWorkers = workers || 40;
    this.processId = processId;
    this.workers = [];

    this.initializeWorkers();
};

/**
 * @initializeWorkers :
 * -------------------
 * Based on the amount of workers passed to the constructor, this method will create
 * each worker.  The queue will then maintain each workers state in memory and
 * schedule them new work until the process is terminated.
 */

Queue.prototype.initializeWorkers = function() {
    for(var i = 0; i < this.maxWorkers; i++) {
        this.getNextJob();
    }
};

/**
 * @getNextJob :
 * -------------
 * Attempts to schedule a new job on the queue, we use memcache here as a scheduled
 * lock between processes to ensure only one process can make a transaction with SQL at
 * a time.  The locks are synchronized to ensure that processes on different clusters
 * cannot access resources at the same time, which would result in a dead-lock.
 *
 * If a worker cannot attain the lock, it will keep polling memcached every 0.25s
 * until it can acquire the lock, only then does work begin on the worker.
 */

Queue.prototype.getNextJob = function() {
    // Check memcache and make sure we aren't running the job to clear dead replays
    if(this.workers.length < this.maxWorkers) {
        memcached.add('locked', true, 2, function(err) {
            if(err) {
                //console.log('[MEMCACHED ERR] '.red, err);
                setTimeout(function() {
                    this.getNextJob();
                }.bind(this), 10);
            } else {
                var conn = new Connection();
                //Logger.writeToConsole('[QUEUE] Fetching next item to run on queue...'.cyan);

                // Set the priority on the queue back to 0 once we start working it
                // Build a query string of all existing replays
                var whereClause = "";
                memcached.get('replays', function(err, data) {
                    if(err || typeof data === 'undefined' || data === null) {
                        console.log('replays property in memcached was empty, got error message: '.red, err);
                        whereClause = "";
                    } else {
                        console.log('got data: ', data);
                        var replayObj = JSON.parse(data);
                        //console.log('replay object is: ', replayObj);
                        var replays = "";
                        if(replayObj.length > 0) {
                            replayObj.forEach(function(replayId) {
                                replays += '"' + replayId + '",';
                            });
                            replays = replays.substr(0, replays.length - 1);
                            if(replays !== "") {
                                whereClause = 'AND replayId NOT IN (' + replays + ')';
                            }
                        }
                    }
                    // This happens, regardless
                    var selectQuery = 'SELECT * FROM queue WHERE completed = false AND reserved_by IS NULL AND scheduled <= NOW() ' + whereClause + ' ORDER BY priority DESC LIMIT 1';
                    //var selectQuery = 'SELECT * FROM queue WHERE completed=false AND reserved_by IS NULL AND scheduled <= NOW() ' + whereClause + ' LIMIT 1';
                    //console.log('Query is: '.yellow, selectQuery);
                    //var updateQuery = 'UPDATE queue SET reserved_at=NOW(), reserved_by="' + this.processId + '", priority=0';

                    conn.selectAndInsertToMemcached(selectQuery, function(replay) {
                        if(typeof replay === 'undefined' || replay === null) {
                            console.log('Couldnt find a replay, getting next job: '.red);
                            memcached.del('locked', function(err) {
                                setTimeout(function() {
                                    this.getNextJob();
                                }.bind(this), 10);
                            }.bind(this));
                        } else {
                            var existsInMemcached = false;
                            memcached.get('replays', function(err, data) {
                                if(err || typeof data === 'undefined' || data === null) {
                                    console.log('error getting replays from memcached, was either null or undefined. Got error: '.red, err);
                                    // Nothing in memcached, create it
                                    memcached.add('replays', JSON.stringify([replay.replayId]), 300, function(err) {
                                        if(err) {
                                            console.log('error when creating replays object in memcached:'.red, err);
                                            memcached.del('locked', function(err) {
                                                setTimeout(function() {
                                                    this.getNextJob();
                                                }.bind(this), 10);
                                            }.bind(this));
                                        } else {
                                            this.addReplayToMemcached(replay);
                                        }
                                    }.bind(this));
                                } else {
                                    var replays = JSON.parse(data);
                                    replays.forEach(function(replayId) {
                                        existsInMemcached = replayId == replay.replayId;
                                        return existsInMemcached;
                                    });
                                    if(existsInMemcached) {
                                        console.log('Already exists in memcached: ' + replay.replayId);
                                        memcached.del('locked', function(err) {
                                            setTimeout(function() {
                                                this.getNextJob();
                                            }.bind(this), 10);
                                        }.bind(this));
                                    } else {
                                        // Update the memcached object of replays
                                        replays.push(replay.replayId);
                                        memcached.replace('replays', JSON.stringify(replays), 300, function(err) {
                                            if(err) {
                                                console.log('couldnt replace in memcached: '.red, err);
                                                setTimeout(function() {
                                                    this.getNextJob();
                                                }.bind(this), 10);
                                            } else {
                                                this.addReplayToMemcached(replay);
                                            }
                                        }.bind(this));
                                    }
                                }
                            }.bind(this));
                        }
                    }.bind(this));
                }.bind(this));
            }
        }.bind(this));
    } else {
        //Logger.writeToConsole('[QUEUE] Not enough jobs running'.yellow);
        setTimeout(function() {
            this.getNextJob();
        }.bind(this), 10);
    }
};

/**
 * @addReplayToMemcached :
 * -----------------------
 * Adds the given replayId to memcached and then starts work on this task.  We also dispose
 * of the memcached lock here so another worker can begin to schedule work
 *
 * @param {object} replay - The replay object
 */

Queue.prototype.addReplayToMemcached = function(replay) {
    memcached.add(replay.replayId, true, 300, function(err) {
        if(err) {
            console.log('Replay: ' + replay.replayId + ' was already in memcached, getting next job'.red, err);
            memcached.del('locked', function(err) {
                setTimeout(function() {
                    this.getNextJob();
                }.bind(this), 10);
            }.bind(this));
        } else {
            memcached.del('locked', function(err) {
                if(err) {
                    console.log('Replay: ' + replay.replayId + ' failed to delete lock, getting next job'.red, err);
                    setTimeout(function() {
                        this.getNextJob();
                    }.bind(this), 10);
                } else {
                    this.runTask(new Replay(this.mongoconn, replay.replayId, replay.checkpointTime, replay.attempts, this));
                }
            }.bind(this));
        }
    }.bind(this));
};

/**
 * @runTask :
 * ----------
 * Checks if the replay already exists in the workers array stored in local process
 * memory, if it does we get get the next job for the queue, otherwise this method
 * begins running work for the given replay.
 *
 * This talks to the Replay object associated to the current worker and will process
 * the replay until it succeeds, fails or completes at which point it will return
 * to @getNextJob for new work
 *
 * @param {object} replay - The replay object to process
 */

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

/**
 * @workerDone :
 * -------------
 * Whenever a replay finishes running work we set a lock in local process memory
 * allowing us to atomically update the the workers array.
 *
 * If the worker gets the lock it splices the worker array of the current worker
 * and then releases the lock allowing another finished worker to dispose of its
 * resource.   If the workers fails to get a lock the promise is rejected and the
 * caller will retry in a defined time set by the caller.
 *
 * @param {object} replay - The replay we want to dispose of in local memory
 *
 * @return {promise} - Tells the caller if it should schedule a new reques to @workerDone
 * or if we successfully got the lock and removed the process.
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

/**
 * @updateMemcachedReplays :
 * -------------------------
 * Updates the replay parameter in memcached so another worker can pick up the replay we are currently working
 * on, this prevents replays from being reserved forever by one specific queue.
 *
 * @param {object} replay - the replay that needs to be removed.
 * @param {function} callback - tells caller when we're done here
 */

Queue.prototype.updateMemcachedReplays = function(replay, callback) {
    memcached.get('replays', function(err, data) {
        if(err || typeof data === 'undefined' || data === null) {
            Logger.writeToConsole('Error when getting, was either undefined or null, got memcached error:'.red, err);
            callback();
        } else {
            var replays = JSON.parse(data);
            var replayIndex = -1;
            replays.some(function(replayId, i) {
                if(replay.replayId === replayId) {
                    replayIndex = i;
                    return true;
                }
                return false;
            });
            replays.splice(replayIndex, 1);
            memcached.replace('replays', JSON.stringify(replays), 300, function(err) {
                if(err) {
                    Logger.writeToConsole('Error when updating memcached replays:'.red, err);
                    callback();
                } else {
                    Logger.writeToConsole('Successfully updated memcached replays.'.green);
                    callback();
                }
            });
        }
    });
};

/**
 * @failed :
 * ---------
 * While processing in @Replay.js the processing failed, this can be caused by no replay data being
 * available, expired replay, error in parser, a bot game being started and stopped.
 *
 * When this happens we increment the number of attempts on the specific replay object, delete its
 * current content from mongo and tell the queue to schedule it 2 minutes from now.
 *
 * The 2 minute reschedule creates enough time to solve problems related to no replay data being found,
 * once this task finishes it calls @getNextJob
 *
 * @param {object} replay - The replay which failed
 */

Queue.prototype.failed = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var scheduledDate = new Date(Date.now() + 120000);
        replay.replayJSON = null;
        var completed = replay.attempts > 10 ? 1 : 0;
        console.log('attempts were: ' + replay.attempts + ' completed is: ' + completed);
        var query = 'UPDATE queue SET reserved_by=null, completed =' + completed + ', checkpointTime = 0, attempts = attempts + 1, priority = 2, scheduled = DATE_ADD(NOW(), INTERVAL 2 MINUTE) WHERE replayId = "' + replay.replayId + '"';
        conn.query(query, function(row) {
            this.updateMemcachedReplays(replay, function() {
                memcached.del(replay.replayId, function() {
                    if(typeof row !== 'undefined' && row !== null && row.affectedRows !== 0) {
                        Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
                        this.deleteFile(replay);
                        this.getNextJob();
                    } else {
                        Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, but there was an error when updating it'.red);
                        this.getNextJob();
                    }
                }.bind(this));
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        this.failed(replay);
    }.bind(this));

};

/**
 * @schedule :
 * -----------
 * When @Replay finishes processing chunks for a live replay, scheduled will be called to re-serve
 * this specific replay after 1 minute.
 *
 * Upon finishing this method calls @getNextJob and @workerDone.
 *
 * @param {object} replay - The Replay to reschedule
 * @param {number} ms - The amount of ms to delay by
 */

Queue.prototype.schedule = function(replay, ms) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var scheduledDate = new Date(Date.now() + ms);
        Logger.writeToConsole('[QUEUE] Scheduled to run: '.blue + replay.replayId + ' at: '.blue, scheduledDate);
        var query = 'UPDATE queue SET reserved_by=null, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            this.updateMemcachedReplays(replay, function() {
                this.uploadFile(replay, function() {
                    memcached.del(replay.replayId, function() {
                        this.getNextJob();
                    }.bind(this));
                }.bind(this));
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        this.schedule(replay, ms);
    }.bind(this));

};

/**
 * @removeItemFromQueue :
 * ----------------------
 * When a replay has finished processing in @Replay.js, this method will be called.  If runs
 * an upsert query against mongo and updates the queue to tell it that it does not need to
 * be serviced again, preventing any other workers from running it in future.
 *
 * This method will only run if it can attain the local removing lock, if it can't it will
 * wait until the lock is released before it updates SQL and Mongo, completion will result
 * in @getNextJob being called.
 *
 * @param {object} replay - The replay that finished processing
 */

Queue.prototype.removeItemFromQueue = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        this.uploadFile(replay, function(err) {
            if(err === null) {
                Logger.writeToConsole('[QUEUE] Replay '.green + replay.replayId + ' finished processing and uploaded to mongo successfully '.green + '✓');
                var query = 'UPDATE queue SET reserved_by=null, priority=0, completed=true, completed_at=NOW(), live=0, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
                conn.query(query, function() {
                    this.updateMemcachedReplays(replay, function() {
                        memcached.del(replay.replayId, function() {});
                    }.bind(this));
                }.bind(this));
                this.getNextJob();
            } else {
                memcached.del(replay.replayId, function() {});
                Logger.writeToConsole('[QUEUE] There was an error when uploading file (this is callback from remove item from queue): '.red + err.message);
                replay.replayJSON = Replay.getEmptyReplayObject();
                this.failed(replay);
            }
        }.bind(this));
    }.bind(this), function() {
        this.removeItemFromQueue(replay);
    }.bind(this));

};

/**
 * @removeDeadReplay :
 * -------------------
 * A dead replay occurs in @Replay.js when the http code returned from the API is a 404.  In the
 * event of this message we know that the replay has expired on epics end we will not be able to
 * process it in future.
 *
 * Therefore we remove it from the queue so we don't waste workers trying to serve it.  Once we
 * complete this task, this method calls @getNextJob
 *
 * @param {object} replay - The replay which doesn't exist in epics API anymore
 */

Queue.prototype.removeDeadReplay = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            this.updateMemcachedReplays(replay, function() {
                memcached.del(replay.replayId, function() {
                    Logger.writeToConsole('[QUEUE] Replay '.red + replay.replayId + ' had either already been processed by another queue, or was a dead replay and reported no checkpoints in 6 minutes, removing from queue.'.red);
                    this.deleteFile(replay);
                    this.getNextJob();
                }.bind(this));
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        this.removeDeadReplay(replay);
    }.bind(this));

};

/**
 * @removeBotGame :
 * ----------------
 * Processing bot games is un-necessary as players cannot gain ELO and Epic do not track bot stats on their API,
 * because of this we do not process bot games and simply remove them from the queue once @Replay.js
 * determines it is a bot game.
 *
 * Once we remove the bot game this method calls @getNextJob
 *
 * @param {object} replay - The bot game replay
 */

Queue.prototype.removeBotGame = function(replay) {
    this.workerDone(replay).then(function() {
        removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function() {
            this.updateMemcachedReplays(replay, function() {
                memcached.del(replay.replayId, function() {
                    Logger.writeToConsole('[QUEUE] Replay removed as it is a bot game for: '.yellow + replay.replayId);
                    this.getNextJob();
                }.bind(this));
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        this.removeBotGame(replay);
    }.bind(this));

};

/**
 * @uploadFile :
 * -------------
 * Uploads a processed replayJSON chunk to mongo, this will either be a completed replay
 * or a partially processed chunk from a scheduled job sent by @scheduled
 *
 * @param {object} replay - The replay which should be uploaded (contains replayJSON as a property)
 * @param {function} callback - Callback function when the upload completes, contains an error
 * message if it fails, else returns null.
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

/**
 * @deleteFile :
 * -------------
 * Removes the given replay from mongo, this happens when a replay fails or expires.
 * We do not pass a callback here as the point of which mongo deletes the record
 * is not important to the continued execution of the process.
 *
 * @param {object} replay - The replay to delete
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

/**
 * @disposeOfLockedReservedEvents :
 * --------------------------------
 * When a process dies we need some way of disposing of any resources that it had reserved.  We handle this
 * by listening to the process.SIGNIT event in @parser.js.  If the process dies we update SQL by
 * removing all instances of resources owned by this process allowing other process workers to start
 * running work on them.
 *
 * We also set its completed status back to false and its checkpoint time to 0 to prevent half processed
 * replays from being uploaded.
 *
 * @param {object} processId - The process id to be disposed of, this consists of the process PID and a
 * random 8 digit alphanumeric string so we don't get collisions across boxes.
 * @param {function} callback - Callback to determine when the query has ran, allowing the memcached lock to
 * be released allowing other workers to start transacting with MySQL again.
 */

Queue.disposeOfLockedReservedEvents = function(processId, callback) {
    Logger.writeToConsole('disposing of locked events');
    var conn = new Connection();
    var query = 'UPDATE queue SET reserved_by=null, checkpointTime=0, completed=0 WHERE reserved_by="' + processId + '"';
    conn.query(query, function() {
        Logger.writeToConsole('disposed');
        callback();
    });
};

module.exports = Queue;
