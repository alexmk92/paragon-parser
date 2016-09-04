var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');

var net = require('net');
require('colors');

/**
 * @QueueClient :
 * --------------
 * Create a new QueueClient, only one is created per process.  QueueClient
 * is responsible for talking to QueueManager which reserves resources
 * for a specific process, only one QueueClient can attain the lock
 * on QueueManager at any one time meaning that all Updates on SQL are
 * handled atomically.
 *
 * @param {object} db - The mongo db object
 * @param {number} workers - The amount of workers to run on this process
 * @param {string} processId - The id of this process, if the server stops or dies
 * then all of this processes reserved jobs will be released from the queue by
 * QueueManager
 */

var Queue = function(db, workers, processId) {
    this.mongoconn = db; // If null, couldn't connect
    this.maxWorkers = workers || 40;
    this.processId = processId;
    this.workers = [];
    this.queue = [];
    this.socket = null;

    this.fetchingReplays = false; // Only one process can fetch replays
    this.initialized = false; // Tells us if we have already initted the queue
    this.terminating = false; // Local variable to determine if we are stopping this process
    this.removing = false; // Local lock to determine if we are removing from workers array
    this.waiting = false; // Bool to throttle the amount of requests we make to QueueManager preventing EPIPE network error

    this.createSocket(); // Creates the socket and then starts the initializeWorkers call on a successful connect
};

/**
 * @createSocket :
 * ---------------
 * Creates a new TCP socket allowing us to query QueueManager, once
 * we connect we write a connectWithProcessId event to QueueManager which
 * will then start the handshake for negotiating a lock with the server
 * and once we get the lock we can start processing replays.
 */

Queue.prototype.createSocket = function() {
    this.socket = new net.Socket();
    this.socket.processId = this.processId;
    this.bindSocketListeners();
    this.socket.connect(process.env.QUEUE_MANAGER_PORT, process.env.QUEUE_MANAGER_HOST, function() {
        this.socket.write(JSON.stringify({ action: 'connectWithProcessId', processId: this.processId }));
    }.bind(this));
};

/**
 * @bindSocketListeners :
 * ----------------------
 * Once we have created a socket, all listeners on the data method of the
 * socket should have listeners bound to them, this method maps all
 * inbound action types from QueueManager to be mapped to methods on
 * QueueClient.
 *
 * All messages sent back from QueueManager are stringified, we parse
 * them in this message and then perform any mappings.
 *
 * If the socket connection listener does not respond with a result of
 * 200, we re-assign the socket as a message other than 200 indicates
 * another process has the same ID (this collision should not happen
 * but its best to handle it on the edge case!)
 */

Queue.prototype.bindSocketListeners = function() {
    if(this.socket !== null) {
        this.socket.on('data', function(payload) {
            try {
                var data = JSON.parse(payload);
                switch(data.action) {
                    case 'connect':
                        if(data.code === 200)  {
                            this.socket.write(JSON.stringify({ action: 'getReplays' }));
                        } else {
                            this.processId = (this.processId + Math.floor((Math.random() * 999) + 1));
                            this.createSocket();
                        }
                        break;
                    case 'replays':
                        if(data.code === 200) {
                            this.getReservedReplays();
                        } else if(data.code === 409) {
                            Logger.writeToConsole(data.message.red);
                            setTimeout(function() {
                                Logger.writeToConsole('[QUEUE] Attempting to get lock on Queue Manager'.yellow);
                                this.socket.write(JSON.stringify({ action: 'getReplays' }));
                            }.bind(this), process.env.QUEUE_CLIENT_WAIT_DELAY_MS);
                        }
                        break;
                    case 'disconnect':
                        Logger.writeToConsole(data.message.yellow);
                        break;
                }
            } catch(e) {
                Logger.writeToConsole('ERROR: When parsing: ', payload);
            }
        }.bind(this));
    } else {
        this.createSocket();
    }
};

/**
 * @getReservedReplays :
 * ---------------------
 * After QueueManager reserves a block of replays for us, we can query them from SQL
 * in this method and then add them to memory.  We will not get deadlocks as
 * the resources have already been updated in SQL and the read is not expensive.
 */

Queue.prototype.getReservedReplays = function() {
    if(!this.fetchingReplays) {
        this.fetchingReplays = true;
        Logger.writeToConsole('getting replays from sql for: '.cyan + this.processId);
        var conn = new Connection();
        var selectQuery = 'SELECT * FROM queue WHERE reserved_by="' + this.processId + '"';
        conn.query(selectQuery, function(rows) {
            if(typeof rows !== 'undefined' && rows && rows.length > 0) {
                rows.forEach(function(replay) {
                    var found = false;
                    this.queue.some(function(replayObj) {
                        found = replay.replayId == replayObj.replayId;
                        return found;
                    });
                    if(!found) { this.queue.push({ replayId: replay.replayId, checkpointTime: replay.checkpointTime, attempts: replay.attempts }); }
                }.bind(this));
                this.fetchingReplays = false;
                this.initializeWorkers();
            } else {
                // wait for the queue to catch up
                setTimeout(function() {
                    this.fetchingReplays = false;
                    Logger.writeToConsole('There were no reserved jobs for: '.yellow + this.processId + ' waiting '.yellow + (process.env.QUEUE_CLIENT_WAIT_DELAY_MS/1000) + ' seconds before trying again'.yellow);
                    if(this.socket !== null) {
                        this.socket.write(JSON.stringify({ action: 'getReplays' }));
                    } else {
                        this.createSocket();
                    }
                }.bind(this), process.env.QUEUE_CLIENT_WAIT_DELAY_MS);
            }
        }.bind(this));
    }

};

/**
 * @terminate :
 * ------------
 * Sent from the queue owner, tells the queue that it is shutting down preventing
 * the workers array to be altered
 */

Queue.prototype.terminate = function() {
    this.socket.destroy();
    this.terminating = true;
};

/**
 * @initializeWorkers :
 * -------------------
 * Based on the amount of workers passed to the constructor, this method will create
 * each worker.  The queue will then maintain each workers state in memory and
 * schedule them new work until the process is terminated.
 */

Queue.prototype.initializeWorkers = function() {
    // Schedule all workers, otherwise schedule ones that need work
    if(!this.initialized) {
        this.initialized = true;
        for(var i = 0; i < this.maxWorkers; i++) {
            this.getNextJob();
        }
    } else {
        var diff = this.maxWorkers - this.workers.length;
        for(var i = 0; i < diff; i++) {
            this.getNextJob();
        }
    }
};

/**
 * @getNextJob :
 * -------------
 * Gets the next job from the queue stored in local memory which is populated from QueueManager,
 * we keep shifting from this local memory array until there are no items left.  At this point
 * we send a request to QueueManager for the next process.env.REPLAY_FETCH_AMOUNT replays.
 *
 * We wait process.env.QUEUE_CLIENT_WAIT_DELAY_MS (ms) before sending this request and
 * set our local waiting variable to true so that we don't over fill the TCP stream
 * buffer and get an EPIPE error on the client.
 *
 * If for whatever reason out socket has died, we will also create a new socket in
 * this method.
 */

Queue.prototype.getNextJob = function() {
    if(this.queue.length > 0 && this.workers.length < this.maxWorkers) {
        var job = this.queue.shift();
        this.runTask(new Replay(this.mongoconn, job.replayId, job.checkpointTime, job.attempts, this));
    } else {
        if(this.socket !== null) {
            if(!this.waiting) {
                this.waiting = true;
                // When these timeouts expire, check if we can just get the next job
                setTimeout(function() {
                    this.waiting = false;
                    if(this.queue.length > 0) {
                        this.getNextJob();
                    } else {
                        this.socket.write(JSON.stringify({ action: 'getReplays' }));
                    }
                }.bind(this), process.env.QUEUE_CLIENT_WAIT_DELAY_MS);
            }
        } else {
            this.createSocket();
        }
    }
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
        //Logger.writeToConsole('[QUEUE] Another worker on this box is already running work for replay: '.yellow + replay.replayId + '. Fetching new job.'.yellow);
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
        if(!this.removing && !this.terminating) {
            this.removing = true;
            var index = -1;
            this.workers.some(function(workerId, i) {
                if(workerId === replay.replayId) {
                    index = i;
                    return true;
                }
                return false;
            });
            if(index > -1 && !this.terminating) {
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
        this.removing = false;

        var conn = new Connection();
        replay.replayJSON = null;
        replay.deleting = true;
        var completed = replay.attempts > 10 ? 1 : 0;
        var query = 'UPDATE queue SET reserved_by=null, completed =' + completed + ', checkpointTime = 0, attempts = attempts + 1, priority=1, scheduled = DATE_ADD(NOW(), INTERVAL 2 MINUTE) WHERE replayId = "' + replay.replayId + '"';
        conn.query(query, function(row) {
            if(row === null && replay.queryAttempts < 99999999999) {
                Logger.writeToConsole('Attempt: '.yellow + replay.queryAttempts + '/99999999999 Retrying query for replay: '.yellow + replay.replayId + ' in 1s'.yellow);
                setTimeout(function() {
                    replay.queryAttempts++;
                    this.failed(replay);
                }.bind(this), 1000);
            } else if(typeof row !== 'undefined' && row !== null && row.affectedRows !== 0) {
                Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
                this.deleteFile(replay, function() {});
                this.getNextJob();
            } else {
                Logger.writeToConsole('[QUEUE] Replay: '.red + replay.replayId + ' failed to process, but there was an error when updating it'.red);
                this.getNextJob();
            }
        }.bind(this));
    }.bind(this), function() {
        setTimeout(function() {
            this.failed(replay);
        }.bind(this), 1000);
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
 * @param {number} seconds - The amount of seconds to delay by
 */

Queue.prototype.schedule = function(replay, seconds) {
    this.workerDone(replay).then(function() {
        this.removing = false;

        var conn = new Connection();
        var scheduledDate = new Date(Date.now() + 60000);
        Logger.writeToConsole('[QUEUE] Scheduled to run: '.blue + replay.replayId + ' at: '.blue, scheduledDate);
        var query = 'UPDATE queue SET reserved_by=null, scheduled = DATE_ADD(NOW(), INTERVAL ' + seconds + ' SECOND), priority=2, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function(rows) {
            if(rows === null && replay.queryAttempts < 99999999999) {
                Logger.writeToConsole('Attempt: '.yellow + replay.queryAttempts + '/99999999999 Retrying query for replay: '.yellow + replay.replayId + ' in 1s'.yellow);
                setTimeout(function() {
                    replay.queryAttempts++;
                    this.schedule(replay, ms);
                }.bind(this), 1000);
            } else {
                this.uploadFile(replay, function() {
                    this.getNextJob();
                }.bind(this));
            }
        }.bind(this));
    }.bind(this), function() {
        setTimeout(function() {
            this.schedule(replay);
        }.bind(this), 1000);
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
        this.removing = false;

        var conn = new Connection();
        delete replay.replayJSON.endChunksParsed;
        delete replay.replayJSON.scheduledBeforeEnd;

        this.deleteFile(replay, function(err) {
            this.uploadFile(replay, function(err) {
                if(err === null) {
                    Logger.writeToConsole('[QUEUE] Replay '.green + replay.replayId + ' finished processing and uploaded to mongo successfully '.green + '✓');
                    var query = 'UPDATE queue SET reserved_by=null, priority=0, completed=true, completed_at=NOW(), live=0, checkpointTime=' + replay.replayJSON.latestCheckpointTime + ' WHERE replayId="' + replay.replayId + '"';
                    conn.query(query, function(rows) {
                        if(rows === null && replay.queryAttempts < 99999999999) {
                            Logger.writeToConsole('Attempt: '.yellow + replay.queryAttempts + '/99999999999 Retrying query for replay: '.yellow + replay.replayId + ' in 1s'.yellow);
                            setTimeout(function() {
                                replay.queryAttempts++;
                                this.removeItemFromQueue(replay);
                            }.bind(this), 1000);
                        } else {
                            this.getNextJob();
                        }
                    }.bind(this));
                } else {
                    Logger.writeToConsole('[QUEUE] There was an error when uploading file (this is callback from remove item from queue): '.red + err.message);
                    replay.replayJSON = Replay.getEmptyReplayObject();
                    this.failed(replay);
                }
            }.bind(this));
        }.bind(this));
    }.bind(this), function() {
        setTimeout(function() {
            this.removeItemFromQueue(replay);
        }.bind(this), 1000);
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
        this.removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        replay.deleting = true;
        conn.query(query, function(rows) {
            if(rows === null && replay.queryAttempts < 99999999999) {
                Logger.writeToConsole('Attempt: '.yellow + replay.queryAttempts + '/99999999999 Retrying query for replay: '.yellow + replay.replayId + ' in 1s'.yellow);
                setTimeout(function() {
                    replay.queryAttempts++;
                    this.removeDeadReplay(replay);
                }.bind(this), 1000);
            } else {
                Logger.writeToConsole('[QUEUE] Replay '.red + replay.replayId + ' had either already been processed by another queue, or was a dead replay and reported no checkpoints in 6 minutes, removing from queue.'.red);
                this.deleteFile(replay, function() {});
                this.getNextJob();
            }
        }.bind(this));
    }.bind(this), function() {
        setTimeout(function() {
            this.removeDeadReplay(replay);
        }.bind(this), 1000);
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
        this.removing = false;

        var conn = new Connection();
        var query = 'UPDATE queue SET reserved_by=null, completed=true, completed_at=NOW() WHERE replayId="' + replay.replayId + '"';
        conn.query(query, function(rows) {
            if(rows === null && replay.queryAttempts < 99999999999) {
                Logger.writeToConsole('Attempt: '.yellow + replay.queryAttempts + '/99999999999 Retrying query for replay: '.yellow + replay.replayId + ' in 1s'.yellow);
                setTimeout(function() {
                    replay.queryAttempts++;
                    this.removeBotGame(replay);
                }.bind(this), 1000);
            } else {
                Logger.writeToConsole('[QUEUE] Replay removed as it is a bot game for: '.yellow + replay.replayId);
                this.getNextJob();
            }
        }.bind(this));
    }.bind(this), function() {
        setTimeout(function() {
            this.removeBotGame(replay);
        }.bind(this), 1000);
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
                        return callback({ message: 'failed to upload file'});
                    } else {
                        Logger.writeToConsole('[QUEUE] Replay file: '.green + replay.replayId + ' uploaded to mongo!'.green);
                        return callback(null);
                    }
                }.bind(this));
        } catch(e) {
            this.failed(replay);
            Logger.writeToConsole('[MONGO ERROR] in OldQueue.js when uploading relay: '.red + replay.replayId + '.  Error: '.red, e);
            return callback({ message: 'failed to upload' });
        }
    } else {
        this.failed(replay);
        return callback({ message: 'replay JSON was null'})
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
 * @param {function} callback - Determines when this is done
 */

Queue.prototype.deleteFile = function(replay, callback) {
    try {
        if(!replay.deleting) {
            replay.deleting = true;
            // get a lock on this specific item
            this.mongoconn.collection('matches').deleteOne(
                { replayId: replay.replayId },
                function(err, results) {
                    if(err) {
                        Logger.writeToConsole('[QUEUE] Failed to remove replay file: '.red + replay.replayId + ' from mongo'.red);
                    } else {
                        Logger.writeToConsole('[QUEUE] Replay: '.yellow + replay.replayId + ' was deleted from mongo successfully.'.yellow);
                    }
                    return callback();
                }.bind(this));
        } else {
            Logger.writeToConsole('[QUEUE] Already deleting replay: '.yellow + replay.replayId + ' from mongo'.yellow);
            return callback();
        }
    } catch(e) {
        replay.deleting = false;
        Logger.writeToConsole('[MONGO ERROR] in OldQueue.js when uploading replay: '.red + replay.replayId + '.  Error: '.red, e);
        return callback();
    }
};

module.exports = Queue;
