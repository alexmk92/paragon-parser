var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var assert = require('assert');
var fs = require('fs');

var conn = new Connection();
var LOG_FILE = './logs/log.txt';

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function(db, workers) {
    console.log("[QUEUE] Queue manager started".cyan);
    this.mongoconn = db; // If null, couldn't connect
    this.queue = [];

    this.maxWorkers = workers || 2;
    this.isInitializingWorkers = false;
    //this.hasStarted = false;

    this.workers = [];

    setInterval(function() {
        //console.log('checking if: ' + this.workers.length + ' is less than ' + this.maxWorkers);
        if(this.workers.length < this.maxWorkers) {
            this.initializeWorkers().then(function() {
                this.runTasks();
            }.bind(this), function(e) {
                process.exit(); // restart on an error
            });
        } else {
            this.runTasks();
        }

        if(this.isEmptyOrReserved()) {
            console.log('[QUEUE] No jobs for queue, refilling buffer as it was either fully reserved, empty or scheduled'.cyan);
            this.fillBuffer(true);
        }
    }.bind(this), 2500);

    setInterval(function() {
        this.disposeOfLockedReservedEvents();
    }.bind(this), 180000);

};

/*
 * Loops through the queue and checks whether or not we need to refresh it
 */

Queue.prototype.isEmptyOrReserved = function() {
    if(this.queue.length <= this.maxWorkers) {
        return true;
    } else {
        var scheduledCount = 0;
        var reservedCount = 0;
        this.queue.forEach(function(item) {
            if(item.scheduledTime.getTime() > new Date().getTime()) {
                scheduledCount++;
            }
            if(item.isReserved) reservedCount++;
        });
        if(scheduledCount >= this.queue.length || reservedCount >= this.queue.length) {
            return true;
        }
    }
    return false;
};

/*
 * Unbinds all locked events from the Queue
 */

Queue.prototype.disposeOfLockedReservedEvents = function() {
    var query = 'UPDATE queue SET reserved = false WHERE TIMEDIFF(reserved_at, NOW()) / 60 > 3';
    conn.query(query, function() {});
};

/*
 * It's possible that no data happens in a solo ai or custom game if people
 * just leave to combat this we just remove it from the queue forever
 */

Queue.prototype.removeDeadReplay = function(item) {
    Logger.append('./logs/failedReplays.txt', 'Replay: ' + item.replayId + ' was either empty or had been processed before and has been removed from the queue');
    console.log('[QUEUE] Replay '.red + item.replayId + ' had either already been processed by another queue, or was a dead replay and reported no checkpoints in 6 minutes, removing from queue.'.red);
    var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
    item.isRunningOnQueue = false;
    item.isReserved = false;
    conn.query(query, function() {
        query = 'DELETE FROM queue WHERE replayId="' + item.replayId + '"';
        conn.query(query, function() {});
    }.bind(this));
    this.workers.some(function(worker, i) {
        if(worker.replayId === item.replayId) {
            this.workers.splice(i, 1);
            return true;
        }
    }.bind(this));
    this.queue.some(function(queueItem, i) {
        if(queueItem.replayId === item.replayId) {
            this.queue.splice(i, 1);
            return true;
        }
        return false;
    }.bind(this));
};

/*
 * Removes a specific item from the queue, set its status to completed,
 * set its reserved to 0,
 */

Queue.prototype.removeItemFromQueue = function(item) {
    this.uploadFile(item, function(err) {
        if(err === null) {
            console.log('[QUEUE] Replay '.green + item.replayId + ' finished processing'.green);
            var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
            item.isRunningOnQueue = false;
            item.isReserved = false;
            conn.query(query, function() {
                query = 'DELETE FROM queue WHERE replayId="' + item.replayId + '"';
                conn.query(query, function() {});
            }.bind(this));
            this.queue.some(function(queueItem, i) {
                if(queueItem.replayId === item.replayId) {
                    this.queue.splice(i, 1);
                    return true;
                }
                return false;
            }.bind(this));
        } else {
            console.log('[QUEUE] There was an error when uploading file (this is callback from remove item from queue): '.red + err.message);
            // Make sure that this item is reset, we cannot process it like this
            item.isUploading = false;
            item.isReserved = false;
            item.replayJSON = Replay.getEmptyReplayObject();
            this.failed(item);
        }
    }.bind(this));
};

/*
 * Uploads the file and disposes of the current worker
 */

Queue.prototype.uploadFile = function(item, callback) {
    try {
        // get a lock on this specific item
        if(!item.isUploading) {
            item.isUploading = true;
            this.mongoconn.collection('matches').update(
                { replayId: item.replayJSON.replayId },
                { $set: item.replayJSON },
                { upsert: true},
                function(err, results) {
                    item.isUploading = false;
                    if(err) {
                        callback({ message: 'failed to upload file'});
                    } else {
                        this.workers.some(function(worker, i) {
                            if(worker.replayId === item.replayId) {
                                worker.isReserved = false;
                                this.workers.splice(i, 1);
                                console.log('[QUEUE] Replay: '.yellow + worker.replayId + ' uploaded, the worker at: '.yellow + i + ' has been disposed'.yellow);
                            }
                        }.bind(this));
                    }
            }.bind(this));
        }
        callback(null);
    } catch(e) {
        console.log('[MONGO ERROR] in Queue.js when uploading relay: '.red + item.replayId + '.  Error: '.red, e);
        callback({ message: 'failed to upload' });
        //Logger.append('./logs/mongoError.txt', 'Mongo error: ' + JSON.stringify(e));
    }
};

/*
 * Tells the queue manager something failed, we retry after 3 seconds
 */

Queue.prototype.failed = function(item) {
    // only run this once!
    if(!item.failed) {
        item.failed = true;
        console.log('[QUEUE] Replay: '.red + item.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
        var scheduledDate = new Date(Date.now() + 120000); // 120000
        item.scheduledTime = scheduledDate;
        item.isRunningOnQueue = false;
        item.isReserved = false;
        item.replayJSON = null;
        item.isScheduledInQueue = true;
        Logger.append(LOG_FILE, new Date() + ' The replay with id: ' + item.replayId + ' failed, its scheduled to re-run at ' + scheduledDate);
        var query = 'UPDATE queue SET attempts = attempts + 1, priority = 2, scheduled = DATE_ADD(NOW(), INTERVAL 2 MINUTE), reserved = false WHERE replayId = "' + item.replayId + '"';
        //Logger.append('./logs/log.txt', 'Replay: ' + item.replayId + ' is now scheduled to run at ' + scheduledDate);
        conn.query(query, function(row) {
            if(row.affectedRows !== 0) {
                query = "UPDATE replays SET completed = false, status = 'UNSET', checkpointTime = 0 WHERE replayId = '" + item.replayId + "'";
                conn.query(query, function(row) {
                    //Logger.append('./logs/log.txt', item.replayId + ' has had its reverted status set back to false.');
                });
                //Logger.append('./logs/log.txt', new Date() + ' Item ' + item.replayId + ' failed, incremented its failed attempts and processing it later');
            } else {
                //Logger.append('./logs/log.txt', new Date() + ' Failed to update the failure attempt for ' + item.replayId + '. Used query: ' + query + ', returned:' + JSON.stringify(row));
            }
        });
        this.workers.some(function(worker, i) {
            if(worker.replayId === item.replayId) {
                worker.isReserved = false;
                console.log('[QUEUE] Item failed, worker at index: '.yellow + i + ' has been disposed'.yellow);
                this.workers.splice(i, 1);
                return true;
            }
            return false;
        }.bind(this));
        this.initializeWorkers().then(function() {
            this.runTasks();
        }.bind(this));
    }
};

/*
 * Tells the queue to schedule the task x seconds in the future
 */

Queue.prototype.schedule = function(item, ms) {
    var scheduledDate = new Date(Date.now() + ms);
    item.isRunningOnQueue = false;
    item.isReserved = false;
    item.scheduledTime = scheduledDate;
    item.isScheduledInQueue = true;
    console.log('[QUEUE] Scheduled to run: '.blue + item.replayId + ' at: '.blue, scheduledDate);
    var query = 'UPDATE queue SET reserved = false, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3 WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        // Allow another worker to pick this up and process the full item as they will not know where to start processing from
        query = "UPDATE replays SET checkpointTime = 0 WHERE replayId = '" + item.replayId + "'";
        conn.query(query, function() {});
        this.uploadFile(item, function() {});
    }.bind(this));
};

/*
 * Fills the Queue buffer with data from MySQL, it will select all records that
 * have not been completed and that are not reserved.
 */

Queue.prototype.fillBuffer = function(forceFill) {
    // First try to get data from the queue
    var queueQuery = 'SELECT queue.replayId, queue.attempts, replays.checkpointTime, queue.scheduled, queue.priority ' +
                     ' FROM queue' +
                     ' JOIN replays ON replays.replayId = queue.replayId' +
                     ' WHERE queue.completed = false AND queue.attempts < 100 ORDER BY priority DESC';

    conn.query(queueQuery, function(results) {
        if(results.length === 0 || forceFill) {
            // Attempt to put new data into the queue
            var query = 'SELECT replays.replayId, replays.checkpointTime, queue.attempts ' +
                ' FROM replays ' +
                ' LEFT JOIN queue ON replays.replayId = queue.replayId ' +
                ' WHERE replays.completed = false ' +
                ' LIMIT 500';

            var replaysInQueue = '';
            this.queue.forEach(function(replay) {
                replaysInQueue += '"' + replay.replayId + '", ';
            });
            if(replaysInQueue.length > 0) {
                replaysInQueue = replaysInQueue.substr(0, replaysInQueue.length -2);
                query = 'SELECT replays.replayId, replays.checkpointTime, queue.attempts ' +
                    ' FROM replays ' +
                    ' LEFT JOIN queue ON replays.replayId = queue.replayId ' +
                    ' WHERE replays.completed = false ' +
                    ' AND replays.replayId NOT IN(' + replaysInQueue + ')' +
                    ' LIMIT 500';
            }

            //console.log('query was: ', query);

            conn.query(query, function(results) {
                if(results.length > 0) {
                    results.forEach(function(result) {
                        var replay = new Replay(this.mongoconn, result.replayId, result.checkpointTime, this);
                        replay.isScheduledInQueue = false;
                        replay.attempts = result.attempts;
                        replay.isReserved = false;
                        replay.isUploading = false;
                        replay.priority = result.priority;
                        replay.scheduledTime = new Date();
                        var found = false;
                        this.queue.some(function(queueItem) {
                            found = queueItem.replayId === result.replayId;
                            return found;
                        });
                        if(!found) {
                            this.queue.push(replay);
                            if(replay.scheduledTime > new Date()) {
                                replay.isScheduledInQueue = true;
                            }
                        }

                        var query = 'INSERT IGNORE INTO queue (replayId) VALUES("' + replay.replayId + '")';
                        conn.query(query, function(row) {});
                    }.bind(this));
                } else {
                    /*
                    console.log('[QUEUE] no jobs for queue, attempting to fill buffer...'.cyan);
                    setTimeout(function() {
                        this.fillBuffer();
                    }.bind(this), 5000);
                    */
                }
            }.bind(this));
        } else {
            setTimeout(function() {
                console.log('[QUEUE] Loading '.green + results.length + ' items onto the queue!'.green);
                //this.queue = [];
                results.forEach(function(result) {
                    var replay = new Replay(this.mongoconn, result.replayId, result.checkpointTime, this);
                    replay.isScheduledInQueue = false;
                    replay.attempts = result.attempts;
                    replay.isReserved = false;
                    replay.isUploading = false;
                    replay.priority = result.priority;
                    replay.scheduledTime = new Date(result.scheduled);
                    var found = false;
                    this.queue.some(function(queueItem) {
                        found = queueItem.replayId === result.replayId;
                        return found;
                    });
                    if(!found) {
                        this.queue.push(replay);
                        if(replay.scheduledTime > new Date()) {
                            replay.isScheduledInQueue = true;
                        }
                    } else {
                        //console.log('exists in queue');
                    }
                }.bind(this));
                //Logger.append('./logs/queueStatus.txt', 'Queue length is: ' + this.queue.length);
                //this.start();
            }.bind(this), 1250);
        }
    }.bind(this));
};

/*
 * Starts running the queue
 */

// Queue.prototype.start = function() {
//     if(!this.hasStarted) {
//         this.hasStarted = true;
//         this.initializeWorkers().then(function() {
//             this.runTasks();
//         }.bind(this));
//     }
// };

/*
 * Runs all tasks on current workers
 */

Queue.prototype.runTasks = function() {
    if(this.workers.length > 0) {
        //console.log('calling run workers');
        this.workers.forEach(function(worker) {
            if(!worker.isRunningOnQueue) {
                this.reserve(worker.replayId).then(function() {
                    if(new Date().getTime() > worker.scheduledTime.getTime()) {
                        console.log('[QUEUE] Queue now owns this resource, reserved and running work for: '.green + worker.replayId);
                        worker.isReserved = true;
                        worker.parseDataAtCheckpoint();
                    } else {
                        //Logger.append('./logs/log.txt', 'This job is scheduled to happen at ' + worker.scheduledTime + ' spawning a new worker for now.');
                        var query = 'UPDATE queue SET reserved = false WHERE replayId = "' + worker.replayId + '"';
                        conn.query(query, function() {
                            this.workers.some(function(currentWorker, i) {
                                if(worker.replayId === currentWorker.replayId) {
                                    this.workers.splice(i, 1);
                                    console.log(('[QUEUE] Tried to run a scheduled item, worker: ' + i + ' has been disposed').blue);
                                    return true;
                                }
                                return false;
                            }.bind(this));
                            Logger.append('./logs/log.txt', 'Removed this worker from the queue, spinning up a new worker');
                             this.initializeWorkers().then(function() {
                                this.runTasks();
                             }.bind(this));
                        }.bind(this));
                    }
                }.bind(this), function() {
                    this.workers.some(function(currentWorker, i) {
                        if(worker.replayId === currentWorker.replayId) {
                            console.log('[QUEUE] Unknown error when running worker, worker at index: '.yellow + i + ' has been disposed'.yellow);
                            this.workers.splice(i, 1);
                            return true;
                        }
                        return false;
                    }.bind(this));
                }.bind(this));
            }
        }.bind(this));
    }
};

/*
 * Reserves an item
 */

Queue.prototype.reserve = function(replayId) {
    return new Promise(function(resolve, reject) {
        // If someone reserves before we do this, then we can bail out
        var existsQuery = 'SELECT replayId FROM queue WHERE replayId = "' + replayId + '"';
        conn.query(existsQuery, function(results) {
             if(results.length > 0) {
                 var reserved_query = 'UPDATE queue SET reserved = true WHERE replayId = "' + replayId + '" AND reserved = false AND completed = false';
                 conn.query(reserved_query, function(row) {
                     if(this.ownsReservedId(replayId)) {
                         resolve();
                     } else if(row.changedRows === 0) {
                         ////Logger.append('./logs/log.txt', replayId + ' is reserved by another process.');
                         reject('reserved by another process as rows were 0');
                     } else {
                         ////Logger.append('./logs/log.txt', replayId + ' is reserved by another process.');
                         reject('reserved by another process');
                     }
                 }.bind(this));
             }
        }.bind(this));
    }.bind(this));
};

Queue.prototype.ownsReservedId = function(replayId) {
    var found = false;
    this.workers.some(function(worker) {
         if(worker.replayId === replayId) {
             found = true;
         }
        return found;
    });
    return found;
};

/*
 * Stops the queue and releases any assets it is locking
 */

Queue.prototype.stop = function(cb) {
    if(this.workers.length > 0) {
        var reservedItems = '';
        var len = this.workers.length - 1;
        this.workers.forEach(function(worker, i) {
            // Release the lock on the resource
            reservedItems += ('"' +worker.replayId + '", ');
            console.log('[QUEUE] Shutting down worker: '.cyan + worker.replayId + '/'.cyan + len);
            this.workers.splice(i, 1);
        }.bind(this));
        reservedItems = reservedItems.trim().substr(0, reservedItems.length-2);

        var query = 'UPDATE queue SET reserved = false WHERE replayId IN(' + reservedItems + ')';
        conn.query(query, function() {
            cb();
        });
    } else {
        cb();
    }
};

/*
 * Resorts the queue to set highest prio queue items first
 */

Queue.prototype.sortQueue = function(cb) {
    this.queue = this.queue.sort(function(a, b) {
        if(a.scheduledTime > b.scheduledTime) return -1;
        if(a.scheduledTime < b.scheduledTime) return 1;
        return 0;
    });
    //Logger.append('./logs/queueStatus.txt', 'Queue length is now: ', this.queue.length);
    cb();
};

Queue.prototype.getScheduledCount = function() {
    var scheduledCount = 0;
    this.queue.forEach(function(replay) {
        if(replay.isScheduledInQueue) scheduledCount += 1;
    });
    return scheduledCount;
};

/*
 * Checks if a given item is running on another worker at any time
 */

Queue.prototype.isItemIsRunningOnAnotherWorker = function(item) {
    var found = false;
    var occupiedWorker = -1;
    this.workers.some(function(worker, i) {
        found = (worker.replayId === item.replayId);
        if(found) occupiedWorker = i;
        return found;
    });
    //if(!found) console.log('Adding: ' + item.replayId + ' to worker');
    //else console.log('Item: '.red + item.replayId + ' is already running on worker '.red + occupiedWorker);
    return found;
};

/*
 * Initiailizes the workers for the queue
 */

Queue.prototype.initializeWorkers = function() {
    return new Promise(function(resolve, reject) {
        if(!this.isInitializingWorkers) {
            this.isInitializingWorkers = true;
            // Now resort the queue
            this.sortQueue(function() {
                // In the event we lose workers, we respawn them here, init them here
                var workersToCreate = this.maxWorkers - this.workers.length;
                //console.log('Can I create: ', workersToCreate + ' workers? current workers is: ' + this.workers.length);
                if(workersToCreate <= this.maxWorkers) {
                    console.log('[QUEUE] There are '.blue + this.workers.length + ' active workers on the queue, '.blue + (this.maxWorkers - this.workers.length) + ' workers are sleeping.'.blue + ' queue length is: '.blue + this.queue.length);
                    if(!(this.getScheduledCount() >= this.queue.length) || this.workers.length < this.maxWorkers) {
                        //console.log('Creating: '.green + workersToCreate + ' workers'.green);
                        for(var i = 0; i < workersToCreate; i++) {
                            var item = this.next();
                            if(typeof item === 'undefined' || item === null) {
                                var j = 0;
                                while(j < this.queue.length) {
                                    item = this.next();
                                    if(typeof item !== 'undefined' && item !== null) {
                                        j = this.queue.length;
                                    }
                                    j++;
                                }
                            }
                            if((typeof item !== 'undefined' && item !== null)) {
                                item.isScheduledInQueue = false;
                                this.workers.push(item);
                                console.log('[QUEUE] New worker created at index: '.yellow + (this.workers.length - 1) + ' servicing replay: '.yellow + item.replayId);
                            }
                        }
                    }
                    this.isInitializingWorkers = false;
                    resolve();
                } else {
                    this.isInitializingWorkers = false;
                    reject("too many workers");
                }
            }.bind(this));
        } else {
            this.isInitializingWorkers = false;
            reject('already initializing workers');
        }
    }.bind(this));
};

/*
 * Gets the next item in the queue
 */

Queue.prototype.next = function() {
    var currentJob = this.queue.shift();

    if(typeof currentJob !== 'undefined' && currentJob !== null) {
        this.queue.push(currentJob);
    }

    //console.log('checking if: ' + currentJob.replayId + ' can be parsed.', new Date().getTime() > currentJob.scheduledTime.getTime());
    if(currentJob && !currentJob.isReserved && new Date().getTime() > currentJob.scheduledTime.getTime() && !this.isItemIsRunningOnAnotherWorker(currentJob)) {
        currentJob.failed = false; // remove failed flag, we can process this item now
        return currentJob;
    } else {
        return null;
    }
};

module.exports = Queue;