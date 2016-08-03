var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var assert = require('assert');
var fs = require('fs');
var config = require('../conf.js');

var conn = new Connection();

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function() {
    this.queue = [];

    this.maxWorkers = 10;
    this.isInitializingWorkers = false;
    this.isRefreshingBuffer = false;

    this.workers = [];

    setInterval(function() {
        console.log('checking if: ' + this.workers.length + ' is less than ' + this.maxWorkers);
        if(this.workers.length < this.maxWorkers) {
            this.initializeWorkers().then(function() {
                this.runTasks();
            }.bind(this), function(e) {
                console.log('rejected!', e)
            });
        }
    }.bind(this), 2500);

    setInterval(function() {
        this.disposeOfLockedReservedEvents();
    }.bind(this), 100000);

};

/*
 * Unbinds all locked events from the Queue
 */

Queue.prototype.disposeOfLockedReservedEvents = function() {
    var query = 'UPDATE queue SET reserved = false WHERE TIMEDIFF(reserved_at, NOW()) / 60 > 2';
    conn.query(query, function() {
        console.log('Disposed of unused events');
    });
};

/*
 * Removes a specific item from the queue, set its status to completed,
 * set its reserved to 0,
 */

Queue.prototype.removeItemFromQueue = function(item) {
    var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
    item.isRunningOnQueue = false;
    conn.query(query, function() {
        query = 'DELETE FROM queue WHERE replayId="' + item.replayId + '"';
        conn.query(query, function() {
            this.uploadFile(item);
            this.queue.some(function(queueItem, i) {
                if(queueItem.replayId === item.replayId) {
                    this.queue.splice(i, 1);
                    return true;
                }
                return false;
            }.bind(this));
            this.workers.some(function(worker, i) {
                if(worker.replayId === item.replayId) {
                    this.workers.splice(i, 1);
                    this.initializeWorkers().then(function() {
                        this.runTasks();
                    }.bind(this));
                }
            }.bind(this));
            Logger.append('./logs/log.txt', 'Updated replays to no longer reference ' + item.replayId);
        }.bind(this));
    }.bind(this));
};

/*
 * Uploads the file and disposes of the current worker
 */

Queue.prototype.uploadFile = function(item) {
    var url = 'mongodb://' + config.MONGO_HOST + '/paragon';
    try {
        MongoClient.connect(url, function(err, db) {
            assert.equal(null, err);
            // get a lock on this specific item
            if(!item.isUploading) {
                item.isUploading = true;
                console.log('connected to the server for uploading: ' + item.replayJSON.replayId);
                db.collection('replays').update(
                    { replayId: item.replayJSON.replayId },
                    { $set: item.replayJSON },
                    { upsert: true},
                    function(err, results) {
                        item.isUploading = false;
                        db.close();
                        if(err) this.failed(item);  // check if we need to process the item again
                }.bind(this));
            }
        });
    } catch(e) {
        Logger.append('./logs/mongoError.txt', 'Mongo error: ' + JSON.stringify(e));
    }
};

/*
 * Tells the queue manager something failed, we retry after 3 seconds
 */

Queue.prototype.failed = function(item) {
    // only run this once!
    if(!item.failed) {
        item.isRunningOnQueue = false;
        item.failed = true;
        console.log('ITEM: ' + item.replayId + ' FAILED');
        Logger.append('./logs/failedReplayId.txt', new Date() + ' The replay with id: ' + item.replayId + ' failed');
        var scheduledDate = new Date(Date.now() + 120000);
        item.scheduledTime = scheduledDate;
        item.isScheduledInQueue = true;
        var query = 'UPDATE queue SET attempts = attempts + 1, priority = 2, scheduled = DATE_ADD(NOW(), INTERVAL 3 MINUTE), reserved = false WHERE replayId = "' + item.replayId + '"';
        Logger.append('./logs/log.txt', 'Replay: ' + item.replayId + ' is now scheduled to run at ' + scheduledDate);
        conn.query(query, function(row) {
            if(row.affectedRows !== 0) {
                query = "UPDATE replays SET completed = false, status = 'UNSET', checkpointTime = '0' WHERE replayId = '" + item.replayId + "'";
                conn.query(query, function(row) {
                    Logger.append('./logs/log.txt', item.replayId + ' has had its reverted status set back to false.');
                });
                Logger.append('./logs/log.txt', new Date() + ' Item ' + item.replayId + ' failed, incremented its failed attempts and processing it later');
                fs.stat('./out/replays/' + item.replayId + '.json', function(err, stats) {
                    if(err) Logger.append('./logs/log.txt', new Date() + 'Tried to remove: ' + item.replayId + '.json from the replays directory but it did not exist. ' + JSON.stringify(err));
                    else {
                        fs.unlink('./out/replays/' + item.replayId + '.json', function(err) {
                            if(err) Logger.append('./logs/log.txt', new Date() + ' Failed to remove: ' + item.replayId + '.json from the replays directory, it does however exist. ' + JSON.stringify(err));
                            else {
                                Logger.append('./logs/log.txt', new Date() + ' Successfully deleted file: ' + item.replayId + '.json from the replays directory, it will be processed again later and its priority has been escalated to 2.');
                            }
                        });
                    }
                });
            } else {
                Logger.append('./logs/log.txt', new Date() + ' Failed to update the failure attempt for ' + item.replayId + '. Used query: ' + query + ', returned:' + JSON.stringify(row));
            }
        });
        this.workers.some(function(worker, i) {
            if(worker.replayId === item.replayId) {
                worker.isReserved = false;
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
    item.scheduledTime = scheduledDate;
    item.isScheduledInQueue = true;
    console.log('scheduled to run at: ', scheduledDate);
    var query = 'UPDATE queue SET reserved = false, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3 WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        this.uploadFile(item);
        this.workers.some(function(worker, i) {
            if(worker.replayId === item.replayId) {
                worker.isReserved = false;
                Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
                this.workers.splice(i, 1);
                Logger.append('./logs/workerLength.txt', '\nWorkers length is now ' + this.workers.length);

                Logger.append('./logs/log.txt', 'Removed worker ' + worker.replayId + ' from the queue, set priority of 3 to service it asap!');
                this.initializeWorkers().then(function() {
                    this.runTasks();
                }.bind(this));
            }
        }.bind(this));
    }.bind(this));
};

/*
 * Fills the Queue buffer with data from MySQL, it will select all records that
 * have not been completed and that are not reserved.
 */

Queue.prototype.fillBuffer = function(forceFill) {
    // First try to get data from the queue
    var queueQuery = 'SELECT queue.replayId, replays.checkpointTime, queue.scheduled, queue.priority ' +
                     ' FROM queue' +
                     ' JOIN replays ON replays.replayId = queue.replayId' +
                     ' WHERE queue.completed = false AND queue.attempts < 100 ORDER BY priority DESC';

    conn.query(queueQuery, function(results) {
        if(results.length === 0 || forceFill) {
            // Attempt to put new data into the queue
            var query = 'SELECT replayId, checkpointTime ' +
                ' FROM replays ' +
                ' WHERE replays.completed = false ' +
                ' LIMIT 100';

            var replaysInQueue = '';
            this.queue.forEach(function(replay) {
                console.log('replay is: ' + replay);
                replaysInQueue += '"' + replay.replayId + '", ';
            });
            if(replaysInQueue.length > 0) {
                replaysInQueue = replaysInQueue.substr(0, replaysInQueue.length -2);
                query = 'SELECT replayId, checkpointTime ' +
                    ' FROM replays ' +
                    ' WHERE replays.completed = false ' +
                    ' AND replayId NOT IN(' + replaysInQueue + ')' +
                    ' LIMIT 100';
            }

            conn.query(query, function(results) {
                if(results.length > 0) {
                    results.forEach(function(result) {
                        var replay = new Replay(result.replayId, result.checkpointTime, this);
                        replay.isScheduledInQueue = false;
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

                        var query = 'INSERT INTO queue (replayId) VALUES("' + replay.replayId + '")';
                        conn.query(query, function(row) {});
                    }.bind(this));
                    console.log('attempting to refill the buffer, normally due to all processes being reserved');
                    setTimeout(function() {
                        this.fillBuffer(false);
                    }.bind(this), 2500);
                } else {
                    console.log('no jobs for queue, attempting to fill buffer in 10 seconds...');
                    if(!this.isRefreshingBuffer) {
                        this.isRefreshingBuffer = true;
                        setTimeout(function() {
                            this.isRefreshingBuffer = false;
                            this.fillBuffer(false);
                        }.bind(this), 10000);
                    }
                }
            }.bind(this));
        } else {
            setTimeout(function() {
                console.log('processing ' + results.length + ' items');
                //this.queue = [];
                results.forEach(function(result) {
                    var replay = new Replay(result.replayId, result.checkpointTime, this);
                    replay.isScheduledInQueue = false;
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
                Logger.append('./logs/queueStatus.txt', 'Queue length is: ' + this.queue.length);
                this.start();
            }.bind(this), 1250);
        }
    }.bind(this));
};

/*
 * Starts running the queue
 */

Queue.prototype.start = function() {
    this.initializeWorkers().then(function() {
        this.runTasks();
    }.bind(this));
};

/*
 * Runs all tasks on current workers
 */

Queue.prototype.runTasks = function() {
    if(this.workers.length > 0) {
        //console.log('calling run workers');
        this.workers.forEach(function(worker) {
            this.reserve(worker.replayId).then(function() {
                if(new Date().getTime() > worker.scheduledTime.getTime() && worker.isRunningOnQueue === false) {
                    console.log('Queue now owns this resource, reserved and running work for: ', worker.replayId);
                    worker.isReserved = true;
                    worker.parseDataAtCheckpoint();
                } else {
                    Logger.append('./logs/log.txt', 'This job is scheduled to happen at ' + worker.scheduledTime + ' spawning a new worker for now.');
                    var query = 'UPDATE queue SET reserved = 0 WHERE replayId = "' + worker.replayId + '"';
                    conn.query(query, function() {
                        this.workers.splice(this.workers.indexOf(worker), 1);
                        Logger.append('./logs/log.txt', 'Removed this worker from the queue, spinning up a new worker');
                        this.initializeWorkers().then(function() {
                            this.runTasks();
                        }.bind(this));
                    }.bind(this));
                }
            }.bind(this), function() {
                this.workers.splice(this.workers.indexOf(worker), 1);
            }.bind(this));
        }.bind(this));
    } else {
        this.initializeWorkers().then(function() {
            this.runTasks();
        }.bind(this));
    }
};

/*
 * Reserves an item
 */

Queue.prototype.reserve = function(replayId) {
    return new Promise(function(resolve, reject) {
        // If someone reserves before we do this, then we can bail out
        var reserved_query = 'UPDATE queue SET reserved = true WHERE replayId = "' + replayId + '" AND reserved = false';
        conn.query(reserved_query, function(row) {
            if(this.ownsReservedId(replayId)) {
                resolve();
            } else if(row.changedRows === 0) {
                Logger.append('./logs/log.txt', replayId + ' is reserved by another process.');
                reject('reserved by another process as rows were 0');
            } else {
                Logger.append('./logs/log.txt', replayId + ' is reserved by another process.');
                reject('reserved by another process');
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
        this.workers.forEach(function(worker, i) {
            // Release the lock on the resource
            reservedItems += ('"' +worker.replayId + '", ');
            console.log('shutting down worker: ' + worker.replayId);
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
        if(a.priority > b.priority) return -1;
        if(a.priority < b.priority) return 1;
        return 0;
    });
    Logger.append('./logs/queueStatus.txt', 'Queue length is now: ', this.queue.length);
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
                //console.log('creating: ' + workersToCreate + ' workers');
                //console.log('Can I create: ', workersToCreate + ' workers? current workers is: ' + this.workers.length);
                if(workersToCreate <= this.maxWorkers) {
                   // console.log('spawning ' + workersToCreate + ' workers');
                    if(!(this.getScheduledCount() >= this.queue.length)) {
                        for(var i = 0; i < workersToCreate; i++) {
                            var item = this.next();
                            if(typeof item !== 'undefined' && item !== null) {
                                item.isScheduledInQueue = false;
                                this.workers.push(item);
                            }
                        }
                    } else {
                        console.log('refilling buffer as  ' + this.getScheduledCount() + ' is greater than ' + this.queue.length);
                        // prevent a process out of memory event, this is caused by workers getting fired when queue is holding to many scheduled events
                        this.fillBuffer(true);
                        this.isInitializingWorkers = false;
                        reject('refilling buffer');
                    }
                    this.isInitializingWorkers = false;
                    resolve();
                } else {
                    this.isInitializingWorkers = false;
                    reject('couldn\'t create workers as we have to many');
                }
            }.bind(this));
        } else {
            this.isInitializingWorkers = false;
            reject('cannot create workers as they have already been initialized');
        }
    }.bind(this));
};

/*
 * Gets the next item in the queue
 */

Queue.prototype.next = function() {
    var currentJob = this.queue.shift();
    if(currentJob && !currentJob.isReserved && new Date().getTime() > currentJob.scheduledTime.getTime()) {
        this.queue.push(currentJob);
        return currentJob;
    }

    // All jobs are reserved or in the future, refill the buffer
    if(typeof currentJob === 'undefined' || currentJob === null) {
        return null;
    } else {
        this.next();
    }
};

module.exports = Queue;