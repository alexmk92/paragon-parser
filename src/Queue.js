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

    this.maxWorkers = 5;

    this.workers = [];
};

/*
 * Removes a specific item from the queue, set its status to completed,
 * set its reserved to 0,
 */

Queue.prototype.removeItemFromQueue = function(item) {
    var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        query = 'DELETE FROM queue WHERE replayId="' + item.replayId + '"';
        conn.query(query, function() {
            this.uploadFile(item.replayJSON);
            this.workers.some(function(worker) {
                if(worker.replayId === item.replayId) {
                    worker.isRunningOnWorker = false;
                    Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
                    this.workers.splice(this.workers.indexOf(worker), 1);
                    Logger.append('./logs/workerLength.txt', '\nWorkers length is now ' + this.workers.length);
                    this.initializeWorkers().then(function() {
                        this.runTasks();
                    }.bind(this));
                }
            }.bind(this));
            if(this.queue.length < 50) {
                this.fillBuffer();
            }
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
            console.log('connected to the server for uploading!');
            db.collection('replays').update(
                { replayId: item.replayId },
                { $set: item },
                { upsert: true}
            );
            db.close();
        });
    } catch(e) {
        Logger.append('./logs/mongoError.txt', JSON.stringify(e));
    }
};

/*
 * Tells the queue manager something failed, we retry after 3 seconds
 */

Queue.prototype.failed = function(item) {
    console.log('ITEM: ' + item.replayId + ' FAILED');
    Logger.append('./logs/failedReplayId.txt', new Date() + ' The replay with id: ' + item.replayId + ' failed');
    var scheduledDate = new Date(Date.now() + 60000);
    var query = 'UPDATE queue SET attempts = attempts + 1, priority = 2, scheduled = "' + scheduledDate + '", reserved = false WHERE replayId = "' + item.replayId + '"';
    Logger.append('./logs/log.txt', 'Replay: ' + item.replayId + ' is now scheduled to run at ' + scheduledDate);
    conn.query(query, function(row) {
        if(row.affectedRows !== 0) {
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
    this.workers.some(function(worker) {
       if(worker.replayId === item.replayId) {
           worker.isReserved = false;
           Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
           this.workers.splice(this.workers.indexOf(worker), 1);
           Logger.append('./logs/workerLength.txt', '\nWorkers length is now ' + this.workers.length);
           return true;
       }
       return false;
    }.bind(this));
};

/*
 * Tells the queue to schedule the task x seconds in the future
 */

Queue.prototype.schedule = function(item, ms) {
    var scheduledDate = new Date(Date.now() + ms);
    console.log('scheduled to run at: ', scheduledDate);
    var query = 'UPDATE queue SET scheduled="' + scheduledDate +  '", priority=3 WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        this.uploadFile(item.replayJSON);
        this.workers.some(function(worker) {
            if(worker.replayId === item.replayId) {
                worker.isReserved = false;
                Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
                this.workers.splice(this.workers.indexOf(worker), 1);
                Logger.append('./logs/workerLength.txt', '\nWorkers length is now ' + this.workers.length);

                Logger.append('./logs/log.txt', 'Removed worker ' + worker.replayId + ' from the queue, set priority of 3 to service it asap!');
                this.initializeWorkers();
                /*
                worker.isRunningOnWorker = false;
                worker.scheduledTime = scheduledDate;
                Logger.append('./logs/log.txt', 'Setting worker: ' + worker.replayId + ' to idle, spawning another worker on the queue temporarily.');
                this.maxWorkers++;
                this.initializeWorkers();
                setTimeout(function() {
                    console.log('scheduled time arrived, getting next cp for: ' + worker.replayId);
                    worker.parseDataAtCheckpoint();
                    this.maxWorkers--;
                }.bind(this), ms);
                */
            }
        }.bind(this));
    }.bind(this));
};

/*
 * Fills the Queue buffer with data from MySQL, it will select all records that
 * have not been completed and that are not reserved.
 */

Queue.prototype.fillBuffer = function() {
    // First try to get data from the queue
    var queueQuery = 'SELECT queue.replayId, replays.checkpointTime FROM queue' +
                     ' JOIN replays ON replays.replayId = queue.replayId' +
                     ' WHERE queue.completed = false AND queue.attempts < 10 ORDER BY priority DESC';
    conn.query(queueQuery, function(results) {
        if(results.length === 0) {
            // Attempt to put new data into the queue
            var query = 'SELECT replayId, checkpointTime FROM replays ' +
                'WHERE replays.completed = false ' +
                'LIMIT 200';

            conn.query(query, function(results) {
                if(results.length > 0) {
                    results.forEach(function(replay) {
                        var query = 'INSERT INTO queue (replayId) VALUES("' + replay.replayId + '")';
                        conn.query(query, function() { });
                    }.bind(this));
                    console.log('attempting to refill the buffer, normally due to all processes being reserved');
                    setTimeout(function() {
                        this.fillBuffer();
                    }.bind(this), 2500);
                } else {
                    console.log('no jobs for queue, attempting to fill buffer in 10 seconds...');
                    setTimeout(function() {
                        this.fillBuffer();
                    }.bind(this), 10000);
                }
            }.bind(this));
        } else {
            setTimeout(function() {
                console.log('processing ' + results.length + ' items');
                this.queue = [];
                results.forEach(function(result) {
                    var replay = new Replay(result.replayId, results.checkpointTime, this);
                    replay.isReserved = false;
                    this.queue.push(replay);
                }.bind(this));
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
        console.log('calling run workers');
        this.workers.forEach(function(worker) {
            this.reserve(worker.replayId).then(function() {
                console.log('trying to parse: ', worker.replayId);
                if(!this.isRunningOnWorker && new Date().getTime() > worker.scheduledTime) {
                    console.log('running work for: ', worker.replayId);
                    worker.isReserved = true;
                    worker.isRunningOnWorker = true;
                    worker.parseDataAtCheckpoint();
                }
            }.bind(this), function() {
                Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
                this.workers.splice(this.workers.indexOf(worker), 1);
                Logger.append('./logs/workerLength.txt', '\nWorkers length is now ' + this.workers.length);

                if(this.workers.length === 0) {
                    this.fillBuffer();
                }
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
                reject();
            } else {
                Logger.append('./logs/log.txt', replayId + ' is reserved by another process.');
                reject();
            }
        }.bind(this));
    }.bind(this));
};

Queue.prototype.ownsReservedId = function(replayId) {
    var found = false;
    this.workers.some(function(worker) {
         if(worker.replayId === replayId) {
             found = true;
             console.log('you own this reserved id: ', replayId);
         }
        return found;
    });
    console.log('checking reserve', found);
    return found;
};

/*
 * Stops the queue and releases any assets it is locking
 */

Queue.prototype.stop = function() {
    this.workers.forEach(function(worker) {
        // Release the lock on the resource
        var query = 'UPDATE queue SET reserved = 0 WHERE replayId="' + worker.replayId + '"';
        conn.query(query, function() {
            Logger.append('./logs/workerLength.txt', '\nWorkers was length ' + this.workers.length);
            this.workers.splice(this.workers.indexOf(worker), 1);
            Logger.append('./logs/workerLength.txt', 'Workers length is now ' + this.workers.length);
        }.bind(this));
    }.bind(this));
    console.log('All workers have been shut down');
};

/*
 * Initiailizes the workers for the queue
 */

Queue.prototype.initializeWorkers = function() {
    return new Promise(function(resolve, reject) {
        // In the event we lose workers, we respawn them here, init them here
        var workersToCreate = this.maxWorkers - this.workers.length;
        for(var i = 0; i < workersToCreate; i++) {
            var item = this.next();
            if(typeof item !== 'undefined' && item !== null) {
                console.log('got item: ' + item.replayId);
                item.isRunningOnWorker = true;
                this.workers.push(item);
            }
        }
        resolve();
    }.bind(this));
};

/*
 * Gets the next item in the queue
 */

Queue.prototype.next = function() {
    var currentJob = this.queue.shift();
    if(currentJob && !currentJob.isReserved) {
        return currentJob;
    } else {
        return null;
    }
};

module.exports = Queue;