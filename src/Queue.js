var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var colors = require('colors');
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var assert = require('assert');
var fs = require('fs');
var config = require('../conf.js');

var conn = new Connection();

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function(db) {
    console.log("NEW QUEUE CREATED");
    this.mongoconn = db; // If null, couldn't connect
    this.queue = [];

    this.maxWorkers = 10;
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
            console.log('refilling buffer as it was either fully reserved, empty or scheduled'.cyan);
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
            /*
            fs.stat('./out/replays/' + item.replayId + '.json', function(err, stats) {
                if(err) {} //Logger.append('./logs/log.txt', new Date() + 'Tried to remove: ' + item.replayId + '.json from the replays directory but it did not exist. ' + JSON.stringify(err));
                else {
                    fs.unlink('./out/replays/' + item.replayId + '.json', function(err) {
                        if(err) {} //Logger.append('./logs/log.txt', new Date() + ' Failed to remove: ' + item.replayId + '.json from the replays directory, it does however exist. ' + JSON.stringify(err));
                        else {
                            Logger.append('./logs/log.txt', new Date() + ' Successfully deleted file: ' + item.replayId + '.json from the replays directory, it will be processed again later and its priority has been escalated to 2.');
                        }
                    });
                }
            });
            */
        }.bind(this));
    }.bind(this));
};

/*
 * Uploads the file and disposes of the current worker
 */

Queue.prototype.uploadFile = function(item) {
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
                    if(err) this.failed(item);  // check if we need to process the item again
            });
        }
        this.workers.some(function(worker, i) {
            if(worker.replayId === item.replayId) {
                worker.isReserved = false;
                this.workers.splice(i, 1);
                console.log('Replay: '.yellow + worker.replayId + ' uploaded, the worker at: '.yellow + i + ' has been disposed'.yellow);
            }
        }.bind(this));
        // this.initializeWorkers().then(function() {
        //     this.runTasks();
        // }.bind(this));
    } catch(e) {
        console.log('Mongo error: '.red, e);
        //Logger.append('./logs/mongoError.txt', 'Mongo error: ' + JSON.stringify(e));
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
        console.log('Replay: '.red + item.replayId + ' failed to process, rescheduling 2 minutes from now'.red);
        var scheduledDate = new Date(Date.now() + 120000);
        item.scheduledTime = scheduledDate;
        item.isScheduledInQueue = true;
        Logger.append('./logs/log.txt', new Date() + ' The replay with id: ' + item.replayId + ' failed, its scheduled to re-run at ' + scheduledDate);
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
                console.log('item failed, worker at index: '.yellow + i + ' has been disposed'.yellow);
                this.workers.splice(i, 1);
                return true;
            }
            return false;
        }.bind(this));
        // this.initializeWorkers().then(function() {
        //     this.runTasks();
        // }.bind(this));
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
    console.log('scheduled to run: '.blue + item.replayId + ' at: '.blue, scheduledDate);
    var query = 'UPDATE queue SET reserved = false, scheduled = DATE_ADD(NOW(), INTERVAL 1 MINUTE), priority=3 WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        // Allow another worker to pick this up and process the full item as they will not know where to start processing from
        query = "UPDATE replays SET checkpointTime = 0 WHERE replayId = '" + item.replayId + "'";
        conn.query(query, function() {});
        this.uploadFile(item);
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
                ' LIMIT 500';

            var replaysInQueue = '';
            this.queue.forEach(function(replay) {
                replaysInQueue += '"' + replay.replayId + '", ';
            });
            if(replaysInQueue.length > 0) {
                replaysInQueue = replaysInQueue.substr(0, replaysInQueue.length -2);
                query = 'SELECT replayId, checkpointTime ' +
                    ' FROM replays ' +
                    ' WHERE replays.completed = false ' +
                    ' AND replayId NOT IN(' + replaysInQueue + ')' +
                    ' LIMIT 500';
            }

            conn.query(query, function(results) {
                if(results.length > 0) {
                    results.forEach(function(result) {
                        var replay = new Replay(this.mongoconn, result.replayId, result.checkpointTime, this);
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
                    /*
                    console.log('attempting to refill the buffer, normally due to all processes being reserved');
                    setTimeout(function() {
                        this.fillBuffer(false);
                    }.bind(this), 2500);
                    */
                } else {
                    /*
                    console.log('no jobs for queue, attempting to fill buffer in 10 seconds...');
                    if(!this.isRefreshingBuffer) {
                        this.isRefreshingBuffer = true;
                        setTimeout(function() {
                            this.isRefreshingBuffer = false;
                            this.fillBuffer(false);
                        }.bind(this), 10000);
                    }
                    */
                }
                // if(forceFill) {
                //     this.initializeWorkers().then(function() {
                //         this.runTasks();
                //     }.bind(this));
                // }
            }.bind(this));
        } else {
            setTimeout(function() {
                console.log('processing '.green + results.length + ' items'.green);
                //this.queue = [];
                results.forEach(function(result) {
                    var replay = new Replay(this.mongoconn, result.replayId, result.checkpointTime, this);
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
                        console.log('Queue now owns this resource, reserved and running work for: '.green + worker.replayId);
                        worker.isReserved = true;
                        worker.parseDataAtCheckpoint();
                    } else {
                        //Logger.append('./logs/log.txt', 'This job is scheduled to happen at ' + worker.scheduledTime + ' spawning a new worker for now.');
                        var query = 'UPDATE queue SET reserved = false WHERE replayId = "' + worker.replayId + '"';
                        conn.query(query, function() {
                            this.workers.some(function(currentWorker, i) {
                                if(worker.replayId === currentWorker.replayId) {
                                    this.workers.splice(i, 1);
                                    console.log(('Tried to run a scheduled item, worker: ' + i + ' has been disposed').blue);
                                    return true;
                                }
                                return false;
                            }.bind(this));
                            //Logger.append('./logs/log.txt', 'Removed this worker from the queue, spinning up a new worker');
                             //this.initializeWorkers().then(function() {
                             //   this.runTasks();
                             //}.bind(this));
                        }.bind(this));
                    }
                }.bind(this), function() {
                    this.workers.some(function(currentWorker, i) {
                        if(worker.replayId === currentWorker.replayId) {
                            console.log('Unknown error when running worker, worker at index: '.yellow + i + ' has been disposed'.yellow);
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
        this.workers.forEach(function(worker, i) {
            // Release the lock on the resource
            reservedItems += ('"' +worker.replayId + '", ');
            console.log('shutting down worker: '.cyan + worker.replayId);
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
                console.log('Creating: '.green + workersToCreate + ' workers'.green);
                //console.log('Can I create: ', workersToCreate + ' workers? current workers is: ' + this.workers.length);
                if(workersToCreate <= this.maxWorkers) {
                    console.log('There are '.blue + this.workers.length + ' active workers on the queue, '.blue + (this.maxWorkers - this.workers.length) + ' workers are sleeping.'.blue + ' queue length is: '.blue + this.queue.length);
                    if(!(this.getScheduledCount() >= this.queue.length) || this.workers.length < this.maxWorkers) {
                        for(var i = 0; i < workersToCreate; i++) {
                            var item = this.next();
                            if((typeof item !== 'undefined' && item !== null)) {
                                item.isScheduledInQueue = false;
                                this.workers.push(item);
                                console.log('New worker created at index: '.yellow + (this.workers.length - 1) + ' servicing replay: '.yellow + item.replayId);
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
    var currentJob = null;
    if(this.queue.length === 1) {
        currentJob = this.queue[0];
        if(currentJob && !currentJob.isReserved && new Date().getTime() > currentJob.scheduledTime.getTime() && !this.isItemIsRunningOnAnotherWorker(currentJob)) {
            return currentJob;
        }
        return null;
    } else {
        currentJob = this.queue.shift();

        if(currentJob && !currentJob.isReserved && new Date().getTime() > currentJob.scheduledTime.getTime() && !this.isItemIsRunningOnAnotherWorker(currentJob)) {
            this.queue.push(currentJob);
            return currentJob;
        }

        // All jobs are reserved or in the future, refill the buffer
        if(typeof currentJob === 'undefined' || currentJob === null) {
            return null;
        } else {
            this.queue.push(currentJob);
            this.next();
        }
    }
};

module.exports = Queue;