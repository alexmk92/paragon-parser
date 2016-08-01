var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var assert = require('assert');
var config = require('../conf.js');

var conn = new Connection();

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function() {
    this.queue = [];

    this.maxWorkers = 1;
    this.currentWorkers = 0;

    this.workers = [];
};

/*
 * Removes a specific item from the queue, set its status to completed,
 * set its reserved to 0,
 */

Queue.prototype.removeItemFromQueue = function(item) {
    var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        query = 'UPDATE queue SET completed=true, reserved=0, priority=0 WHERE replayId="' + item.replayId + '"';
        conn.query(query, function() {
            this.uploadFile(item.replayJSON);
            this.workers.some(function(worker) {
                if(worker.replayId === item.replayId) {
                    this.workers.splice(this.workers.indexOf(worker));
                    this.currentWorkers-=1;
                    this.initializeWorkers().then(function() {
                        this.runTasks();
                    }.bind(this));
                }
            }.bind(this));
            if(this.queue.length < 50) {
                this.fillBuffer();
            }
            Logger.log('./logs/log.txt', 'Updated replays to no longer reference ' + item.replayId);
        }.bind(this));
    }.bind(this));
};

/*
 * Uploads the file and disposes of the current worker
 */

Queue.prototype.uploadFile = function(item) {
    var url = 'mongodb://' + config.MONGO_HOST + '/paragon';
    MongoClient.connect(url, function(err, db) {
        assert.equal(null, err);
        console.log('connected to the server woo!');
        db.collection('replays').insertOne(item, function(err, result) {
            assert.equal(err, null);
            console.log('Inserted document into the collection')
        });
        db.close();
    });
};

/*
 * Tells the queue to schedule the task x seconds in the future
 */

Queue.prototype.schedule = function(item, ms) {
    var scheduledDate = new Date(Date.now() + ms);
    var query = 'UPDATE queue SET scheduled="' + scheduledDate +  '", priority=1 WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
        this.uploadFile(item.replayJSON);
        this.workers.some(function(worker) {
            if(worker.replayId === item.replayId) {
                worker.scheduledTime = scheduledDate;
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
    var queueQuery = 'SELECT * FROM queue WHERE completed = false ORDER BY priority DESC';
    conn.query(queueQuery, function(results) {
        if(results.length === 0) {
            // Attempt to put new data into the queue
            var query = 'SELECT replayId FROM replays ' +
                'WHERE replays.completed = false ' +
                'LIMIT 10';

            conn.query(query, function(results) {
                if(results.length > 0) {
                    results.forEach(function(replay) {
                        var query = 'INSERT INTO queue (replayId) VALUES("' + replay.replayId + '")';
                        conn.query(query, function(results) {
                            Logger.log('./logs/log.txt', replay.replayId + ' is now in the queue');
                        });
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
                    this.queue.push(new Replay(result.replayId, result.checkpointTime, this));
                }.bind(this));
                this.start();
            }.bind(this), 5000);
        }
    }.bind(this));
};

/*
 * Starts running the queue
 */

Queue.prototype.start = function() {
    this.initializeWorkers().then(function() {
        setInterval(function() {
            this.runTasks();
        }.bind(this), 2000);
    }.bind(this));
};

/*
 * Runs all tasks on current workers
 */

Queue.prototype.runTasks = function() {
    if(this.workers.length > 0) {
        this.workers.forEach(function(worker) {
            this.reserve(worker.replayId).then(function() {
                if(new Date().getTime() > worker.scheduledTime) {
                    console.log('running work for: ', worker.replayId);
                    worker.parseDataAtCheckpoint();
                }
            }.bind(this), function() {
                this.workers.splice(this.workers.indexOf(worker));
                if(this.workers.length < 2) {
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
                reject();
            } else {
                resolve();
            }
            Logger.log('./logs/log.txt', replayId + ' is now reserved for processing by this queue');
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
            console.log('shut down worker: ', worker.replayId);
            this.workers.splice(this.workers.indexOf(worker), 1);
            this.currentWorkers-=1;
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
            this.workers.push(item);
            this.currentWorkers++;
        }
        resolve();
    }.bind(this));
};

/*
 * Gets the next item in the queue
 */

Queue.prototype.next = function() {
    var currentJob = this.queue.shift();
    if(currentJob) {
        return currentJob;
    }
};

module.exports = Queue;