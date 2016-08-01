var Connection = require('./Connection');
var Replay = require('./Replay');
var Logger = require('./Logger');

var conn = new Connection();

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function() {
    this.queue = [];

    this.maxWorkers = 5;
    this.currentWorkers = 0;
    this.scheduledJobs = 0;

    this.workers = [];
};

/*
 * Removes a specific item from the queue, set its status to completed,
 * set its reserved to 0,
 */

Queue.prototype.removeItemFromQueue = function(item) {
    console.log("Removing replay: ", item.replayId);
    var query = 'UPDATE replays SET completed=true, status="FINAL" WHERE replayId="' + item.replayId + '"';
    conn.query(query, function() {
       Logger.log('./logs/log.txt', 'Updated replays to no longer reference ' + item.replayId);
    });
};

/*
 * Uploads the file and disposes of the current worker
 */

Queue.prototype.uploadFile = function(item) {
    
};

/*
 * Tells the queue to schedule the task x seconds in the future
 */

Queue.prototype.schedule = function(item, ms) {
    console.log("Setting scheduler");
    var scheduledDate = new Date(Date.now() + ms);
    console.log("Scheduling for: ", scheduledDate);
};

/*
 * Fills the Queue buffer with data from MySQL, it will select all records that
 * have not been completed and that are not reserved.
 */

Queue.prototype.fillBuffer = function() {
    // First try to get data from the queue
    var queueQuery = 'SELECT * FROM queue WHERE reserved = false AND completed = false';
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
                    this.fillBuffer();
                } else {
                    console.log('no jobs for queue, attempting to fill buffer in 10 seconds...');
                    setTimeout(function() {
                        this.fillBuffer();
                    }.bind(this), 10000);
                }
            }.bind(this));
        } else {
            console.log('processing ' + results.length + ' items');
            results.forEach(function(result) {
                this.queue.push(new Replay(result.replayId, this));
            }.bind(this));
            this.start();
        }
    }.bind(this));
};

/*
 * Reserves an item
 */

Queue.prototype.reserve = function(replay) {
    // If someone reserves before we do this, then we can bail out
    var reserved_query = 'UPDATE queue SET reserved = true WHERE replayId = "' + replay.replayId + '" AND reserved = false';
    conn.query(reserved_query, function(rows) {
        rows.forEach(function(row) {
            if(row.changedRows === 0) {
                console.log('this is reserved');
            } else {
                console.log('do the work')
            }
        });
        Logger.log('./logs/log.txt', replay.replayId + ' is now reserved for processing by this queue');
    });
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

};

/*
 * Initiailizes the workers for the queue
 */

Queue.prototype.initializeWorkers = function() {
    return new Promise(function(resolve, reject) {
        for(var i = 0; i < this.maxWorkers; i++) {
            this.workers.push(this.next());
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
    if(!currentJob || this.items.length < 100) {
        this.fillBuffer();
    }
};

module.exports = Queue;