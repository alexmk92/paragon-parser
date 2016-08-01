var Connection = require('./Connection');
var Replay = require('./Replay');

/*
 * Queue runs and manages the data inside of the mysql collection
 */

var Queue = function() {
    this.queue = [];
    //this.stack = [];
    this.numItems = 0;
    //this.isProcessingItem = false;
    this.items = [
        { label: 1, completed: false },
        { label: 2, completed: false },
        { label: 3, completed: false },
        { label: 4, completed: false },
        { label: 5, completed: false },
        { label: 6, completed: false },
        { label: 7, completed: false },
        { label: 8, completed: false },
        { label: 9, completed: false }
    ];
};

/*
 * Removes a specific item from the queue
 */

Queue.prototype.removeItemFromQueue = function(item) {
    console.log("Removing replay: ", item.replayId);
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
    if(this.items.length === 0) {
        
    }
};

/*
 * Starts running the queue
 */

Queue.prototype.start = function() {

};

/*
 * Stops running the queue
 */

/*
 * Gets the next item in the queue
 */

Queue.prototype.next = function() {
    var currentJob = this.items.shift();
    if(currentJob) {
        currentJob.completed = Math.random() > 0.9;
        if(!currentJob.completed) {
            this.items.push(currentJob);
            var itemString = '';
            this.items.forEach(function(job) {
                itemString += 'item #' + job.label + ', ';
            });
        } else {
            console.log('disposing of: ', currentJob.label);
        }
    } else {
        console.log('no jobs left in the queue');
    }
};

module.exports = Queue;