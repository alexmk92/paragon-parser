var Replay = require('./src/Replay');
var Logger = require('./src/Logger');
var colors = require('colors');
var cluster = require('cluster');

if(cluster.isMaster) {

    cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        console.log('[SCRAPER] Something went wrong, forking a new process!'.red);
        cluster.fork();
    });
}

if(cluster.isWorker) {

    var recordFrom = new Date('August 8, 2016 23:00:59');

    function getCustomAndFeaturedReplays() {
        Replay.latest(null, 'false', recordFrom);
        Replay.latest('custom', 'false', recordFrom);
        Replay.latest('featured', 'false', recordFrom);
    }

    function getLiveReplays() {
        Replay.latest(null, 'true', recordFrom);
    }

    // Start with a single call and then every 10 sec
    getCustomAndFeaturedReplays();
    setInterval(function() {
        getCustomAndFeaturedReplays();
    }, 120000);

    // Get all of the less popular replays and service these once per 3 mins
    getLiveReplays();
    setInterval(function() {
        getLiveReplays();
    }, 6000);

    // Handle closing here:
    process.stdin.resume();//so the program will not close instantly

    function cleanup() {
        console.log('[SCRAPER] Scraper was shut down successfully.'.yellow);
    }

    //do something when app is closing
    process.on('exit', cleanup);

    //catches ctrl+c event
    process.on('SIGINT', cleanup);

    //catches uncaught exceptions
    process.on('uncaughtException', function(err) {
        console.log('[SCRAPER] Uncaught Exception: '.red, err);
        cleanup();
    });
}
