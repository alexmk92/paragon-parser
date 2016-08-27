/*
 * Self managed cluster that does not need to be ran through PM2, it will scrape
 * Epics API for new live/completed replays at intervals defined by the constants:
 *
 * LIVE_REPLAYS_TIMEOUT and EXPIRED_REPLAYS_TIMEOUT
 *
 * Once the scraper starts it will continually call @Replay.latest method with
 * a set URL filter to start scraping from the Epic API.
 */

require('dotenv').config();

var Replay = require('./src/Replay');
var Logger = require('./src/Logger');
var colors = require('colors');
var cluster = require('cluster');

var LIVE_REPLAYS_TIMEOUT = 10000;
var EXPIRED_REPLAYS_TIMEOUT = 5000;

if(cluster.isMaster) {

    cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        Logger.writeToConsole('[SCRAPER] Something went wrong, forking a new process!'.red);
        cluster.fork();
    });
}

if(cluster.isWorker) {

    var recordFrom = new Date('August 16, 2016 21:00:00');

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
    }, EXPIRED_REPLAYS_TIMEOUT);

    // Get all of the less popular replays and service these once per 3 mins
    getLiveReplays();
    setInterval(function() {
        getLiveReplays();
    }, LIVE_REPLAYS_TIMEOUT);

    // Handle closing here:
    process.stdin.resume();//so the program will not close instantly

    function cleanup() {
        Logger.writeToConsole('[SCRAPER] Scraper was shut down successfully.'.yellow);
    }

    //do something when app is closing
    process.on('exit', cleanup);

    //catches ctrl+c event
    process.on('SIGINT', cleanup);

    //catches uncaught exceptions
    process.on('uncaughtException', function(err) {
        Logger.writeToConsole('[SCRAPER] Uncaught Exception: '.red, err);
        cleanup();
    });
}
