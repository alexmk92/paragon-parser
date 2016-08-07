var Replay = require('./src/Replay');
var Logger = require('./src/Logger');
var colors = require('colors');

/*
 * Populate the list of replays
 */
function getLatestReplays() {
    console.log("[SCRAPER] Getting replays".cyan);
    Replay.latest();
    Replay.latest('custom');
};

// Start with a single call and then every 10 sec
getLatestReplays();
setInterval(function() {
    getLatestReplays();
}, 6000);

