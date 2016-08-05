var Replay = require('./src/Replay');
var Logger = require('./src/Logger');
var colors = require('colors');

/*
 * Populate the list of replays
 */
function getLatestReplays() {
    console.log("Getting replays".cyan);
    Replay.latest();
};

setInterval(function() {
    getLatestReplays();
}, 3000);

