var Replay = require('./src/Replay');
var Logger = require('./src/Logger');
var colors = require('colors');

function getCustomAndFeaturedReplays() {
    console.log("[SCRAPER] Getting latest pvp, custom & featured replays".cyan);
    Replay.latest(null, 'false');
    Replay.latest('custom', 'false');
    Replay.latest('featured', 'false');
}

function getLiveReplays() {
    console.log("[SCRAPER] Getting latest live replays".cyan);
    Replay.latest(null, 'true');
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

