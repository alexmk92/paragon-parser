var Replay = require('./src/Replay');
var Logger = require('./src/Logger');

/*
 * Populate the list of replays
 */
function getLatestReplays() {
    console.log("Getting replays");
    Replay.latest().then(function(data) {
        console.log('Got ' + data.length + ' records');
    });
    /*
    return new Promise(function(resolve, reject) {
        Replay.latest();
        console.log('got replays');
    });
    */
};

setInterval(function() {
    getLatestReplays();
}, 3000);

