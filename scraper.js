var Replay = require('./src/Replay');
var Logger = require('./src/Logger');

/*
 * Populate the list of replays
 */
function getLatestReplays() {
    console.log("Getting replays");
    return new Promise(function(resolve, reject) {
        Replay.latest().then(function(results) {
            results.forEach(function(replay) {
                new Replay(replay.replayId)
            });
            resolve();
        }, function(err) {
            Logger.append('./logs/log.txt', err);
            reject();
        });
        console.log('got replays');
    });
};

setInterval(function() {
    getLatestReplays();
}, 3000);

