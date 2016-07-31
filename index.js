var request = require('requestify');
var Replay = require('./src/Replay');
var Logger = require('./src/Logger');

var replays = [];

var RETRY_SERVICE_DELAY = 10000; // if we can't get the latest 500, this is a retry delay 

 /*
 * Populate the list of replays
 */
function getLatestReplays() {
    return new Promise(function(resolve, reject) {
        Replay.latest().then(function(results) {
            results.forEach(function(replay) {
                var found = false;
                replays.some(function(existingReplay) {
                    found = (existingReplay.replayId === replay.replayId);
                    return found;
                });
                if(!found) {
                    replays.push(new Replay(replay.replayId));
                }
            });
            resolve();
        }, function(err) {
            Logger.append('./logs/log.txt', err);
            reject();
        });
        console.log('got replays');
    }).then(function() {
        beginQueue();
    });
};

getLatestReplays();

function beginQueue() {
    console.log('beginning queue');
    console.log(replays[0]);
}

