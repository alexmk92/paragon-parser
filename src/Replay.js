var requestify = require('requestify');
var http = require('http');
var fs = require('fs');
var Logger = require('./Logger');
var Connection = require('./Connection');

var conn = new Connection();
var REPLAY_URL = 'https://orionreplay-public-service-prod09.ol.epicgames.com';
var LOG_FILE = './logs/log.txt';

/*
 * Replay object manages which chunk of data we want to stream from the endpoint
 * it has one static method called latest which returns a list of replay ids
 */

var Replay = function(replayId, queue) {
    this.replayId = replayId;
    this.replayJSON = null;

    this.queueManager = queue;
    //this.streamIndex = 0;
    //this.currentChunk = null;
};

/*
 * Kicks off the stream from the constructor once the client program invokes it on this
 * specific instance
 */

Replay.prototype.parseDataAtCheckpoint = function() {

    // Get a handle on the old file:
    try {
        this.replayJSON = JSON.parse(fs.readFileSync('./out/replays/' + this.replayId + '.json'));
    } catch(e) {
        if(e.code === 'ENOENT') {
            fs.writeFile('./out/replays/' + this.replayId + '.json', JSON.stringify(Replay.getEmptyReplayObject()));
        }
    }

    // Get the header and check if the game has actually finished
    this.isGameLive().then(function(data) {
        console.log("LIVE: " + data.isLive);
        this.replayJSON.isLive = data.isLive;
        // Keep getting the latest check point
        this.getNextCheckpoint(this.replayJSON.lastCheckPointTime, function(checkpoint) {
            if(checkpoint.code === 1) {
                // Schedule the queue to come back to this item in 3 minutes
                this.queueManager.schedule(this, 120000);
                console.log('checkpoint up to date and is: ', checkpoint);
                Logger.log(LOG_FILE, new Date() + ' we are already up to date with the replay: ' + this.replayId + ', ending parse for this session');
            } else {
                //checkpoint;
            }
        }.bind(this));
    }.bind(this), function(httpStatus) {
        if(httpStatus === 404) {
            Logger.append(LOG_FILE, "The replay id: " + this.replayId + " has expired.");
            this.queueManager.removeItemAtIndex(this);
        }
        else {
            // do another attempt
        }
    }.bind(this));



    // Set reserved on the replay


    // We're at the end of the game, get the overview
    /*
    if(this.header.state === "FINAL") {
        parsedData.replayData = this.getReplaySummary();
    }
    */

    // Set the header information 
    //this.header = Replay.getHeaderInformation(this.replayId);
};

/*
 * TYPE: GET
 * EP: /replay/v2/event/3f3c2c12f1874969b66ae50072714819_replay_details
 * Gets the current overall summary of the match events
 */

Replay.prototype.getReplaySummary = function() {

};

/*
 * TYPE: POST
 * EP : /replay/v2/replay/{replayId}/startDownloading?user={userId}
 * Hits the /startDownloading endpoint, this will tell us at what chunk the current
 * live match is in, if the state is final we can ignore all other chunks and just process
 * the final chunk
 */
Replay.prototype.isGameLive = function() {
    return new Promise(function(resolve, reject) {
        var url = `${REPLAY_URL}/replay/v2/replay/${this.replayId}/startDownloading`;
        console.log("URL IS: " + url);
        requestify.post(url).then(function(response) {
            var body = response.getBody();
            if(body.hasOwnProperty('state')) {
                console.log("RESPONSE: ", body);
                if(body.state.toUpperCase() === 'FINAL') {
                    resolve({ isLive: false});
                } else {
                    resolve({ isLive: true });
                }
            }
        }, function(err) {
            Logger.append(LOG_FILE, "Couldn't find replay: " + err.code);
            reject(err.code);
        });
    }.bind(this));
};

Replay.prototype.writeFile = function() {
    var json = {};
    // Attempt to read from an existing file:

    // If file doesn't exist use this file:


    // Write the file:

};

/*
 * TYPE: GET
 */

Replay.prototype.getTowerKillsAtCheckpoint = function(checkpointTime) {

};

Replay.prototype.getHeroKillsAtCheckpoint = function(checkpointTime) {

};

/*
 * TYPE: GET
 * EP :  /replay/v2/replay/{replayId}/event?group={groupType}
 */
 Replay.prototype.getInformationForGroup = function(group) {

 };

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=checkpoint
 * Gets a list of all current check points for this match
 */

Replay.prototype.getNextCheckpoint = function(lastCheckpointTime, cb) {
    var newCheckpointTime = 0;
    console.log("Checking if " + lastCheckpointTime + " is equal to " + newCheckpointTime);
    var url = `${REPLAY_URL}/replay/v2/replay/${this.replayId}/event?group=checkpoint`;

    requestify.get(url).then(function(response) {
        var newCheckpointTime = response.getBody();
        if(newCheckpointTime.hasOwnProperty('events')) {
            newCheckpointTime = newCheckpointTime.events;
            if(lastCheckpointTime === 0) {
                cb({ code: 0, lastCheckpointTime: lastCheckpointTime, currentCheckpointTime: newCheckpointTime[0]['time1'] });
            } else {
                newCheckpointTime.some(function(checkpoint) {
                    if(checkpoint['time1'] > lastCheckpointTime) {
                        newCheckpointTime = checkpoint['time1'];
                        return true;
                    }
                    return false;
                });
                cb({ code: 0, lastCheckpointTime: lastCheckpointTime, currentCheckpointTime: newCheckpointTime });
            }
            cb({ code: 1 });
        } else {
            Logger.append('./logs/log.txt', 'events was not a valid key for the checkpoints array')
            cb({ code: 1 });
        }
    });
};


/*
 * STATIC
 * This method gets a list of the 500 most recent games from the Epic API.
 * we only send back the Replay ID as we then create a Replay object for
 * each of these replays
 */

Replay.latest = function() {
    return new Promise(function(resolve, reject) {
        console.log("GETTING THEM");
        var url = `${REPLAY_URL}/replay/v2/replay`;
        var data = null;
        requestify.get(url).then(function (response) {
            if (response.body && response.body.length > 0) {
                data = JSON.parse(response.body);
                if (data.hasOwnProperty('replays')) {
                    data.replays.map(function (replay) {
                        // Insert into SQL
                        var query = 'INSERT INTO replays (replayId, status) VALUES ("' + replay.SessionName + '", "UNSET")';
                        conn.query(query);
                    });
                    resolve(data.replays);
                }
                reject('0 Replays were on the endpoint');
            } else {
                reject('The body had no replay data.');
            }
        });
    });
};

/*
 * STATIC
 * Return a object which contains the empty JSON structure for a Replay object
 */

Replay.getEmptyReplayObject = function() {
    return {
        replayId: null,
        gameStart: null,
        lastCheckPointTime: 0,
        isLive: true, // pertains to Final / Active
        gameType: null, // get from /replay/{streamId}/users -- if flag_pvp = pvp, flag_coop = bot, flag_custom = custom
        players: [
            {
                team: 0,
                hero: null,
                username: null,  // get the user id here
                accountId: null, // get this from /replay/{streamId}/users in the users object at index i
                damageToTowers: 0,
                damageToHeroes: 0,
                damageToJungle: 0,
                damageToMinions: 0,
                damageToHarvester: 0,
                damageToInhibitors: 0,
                heroLevel: 0,
                deaths: 0,
                assists: 0,
                towerLastHits: 0,
                mmr: null
            }
            // 10 of them
        ],
        kills: [], //{ killer: 'bobby', killed: 'jane', timestamp: '' }
        towers: [], // {killer: 'bobby', timestamp: '' }   (just do the ?group=towerKills query as killer is in meta
        winningTeam: null
    };
};


module.exports = Replay;