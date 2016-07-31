var requestify = require('requestify');
var http = require('http');
var sql = require('mysql');
var Logger = require('./Logger');
var Connection = require('./Connection');

var conn = new Connection();
var REPLAY_URL = 'https://orionreplay-public-service-prod09.ol.epicgames.com';

/*
 * Replay object manages which chunk of data we want to stream from the endpoint
 * it has one static method called latest which returns a list of replay ids
 */

var Replay = function(replayId) {
    this.replayId = replayId;

    //this.streamIndex = 0;
    //this.currentChunk = null;

    this.header = {
        state: 'UNSET',
        numChunks: -1,
        time: 0,
        viewerId: null
    };

    // Insert into SQL
    var query = 'INSERT INTO replays (replayId, status) VALUES ("' + replayId + '", "UNSET")';
    conn.query(query);
};

/*
 * Kicks off the stream from the constructor once the client program invokes it on this
 * specific instance
 */

Replay.prototype.parse = function() {
    var parsedData = {
        replayData: null,
        killFeed: null
    };

    // We're at the end of the game, get the overview
    if(this.header.state === "FINAL") {
        parsedData.replayData = this.getReplaySummary();
    }

    // Set the header information 
    this.header = Replay.getHeaderInformation(this.replayId);
};

/*
 * TYPE: GET
 * EP: /replay/v2/event/3f3c2c12f1874969b66ae50072714819_replay_details
 * Gets the overall summary of the match that had just been played
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
Replay.prototype.getHeaderInformation = function() {
    var url = `${REPLAY_URL}/replay/v2/replay/${this.replayId}/startDownloading`;

    requestify.post(url).then(function(response) {
        var body = response.getBody();
        console.log("BODY IS: ", body);
        this.header.numChunks = body.numChunks;
        this.header.time = body.time;
        this.header.viewerId = body.viewerId;
        this.header.state = body.state;
    }.bind(this)).then(function() {
        // Check if we got a valid state back
        if(this.header.state !== null) {
            if(this.header.state.toUpperCase() === 'FINAL') {
                this.currentChunk = this.header.numChunks-1;
            } else {
                // Start at stream beginning because the game is still live?
                this.currentChunk = 0;
            }
            //this.getChunk();
        }
    }.bind(this));
};

/*
 * TYPE: GET
 * EP :  /replay/v2/replay/{replayId}/event?group={groupType}
 */
 Replay.prototype.getInformationForGroup = function(group) {

 };

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/file/stream.{chunkIndex}
 * We dont want to deal with the binary data yet
Replay.prototype.getChunk = function() {
    var url = `${this.ROOT_URL}/replay/v2/replay/${this.replayId}/file/stream.${this.currentChunk}`;
    requestify.get(url).then(function(response) {
       var body = response.body;
    });
};
*/

/*
 * TYPE: GET
 * EP :  /replay/v2/replay/{replayId}/file/replay.header
 * Tells client which map to load when negotiating the replay, I dont believe we need this yet
 * =��,   �ɾ�      ٮ.     /Game/Maps/Agora/Agora8/Agora_P      is the response from server
 Replay.prototype.getMap = function() {

 };
 */

/*
 * TYPE: GET
 * EP : /replay/v2/replay/3275f20be8834c92ae4e8eb1b91f99b5/event?group=checkpoint
 * Tells us how long this game was, each meta point resembles 3 minutes of time, this lines up with
 * the chunks we get base on the time that has elapsed
 */

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
                    Logger.log('./logs/raw/replayLists/replay_' + new Date(), JSON.stringify(data.replays));
                    data = data.replays.map(function (replay) {
                        return {
                            replayId: replay.SessionName
                        };
                    });
                    resolve(data);
                }
                reject('no replays were found');
            } else {
                reject();
            }
        });
    });
};


module.exports = Replay;