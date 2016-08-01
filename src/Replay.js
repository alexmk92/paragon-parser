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
};

/*
 * Kicks off the stream from the constructor once the client program invokes it on this
 * specific instance
 */

Replay.prototype.parseDataAtCheckpoint = function() {

    // Get a handle on the old file:
    this.getFileHandle().then(function(file) {
        console.log('GOT FILE HANDLE :) ');
        // Get the header and check if the game has actually finished
        // TODO Optimise so if the game status is false then we dont waste API requests
        this.isGameLive().then(function(data) {
            this.replayJSON.isLive = data.isLive;
            // Keep getting the latest check point
            this.getNextCheckpoint(this.replayJSON.newCheckpointTime, function(checkpoint) {
                this.replayJSON.lastCheckpointTime = checkpoint.lastCheckpointTime;
                this.replayJSON.newCheckpointTime = checkpoint.currentCheckpointTime;
                if(checkpoint.code === 1 && data.isLive === true) {
                    // Schedule the queue to come back to this item in 3 minutes
                    this.queueManager.schedule(this, 60000);
                    Logger.log(LOG_FILE, new Date() + ' we are already up to date with the replay: ' + this.replayId + ', reserving this replay in 1 minute');
                } else {
                    if(checkpoint.code === 0) {
                        // Update the file with the new streaming data
                        if(this.replayJSON.players.length === 0) {
                            console.log('GETTING PLAYERS');
                            this.getPlayersAndGameType(this.replayId).then(function(matchInfo) {
                                this.replayJSON.players = matchInfo.players;
                                this.replayJSON.gameType = matchInfo.gameType;
                                this.getEventFeedForCheckpoint(checkpoint.lastCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
                                    this.replayJSON.towerKills = this.replayJSON.towerKills.concat(events.towerKills);
                                    this.replayJSON.kills = this.replayJSON.kills.concat(events.kills);
                                    console.log('attemptupdate player');
                                    this.updatePlayerStats().then(function(newPlayers) {
                                        //this.replayJSON.players = newPlayers;
                                        fs.writeFileSync('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON));
                                        this.parseDataAtCheckpoint();
                                    }.bind(this));
                                }.bind(this));
                            }.bind(this));
                        } else {
                            this.getEventFeedForCheckpoint(checkpoint.lastCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
                                this.replayJSON.towerKills = this.replayJSON.towerKills.concat(events.towerKills);
                                this.replayJSON.kills = this.replayJSON.kills.concat(events.kills);
                                console.log('attemptupdate player');
                                this.updatePlayerStats().then(function(newPlayers) {
                                    //this.replayJSON.players = newPlayers;
                                    fs.writeFileSync('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON));
                                    this.parseDataAtCheckpoint();
                                }.bind(this));
                            }.bind(this));
                        }
                    } else if(checkpoint.code === 1) {
                        // Its finished lets get the match result
                        this.getMatchResult().then(function(winningTeam) {
                            this.replayJSON.winningTeam = winningTeam;
                            fs.writeFile('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON), function(err) {
                                if(err) {
                                    Logger.log(LOG_FILE, err);
                                } else {
                                    this.queueManager.removeItemFromQueue(this);
                                    Logger.log(LOG_FILE, new Date() + ' finished processing replay: ' + this.replayId);
                                }
                            }.bind(this));
                        }.bind(this));
                    }
                }
            }.bind(this));
        }.bind(this), function(httpStatus) {
            if(httpStatus === 404) {
                Logger.append(LOG_FILE, "The replay id: " + this.replayId + " has expired.");
                this.queueManager.removeItemAtIndex(this);
            }
            else {
                /*
                 fs.writeFile('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON));
                 this.parseDataAtCheckpoint();
                 */
            }
        }.bind(this));
    }.bind(this));
};

/*
 * Gets the result of the match
 */
Replay.prototype.getMatchResult = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';

    return requestify.get(url).then(function(response) {
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('WinningTeam')) {
                return data['WinningTeam'];
            }
        }
    });
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{streamId}/users
 */

Replay.prototype.getPlayersAndGameType = function() {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/users';

    return new Promise(function(resolve, reject) {
        this.getReplaySummary().then(function(payload) {
            if(payload.code === 0) {
                if(payload.data.hasOwnProperty('UserDetails')) {
                    // Get the player id's
                    console.log('has user details');
                    requestify.get(url).then(function(response) {
                        if (response.body && response.body.length > 0) {
                            var data = JSON.parse(response.body);
                            if(data.hasOwnProperty('users')) {
                                console.log('has users, loading them and game type to memory');
                                var matchDetails = {
                                    players: [],
                                    gameType: null
                                };

                                for(var i = 0; i < 10; i++) {
                                    var player = Replay.getEmptyPlayerObject();
                                    player.accountId = data.users[i];
                                    player.username = payload.data['UserDetails'][i].Nickname;
                                    player.kills = payload.data['UserDetails'][i].HeroLastHits;
                                    player.towerLastHits = payload.data['UserDetails'][i].TowerLastHits;
                                    player.deaths = payload.data['UserDetails'][i].Deaths;
                                    player.assists = payload.data['UserDetails'][i].Assists;
                                    player.heroLevel = payload.data['UserDetails'][i].Level;
                                    player.team = payload.data['UserDetails'][i].Team;
                                    player.hero = payload.data['UserDetails'][i].HeroName;

                                    matchDetails.players.push(player);
                                }
                                // Get the game type
                                var custom = false;
                                var featured = false;
                                var pvp = false;

                                // Get the game type
                                for(var j = 10; j < data.users.length; j++) {
                                    if(data.users[j].toUpperCase().trim() === 'FLAG_CUSTOM') {
                                        custom = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_PVP') {
                                        pvp = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_FEATURED') {
                                        featured = true;
                                    }
                                }
                                if(custom && featured) {
                                    matchDetails.gameType = 'CUSTOM FEATURED';
                                } else if(custom) {
                                    matchDetails.gameType = 'CUSTOM';
                                } else if(pvp && featured) {
                                    matchDetails.gameType = 'CUSTOM PVP';
                                } else if(pvp) {
                                    matchDetails.gameType = 'PVP';
                                } else if(featured) {
                                    matchDetails.gameType = 'FEATURED';
                                } else {
                                    matchDetails.gameType = 'PVP';
                                }

                                resolve(matchDetails);
                            }
                        } else {
                            Logger.log(LOG_FILE, 'No response body for getPlayersAndGameType');
                            reject();
                        }
                    }.bind(this));
                }
            } else {
                Logger.log(LOG_FILE, 'No response body for getReplaySummary');
                reject();
            }
        }.bind(this));
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/event/{streamId}_replay_details
 *
 * Hits the /users endpoint to update the players KDA
 */
Replay.prototype.updatePlayerStats = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';

    return requestify.get(url).then(function(response) {
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if(data.hasOwnProperty('UserDetails')) {
                var startTime = this.replayJSON.lastCheckpointTime;
                var endTime = this.replayJSON.newCheckpointTime;

                console.log('getting here cp dmg')
                return this.getHeroDamageAtCheckpoint(startTime, endTime).then(function(allPlayerDamage) {
                    console.log('mapping players')
                    var newPlayers = this.replayJSON.players.map(function(player, i) {
                        var playerDamage = Replay.getDamageForPlayer(player, allPlayerDamage);

                        player.kills = data['UserDetails'][i].HeroLastHits;
                        player.towerLastHits = data['UserDetails'][i].TowerLastHits;
                        player.deaths = data['UserDetails'][i].Deaths;
                        player.assists = data['UserDetails'][i].Assists;
                        player.heroLevel = data['UserDetails'][i].Level;

                        player.damageToHeroes = playerDamage.damageToHeroes;
                        player.damageToTowers = playerDamage.damageToTowers;
                        player.damageToMinions = playerDamage.damageToMinions;
                        player.damageToInhibitors = playerDamage.damageToInhibitors;
                        player.damageToHarvesters = playerDamage.damageToHarvesters;
                        player.damageToJungle = playerDamage.damageToJungle;

                        return player;
                    }.bind(this));
                    return Promise.all(newPlayers);
                }.bind(this));
            }
        } else {
            Logger.log(LOG_FILE, 'No response body for getPlayersAndGameType');
        }
    }.bind(this));
};

Replay.prototype.getHeroDamageAtCheckpoint = function(time1, time2) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=damage&time1=' + time1 + '&time2=' + time2;
    return requestify.get(url).then(function(response) {
        var allDamage = null;
        if(response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if(data.hasOwnProperty('events')) {
                allDamage = data.events.map(function(damageEvent) {
                    return this.getDamageForCheckpointId(damageEvent.id).then(function(damageItem) {
                        return damageItem;
                    }.bind(this));
                }.bind(this));
            }
        }
        return Promise.all(allDamage);
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event/{damageId}
 * Gets a specific damage event.
 */

Replay.prototype.getDamageForCheckpointId = function(eventId) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event/' + eventId;

    return requestify.get(url).then(function(response) {
        if(response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if(data.hasOwnProperty('DamageList')) {
                var damage = [];
                data['DamageList'].forEach(function(damageEvent) {
                    if(damage.length === 0) {
                        damage.push({
                            username: damageEvent['DisplayName'],
                            damageToHeroes: damageEvent['HeroDamageDealt'],
                            damageToMinions: damageEvent['MinionDamageDealt'],
                            damageToJungle: damageEvent['JungleMinionDamageDealt'],
                            damageToTowers: damageEvent['TowerDamageDealt'],
                            damageToHarvesters: damageEvent['RigDamageDealt'],
                            damageToInhibitors: damageEvent['InhibitorDamageDealt']
                        });
                    } else {
                        var found = false;
                        var modIndex = -1;
                        damage.some(function(event, i) {
                            if(damageEvent['DisplayName'] === event.username) {
                                modIndex = i;
                                found = true;
                            }
                            return found;
                        });
                        if(!found) {
                            damage.push({
                                username: damageEvent['DisplayName'],
                                damageToHeroes: damageEvent['HeroDamageDealt'],
                                damageToMinions: damageEvent['MinionDamageDealt'],
                                damageToJungle: damageEvent['JungleMinionDamageDealt'],
                                damageToTowers: damageEvent['TowerDamageDealt'],
                                damageToHarvesters: damageEvent['RigDamageDealt'],
                                damageToInhibitors: damageEvent['InhibitorDamageDealt']
                            })
                        } else if(modIndex > -1) {
                            damage[modIndex].damageToHeroes += damageEvent['HeroDamageDealt'];
                            damage[modIndex].damageToMinions += damageEvent['MinionDamageDealt'];
                            damage[modIndex].damageToJungle += damageEvent['JungleMinionDamageDealt'];
                            damage[modIndex].damageToTowers += damageEvent['TowerDamageDealt'];
                            damage[modIndex].damageToHarvesters += damageEvent['RigDamageDealt'];
                            damage[modIndex].damageToInhibitors += damageEvent['InhibitorDamageDealt'];
                        }
                    }
                });
                return damage;
            } else {
                Logger.append(LOG_FILE, 'API may have changed, event/{damageId} endpoint did not return a DamageList property.');
            }
        }
    })
};

/*
 * TYPE: GET
 * EP: /replay/v2/event/{replay_id}_replay_details
 * Gets the current overall summary of the match events
 */

Replay.prototype.getReplaySummary = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';
    return requestify.get(url).then(function (response) {
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            return { code: 0, data: data };
        } else {
            return { code: 1 };
        }
    });
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
                if(body.state.toUpperCase() === 'FINAL') {
                    resolve({ isLive: false});
                } else {
                    resolve({ isLive: true });
                }
            } else {
                Logger.append(LOG_FILE, "No state property on the download object");
                reject();
            }
        }, function(err) {
            Logger.append(LOG_FILE, "Couldn't find replay: " + err.code);
            reject(err.code);
        });
    }.bind(this));
};

/*
 * Gets the tower kills and hero kills event feed at a specific given
 * check point
 */
Replay.prototype.getEventFeedForCheckpoint = function(time1, time2) {
    return new Promise(function(resolve, reject) {
        var eventFeed = {
            kills: [],
            towerKills: []
        };
        this.getTowerKillsAtCheckpoint(time1, time2, function(events) {
            eventFeed.towerKills = events;
            this.getHeroKillsAtCheckpoint(time1, time2).then(function(events) {
                eventFeed.kills = events;
                resolve(eventFeed);
            });
        }.bind(this));
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=towerKills
 */

Replay.prototype.getTowerKillsAtCheckpoint = function(time1, time2, cb) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=towerKills&time1=' + time1 + '&time2=' + time2;
    requestify.get(url).then(function (response) {
        var events = [];
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = data.events.map(function(event) {
                   return { killer: event.meta, timestamp: event['time1'] };
                });
            }
        }
        cb(events);
    });
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=kills&time1=x&time2=y
 */

Replay.prototype.getHeroKillsAtCheckpoint = function(time1, time2) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=kills&time1=' + time1 + '&time2=' + time2;
    return requestify.get(url).then(function (response) {
        var events = [];
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = data.events.map(function(event) {
                    return this.getDataForHeroKillId(event.id).then(function(killInfo) {
                        return { killer: killInfo.killer, killed: killInfo.killed, timestamp: event['time1'] };
                    }.bind(this));
                }.bind(this));
            }
        }
        return Promise.all(events);
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event/{killId}
 *
 * Given an ID from getHeroKillsAtCheckpoint we get the killer and killer from
 * this endpoint
 */
Replay.prototype.getDataForHeroKillId = function(id) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event/' + id;
    return requestify.get(url).then(function (response) {
        if (response.body && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('Killer')) {
                return { killer: data.Killer, killed: data.Killed};
            } else {
                Logger.append(LOG_FILE, 'There was no Killer property on the kills array, API may have changed');
                reject();
            }
        }
    });
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=checkpoint
 * Gets a list of all current check points for this match
 *
 * CODE VALUES:
 * 0 = Another chunk should be taken after this one
 * 1 = Reschedule the scheduler for 3 minutes
 * 2 = There was an error, delete this?
 */

Replay.prototype.getNextCheckpoint = function(lastCheckpointTime, cb) {
    var url = `${REPLAY_URL}/replay/v2/replay/${this.replayId}/event?group=checkpoint`;

    requestify.get(url).then(function(response) {
        var newCheckpointTime = response.getBody();
        if(newCheckpointTime.hasOwnProperty('events')) {
            newCheckpointTime = newCheckpointTime.events;
            var found = false;
            if(lastCheckpointTime > 0) {
                newCheckpointTime.some(function(checkpoint) {
                    if(checkpoint['time1'] > lastCheckpointTime) {
                        newCheckpointTime = checkpoint['time1'];
                        found = true;
                    }
                    return found;
                });
            } else {
                found = true;
                newCheckpointTime = newCheckpointTime[0]['time1']
            }

            if(found) {
                cb({ code: 0, lastCheckpointTime: lastCheckpointTime, currentCheckpointTime: newCheckpointTime });
            } else {
                cb({ code: 1 });
            }
            return;
        } else {
            Logger.append('./logs/log.txt', 'events was not a valid key for the checkpoints array');
            cb({ code: 1 });
            return;
        }
    });
};

/*
 * Opens the file for this replay or creates it
 */

Replay.prototype.getFileHandle = function() {
    return new Promise(function(resolve, reject) {
        try {
            this.replayJSON = JSON.parse(fs.readFileSync('./out/replays/' + this.replayId + '.json'));
            console.log('got file handle from reading file');
            resolve();
        } catch(e) {
            if(e.code === 'ENOENT') {
                this.replayJSON = Replay.getEmptyReplayObject(this.replayId);
                fs.writeFile('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON), function(err) {
                    if(err) {
                        Logger.log(LOG_FILE, e);
                        reject(e);
                    } else {
                        console.log('got file handle from writing file');
                        resolve();
                    }
                });
            } else {
                Logger.log(LOG_FILE, e);
                reject(e);
            }
        }
    }.bind(this));
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
 * Calculates damage at a specific index based on the given player, the damage array
 * and the requested stat
 */

Replay.getDamageForPlayer = function(player, allPlayerDamage) {
    var playerDamage = {
        damageToHeroes: player.damageToHeroes,
        damageToTowers: player.damageToTowers,
        damageToJungle: player.damageToJungle,
        damageToInhibitors: player.damageToInhibitors,
        damageToHarvesters: player.damageToHarvesters,
        damageToMinions: player.damageToMinions
    };
    console.log('player damage is: ', playerDamage);
    allPlayerDamage.some(function(damageData) {
        console.log('damage data is: ', damageData);
        if(damageData.username === player.username) {
            console.log('adding from damage data: ', damageData);
            console.log('set damage');
            playerDamage.damageToHeroes += damageData.damageToHeroes;
            playerDamage.damageToTowers += damageData.damageToTowers;
            playerDamage.damageToJungle += damageData.damageToJungle;
            playerDamage.damageToInhibitors += damageData.damageToInhibitors;
            playerDamage.damageToHarvesters += damageData.damageToHarvesters;
            playerDamage.damageToMinions += damageData.damageToMinions;
            return true;
        }
        return false;
    });
    return playerDamage;
};

/*
 * STATIC
 * Return a object which contains the empty JSON structure for a Replay object
 */

Replay.getEmptyReplayObject = function(replayId) {
    return {
        replayId: replayId,
        gameStart: null,
        lastCheckpointTime: 0,
        newCheckpointTime: 0,
        isLive: true, // pertains to Final / Active
        gameType: null, // get from /replay/{streamId}/users -- if flag_pvp = pvp, flag_coop = bot, flag_custom = custom
        players: [],
        kills: [], //{ killer: 'bobby', killed: 'jane', timestamp: '' }
        towerKills: [], // {killer: 'bobby', timestamp: '' }   (just do the ?group=towerKills query as killer is in meta
        winningTeam: null
    };
};

/*
 * STATIC
 * Returns an empty player JSON object for each player we retrieve
 */

Replay.getEmptyPlayerObject = function() {
    return {
        team: 0,
        hero: null,
        username: null,  // get the user id here
        accountId: null, // get this from /replay/{streamId}/users in the users object at index i
        damageToTowers: 0,
        damageToHeroes: 0,
        damageToJungle: 0,
        damageToMinions: 0,
        damageToHarvesters: 0,
        damageToInhibitors: 0,
        heroLevel: 0,
        deaths: 0,
        assists: 0,
        towerLastHits: 0,
        mmr: null
    }
};

module.exports = Replay;