var requestify = require('requestify');
var http = require('http');
var fs = require('fs');
var Logger = require('./Logger');
var Connection = require('./Connection');
//var conf = require('../conf.js');
var request = require('request');

//var conn = new Connection();
var REPLAY_URL = 'https://orionreplay-public-service-prod09.ol.epicgames.com';
/*
 * Replay object manages which chunk of data we want to stream from the endpoint
 * it has one static method called latest which returns a list of replay ids
 */

var Replay = function(db, replayId, checkpointTime, attempts, queue) {
    this.mongoconn = db;
    this.replayId = replayId;
    this.replayJSON = null;
    this.maxCheckpointTime = 0;
    this.checkpointTime = 0;
    this.attempts = attempts;

    this.queueManager = queue;
};

/*
 * Kicks off the stream from the constructor once the client program invokes it on this
 * specific instance
 */

Replay.prototype.parseDataAtCheckpoint = function() {
    // Get a handle on the old file:
    this.getFileHandle().then(function() {
        // if(this.replayJSON.isLive === false && this.replayJSON.previousCheckpointTime === this.replayJSON.latestCheckpointTime && this.replayJSON.winningTeam !== null) {
        //     this.queueManager.removeDeadReplay(this);
        //     return;
        // }

        // Get the header and check if the game has actually finished
        // TODO Optimise so if the game status is false then we dont waste API requests
        this.isGameLive().then(function(data) {
            this.replayJSON.isLive = data.isLive;
            this.replayJSON.startedAt = new Date(data.startedAt);

            // Keep getting the latest check point
            this.getNextCheckpoint(this.replayJSON.latestCheckpointTime).then(function(checkpoint) {
                if(typeof checkpoint.previousCheckpointTime !== 'undefined' && typeof checkpoint.currentCheckpointTime !== 'undefined') {
                    this.replayJSON.previousCheckpointTime = checkpoint.previousCheckpointTime;
                    this.replayJSON.latestCheckpointTime = checkpoint.currentCheckpointTime;
                }
                //var liveString = data.isLive ? 'live' : 'not live';
                //Logger.writeToConsole('Replay: '.magenta + this.replayId + ' is '.magenta + liveString + ' and has streamed '.magenta + this.replayJSON.latestCheckpointTime + '/'.magenta + this.maxCheckpointTime + 'ms'.magenta);
                if(checkpoint.code === 2 && this.maxCheckpointTime === 0) {
                    // this happens when no checkponint data is found
                    this.attempts++;
                    if(this.attempts > 10) {
                        this.queueManager.removeDeadReplay(this);
                    } else {
                        // TODO: Refactor this in future so its cleaner
                        // We check 5 times (5 minutes) to see if any events have happened, if not increment its failed attempts again
                        if(this.attempts <= 4) {
                            if(typeof this.replayJSON.players !== 'undefined' && this.replayJSON.players.length === 0) {
                                this.getPlayersAndGameType(this.replayId).then(function(matchInfo) {
                                    this.replayJSON.players = matchInfo.players;
                                    this.replayJSON.gameType = matchInfo.gameType;
                                    this.replayJSON.isFeatured = matchInfo.isFeatured;
                                    //Logger.writeToConsole('[REPLAY] Replay: '.yellow + this.replayId + ' has no checkpoint data yet, but has been uploaded with empty stats: '.yellow + this.replayId);
                                    Logger.writeToConsole('[REPLAY] Replay: '.yellow + this.replayId + ' has no checkpoint data yet, but has been uploaded with empty stats'.yellow);
                                    this.queueManager.schedule(this, 60000);
                                }.bind(this), function(isBotGame) {
                                    if(isBotGame) {
                                        this.queueManager.removeBotGame(this);
                                    } else {
                                        this.queueManager.failed(this);
                                    }
                                }.bind(this));
                            }
                        } else {
                            this.queueManager.failed(this);
                        }
                    }
                } else if(checkpoint.code === 1 && data.isLive === true) {
                    // Schedule the queue to come back to this item in 1 minute
                    this.queueManager.schedule(this, 60000);
                } else {
                    if(checkpoint.code === 0) {
                        // Update the file with the new streaming data
                        if(typeof this.replayJSON.players !== 'undefined' && this.replayJSON.players.length === 0) {
                            this.getPlayersAndGameType(this.replayId).then(function(matchInfo) {
                                this.replayJSON.players = matchInfo.players;
                                this.replayJSON.gameType = matchInfo.gameType;
                                this.replayJSON.isFeatured = matchInfo.isFeatured;

                                this.getEventFeedForCheckpoint(checkpoint.previousCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
                                    if(events.towerKills.length !== 0 || events.kills.length !== 0) {
                                        events.towerKills.forEach(function(towerKill) {
                                            var found = false;
                                            if(this.replayJSON.towerKills.length > 0) {
                                                this.replayJSON.towerKills.some(function(tk) {
                                                    found = (towerKill.killer === tk.killer && towerKill.timestamp === tk.timestamp);
                                                    return found;
                                                });
                                            }
                                            if(!found) this.replayJSON.towerKills.push(towerKill);
                                        }.bind(this));

                                        events.kills.forEach(function(kill) {
                                            var found = false;
                                            if(this.replayJSON.playerKills.length > 0) {
                                                this.replayJSON.playerKills.some(function(k) {
                                                    found = kill.killer === k.killer && kill.timestamp === k.timestamp && kill.killed === k.killed;
                                                    return found;
                                                });
                                            }
                                            if(!found) this.replayJSON.playerKills.push(kill);
                                        }.bind(this));
                                    }
                                    this.updatePlayerStats().then(function(newPlayers) {
                                        if(newPlayers !== null) {
                                            this.replayJSON.players = newPlayers;
                                        }
                                        this.parseDataAtCheckpoint();
                                    }.bind(this));
                                }.bind(this));
                            }.bind(this), function(isBotGame) {
                                if(isBotGame) {
                                    this.queueManager.removeBotGame(this);
                                } else {
                                    this.queueManager.failed(this);
                                }
                            }.bind(this));
                        } else {
                            this.getEventFeedForCheckpoint(checkpoint.previousCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
                                if(events.towerKills.length !== 0 || events.kills.length !== 0) {
                                    events.towerKills.forEach(function (towerKill) {
                                        var found = false;
                                        if (this.replayJSON.towerKills.length > 0) {
                                            this.replayJSON.towerKills.some(function (tk) {
                                                found = (towerKill.killer === tk.killer && towerKill.timestamp === tk.timestamp);
                                                return found;
                                            });
                                        }
                                        if (!found) this.replayJSON.towerKills.push(towerKill);
                                    }.bind(this));

                                    events.kills.forEach(function (kill) {
                                        var found = false;
                                        if (this.replayJSON.playerKills.length > 0) {
                                            this.replayJSON.playerKills.some(function (k) {
                                                found = kill.killer === k.killer && kill.timestamp === k.timestamp && kill.killed === k.killed;
                                                return found;
                                            });
                                        }
                                        if (!found) this.replayJSON.playerKills.push(kill);
                                    }.bind(this));
                                }
                                this.updatePlayerStats().then(function(newPlayers) {
                                    if(newPlayers !== null) {
                                        this.replayJSON.players = newPlayers;
                                    }
                                    this.parseDataAtCheckpoint();
                                }.bind(this));
                            }.bind(this));
                        }
                    } else if(checkpoint.code === 1 && this.maxCheckpointTime > 0) {
                        // Its finished lets get the match result
                        this.getMatchResult().then(function(matchResult) {
                            this.replayJSON.winningTeam = matchResult.winningTeam;
                            this.replayJSON.gameLength = matchResult.gameLength;
                            this.replayJSON.isLive = false;
                            this.replayJSON.previousCheckpointTime = this.replayJSON.latestCheckpointTime;

                            var ms = new Date(this.replayJSON.startedAt).getTime();
                            this.replayJSON.endedAt = new Date(ms + matchResult.gameLength);
                            //this.endMatch();
                            this.queueManager.removeItemFromQueue(this);
                        }.bind(this), function(err) {
                            //Logger.writeToConsole('[REPLAY] Error when getting match result: '.red + err);
                            Logger.writeToConsole('[REPLAY] Error when getting match result for replay: '.red + this.replayId);
                            this.queueManager.failed(this);
                        }.bind(this));
                    }
                }
            }.bind(this)).catch(function(err) {
                var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
                this.queueManager.failed(this);
            }.bind(this));
        }.bind(this), function(httpStatus) {
            if(httpStatus === 404) {
                Logger.writeToConsole('[REPLAY] The replay: '.red + this.replayId + ' has expired.'.red);
                this.queueManager.removeItemFromQueue(this);
            } else {
                Logger.writeToConsole('[REPLAY] Failed as a http status of: '.red + httpStatus + ' was returned for replay '.red + this.replayId);
                this.queueManager.failed(this);
            }
        }.bind(this));
    }.bind(this));
};

/*
 * Gets the result of the match
 */

Replay.prototype.getMatchResult = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';
    var matchLengthUrl = REPLAY_URL + '/replay/v2/replay?user=' + this.replayId;

    return new Promise(function(resolve, reject) {
        var matchResult = {
            winningTeam: null,
            gameLength: 0
        };

        requestify.get(url).then(function(response) {
            if (typeof response.body !== 'undefined' && response.body.length > 0) {
                var data = JSON.parse(response.body);
                if (data.hasOwnProperty('WinningTeam')) {
                    matchResult.winningTeam = data.WinningTeam;
                    requestify.get(matchLengthUrl).then(function(response) {
                        var data = JSON.parse(response.body);
                        if(data.hasOwnProperty('replays')) {
                            if(data.replays[0].hasOwnProperty('DemoTimeInMS')) {
                                matchResult.gameLength = data.replays[0].DemoTimeInMS;
                                resolve(matchResult);
                            } else {
                                //Logger.writeToConsole(data);
                                reject('DemoTimeInMS was not a valid property in getMatchResult.');
                            }
                        } else {
                            reject('replays was not a valid property in getMatchResult.');
                        }
                    }, function(err) {
                        reject(err);
                    });
                } else {
                    reject('WinningTeam was not a valid property in getMatchResult');
                }
            }
        }, function(err) {
            var error = 'Error in getMatchResult: ' + JSON.stringify(err);
            this.queueManager.failed(this);
            reject(err);
        }.bind(this));
    });
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/users
 */

Replay.prototype.getPlayersAndGameType = function() {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/users';

    return new Promise(function(resolve, reject) {
        this.getReplaySummary().then(function(payload) {
            if(payload.code === 0) {
                if(payload.data.hasOwnProperty('UserDetails')) {
                    // Get the player id's
                    requestify.get(url).then(function(response) {
                        if (typeof response.body !== 'undefined' && response.body.length > 0) {
                            var data = JSON.parse(response.body);
                            if(data.hasOwnProperty('users')) {
                                var matchDetails = {
                                    players: [],
                                    gameType: null
                                };

                                // Get the game type
                                var custom = false;
                                var featured = false;
                                var pvp = false;
                                var solo_ai = false;
                                var coop_ai = false;

                                // Get the game type
                                for(var j = 0; j < data.users.length; j++) {
                                    if(data.users[j].toUpperCase().trim() === 'FLAG_CUSTOM') {
                                        custom = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_PVP') {
                                        pvp = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_FEATURED') {
                                        featured = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_SOLO') {
                                        solo_ai = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_COOP') {
                                        coop_ai = true;
                                    }
                                }

                                matchDetails.isFeatured = featured;
                                if(custom) { matchDetails.gameType = 'custom' }
                                if(pvp) { matchDetails.gameType = 'pvp' }
                                if(coop_ai) { matchDetails.gameType = 'coop_ai' }
                                if(solo_ai) { matchDetails.gameType = 'solo_ai' }

                                var playersArray = [];
                                var botsArray = [];

                                // Remove bots from master array and store in temp array
                                payload.data.UserDetails.forEach(function(user, i) {
                                    var player = Replay.getEmptyPlayerObject();
                                    if(coop_ai || solo_ai) {
                                        if(Replay.isBot(user.Nickname)) {
                                            player.accountId = 'bot';
                                            player.username = payload.data.UserDetails[i].Nickname;
                                            player.kills = payload.data.UserDetails[i].HeroLastHits;
                                            player.towerLastHits = payload.data.UserDetails[i].TowerLastHits;
                                            player.deaths = payload.data.UserDetails[i].Deaths;
                                            player.assists = payload.data.UserDetails[i].Assists;
                                            player.heroLevel = payload.data.UserDetails[i].Level;
                                            player.team = payload.data.UserDetails[i].Team;
                                            player.hero = payload.data.UserDetails[i].HeroName;
                                            botsArray.push(player);
                                        } else {
                                            player.username = payload.data.UserDetails[i].Nickname;
                                            player.kills = payload.data.UserDetails[i].HeroLastHits;
                                            player.towerLastHits = payload.data.UserDetails[i].TowerLastHits;
                                            player.deaths = payload.data.UserDetails[i].Deaths;
                                            player.assists = payload.data.UserDetails[i].Assists;
                                            player.heroLevel = payload.data.UserDetails[i].Level;
                                            player.team = payload.data.UserDetails[i].Team;
                                            player.hero = payload.data.UserDetails[i].HeroName;
                                            playersArray.push(player);
                                        }
                                    } else {
                                        player.username = payload.data.UserDetails[i].Nickname;
                                        player.kills = payload.data.UserDetails[i].HeroLastHits;
                                        player.towerLastHits = payload.data.UserDetails[i].TowerLastHits;
                                        player.deaths = payload.data.UserDetails[i].Deaths;
                                        player.assists = payload.data.UserDetails[i].Assists;
                                        player.heroLevel = payload.data.UserDetails[i].Level;
                                        player.team = payload.data.UserDetails[i].Team;
                                        player.hero = payload.data.UserDetails[i].HeroName;
                                        playersArray.push(player);
                                    }
                                });
                                // Bind the user ids for each player
                                playersArray.forEach(function(player, i) {
                                    player.accountId = data.users[i];
                                });
                                // Add bots back into array
                                playersArray = playersArray.concat(botsArray);
                                matchDetails.players = playersArray;

                                if(!coop_ai && !solo_ai) { // If not a bot game, parse it
                                    // Check for MMR
                                    //resolve(matchDetails);
                                    this.getPlayersElo(playersArray, this.replayId).then(function(playersWithElo) {
                                        matchDetails.players = playersWithElo;
                                        //Logger.writeToConsole('[REPLAY] Successfully got players current ELO for this game.'.green);
                                        resolve(matchDetails);
                                    }, function(err) {
                                        //Logger.writeToConsole('[REPLAY] Failed to get players ELO: '.red + err);
                                        Logger.writeToConsole('[REPLAY] Failed to get players ELO: '.red + this.replayId);
                                        this.queueManager.failed(this);
                                    }.bind(this));
                                } else {
                                    reject(true);
                                }
                            }
                        } else {
                            reject(false);
                        }
                    }.bind(this)).catch(function(err) {
                        var error = 'Error in getPlayersAndGameType: ' + JSON.stringify(err);
                        this.queueManager.failed(this);
                        reject(false);
                    }.bind(this));
                }
            } else {
                reject(false);
            }
        }.bind(this));
    }.bind(this));
};

/*
 * TYPE: POST
 * EP: /api/v1/parser/getPlayersElo
 * Params: Array of players
 */

Replay.prototype.getPlayersElo = function(players, matchId) {
    // TODO Migrate all requestify requests to request lib as its much more lightweight
    var url = process.env.PGG_HOST + '/api/v1/parser/getPlayersElo';
    return new Promise(function(resolve, reject) {
        var options = {
            url: url,
            method: 'POST',
            json: true,
            body: { players: players, matchId: matchId },
            headers: {
                'Content-Type': 'application/json'
            }
        };
        request(options, function(err, response, body) {
            if(err) {
                //Logger.writeToConsole('Error: When getting player elo for match: '.red + matchId);
                Logger.writeToConsole('Error: When getting player elo for match: '.red + matchId);
                reject(err);
            } else {
                if(typeof body !== 'undefined' && body && body.length > 0) {
                    //Logger.writeToConsole('response was: ', response.body);
                    if(Object.prototype.toString.call(response.body) === '[object Array]') {
                        var newPlayers = [];
                        players.forEach(function(player) {
                            response.body.some(function(playerElo) {
                                if(playerElo.accountId === player.accountId) {
                                    player.elo = playerElo.elo;
                                    newPlayers.push(player);
                                    return true;
                                }
                                return false;
                            });
                        });
                        resolve(newPlayers);
                    } else {
                        //Logger.writeToConsole('[REPLAY] Response body was not an array, instead it was: '.red, response.body);
                        Logger.writeToConsole('[REPLAY] Response body was not an array, instead it was'.red, response.body);
                        reject();
                    }
                } else {
                    //Logger.writeToConsole('Error: No data sent back from server in body: '.red + matchId);
                    Logger.writeToConsole('[REPLAY] Error: No data sent back from server in body: '.red + matchId);
                    reject('[REPLAY] Could not get player Elo for match: ' + matchId);
                }
            }
        });
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /api/v1/parser/endMatch/{id}
 *
 * Tells PGG that the match has finished and to create a job to calculate
 * the players new ELO
 */

Replay.prototype.endMatch = function() {

    var url = process.env.PGG_HOST + '/api/v1/parser/endMatch/' + this.replayId;
    //Logger.writeToConsole('Match ended, sending GET request to: ' + url);
    requestify.get(url).then(function(response) {
         //Logger.writeToConsole('Sent request to update player ELO:', response);
    }, function(err) {
        //Logger.writeToConsole('Error when match ended when trying to calculate new ELO: '.red, err);
        Logger.writeToConsole('Error when match ended when trying to calculate new ELO: '.red, err);
    });

};

/*
 * TYPE: GET
 * EP: /replay/v2/event/{streamId}_replay_details
 *
 * Hits the /users endpoint to update the players KDA
 */
Replay.prototype.updatePlayerStats = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';

    if(this.replayJSON.gameType !== 'solo_ai' && this.replayJSON.gameType !== 'coop_ai') {
        return requestify.get(url).then(function(response) {
            if (typeof response.body !== 'undefined' && response.body.length > 0) {
                var data = JSON.parse(response.body);
                if(data.hasOwnProperty('UserDetails')) {
                    var startTime = this.replayJSON.previousCheckpointTime;
                    var endTime = this.replayJSON.latestCheckpointTime;

                    return this.getHeroDamageAtCheckpoint(startTime, endTime).then(function(allPlayerDamage) {
                        var newPlayers = this.replayJSON.players.map(function(player, i) {
                            var playerDamage = Replay.getDamageForPlayer(player, allPlayerDamage);

                            if(this.replayJSON.gameType === 'coop_ai' || this.replayJSON.gameType === 'solo_ai') {
                                var playerData = null;
                                for(var j = 0; j < data.UserDetails.length; j++) {
                                    if(player.username === data.UserDetails[j].Nickname) {
                                        playerData = data.UserDetails[j];
                                    }
                                }
                                player.kills = playerData.HeroLastHits;
                                player.towerLastHits = playerData.TowerLastHits;
                                player.deaths = playerData.Deaths;
                                player.assists = playerData.Assists;
                                player.heroLevel = playerData.Level;
                            } else {
                                player.kills = data.UserDetails[i].HeroLastHits;
                                player.towerLastHits = data.UserDetails[i].TowerLastHits;
                                player.deaths = data.UserDetails[i].Deaths;
                                player.assists = data.UserDetails[i].Assists;
                                player.heroLevel = data.UserDetails[i].Level;
                            }

                            player.damageToHeroes = playerDamage.damageToHeroes;
                            player.damageToTowers = playerDamage.damageToTowers;
                            player.damageToMinions = playerDamage.damageToMinions;
                            player.damageToInhibitors = playerDamage.damageToInhibitors;
                            player.damageToHarvesters = playerDamage.damageToHarvesters;
                            player.damageToJungle = playerDamage.damageToJungle;

                            return player;
                        }.bind(this));
                        return Promise.all(newPlayers);
                    }.bind(this), function(err) {
                        var error = 'Error in getPlayersAndGameType: ' + JSON.stringify(err);
                    }.bind(this));
                }
            }
        }.bind(this)).catch(function(err) {
            var error = 'Error in updatePlayerStats: ' + JSON.stringify(err);
            this.queueManager.failed(this);
        }.bind(this));
    } else {
        // Caller expects a promise to be returned, resolve with null so we can continue
        // execution as normal
        return new Promise(function(resolve, reject) {
            resolve(null);
        });
    }
};

Replay.prototype.getHeroDamageAtCheckpoint = function(time1, time2) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=damage&time1=' + time1 + '&time2=' + time2;
    return requestify.get(url).then(function(response) {
        var allDamage = null;
        if(typeof response.body !== 'undefined' && response.body.length > 0) {
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
    }.bind(this)).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
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
        if(typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if(data.hasOwnProperty('DamageList')) {
                var damage = [];
                data.DamageList.forEach(function(damageEvent) {
                    if(damage.length === 0) {
                        damage.push({
                            username: damageEvent.DisplayName,
                            damageToHeroes: damageEvent.HeroDamageDealt,
                            damageToMinions: damageEvent.MinionDamageDealt,
                            damageToJungle: damageEvent.JungleMinionDamageDealt,
                            damageToTowers: damageEvent.TowerDamageDealt,
                            damageToHarvesters: damageEvent.RigDamageDealt,
                            damageToInhibitors: damageEvent.InhibitorDamageDealt
                        });
                    } else {
                        var found = false;
                        var modIndex = -1;
                        damage.some(function(event, i) {
                            if(damageEvent.DisplayName === event.username) {
                                modIndex = i;
                                found = true;
                            }
                            return found;
                        });
                        if(!found) {
                            damage.push({
                                username: damageEvent.DisplayName,
                                damageToHeroes: damageEvent.HeroDamageDealt,
                                damageToMinions: damageEvent.MinionDamageDealt,
                                damageToJungle: damageEvent.JungleMinionDamageDealt,
                                damageToTowers: damageEvent.TowerDamageDealt,
                                damageToHarvesters: damageEvent.RigDamageDealt,
                                damageToInhibitors: damageEvent.InhibitorDamageDealt
                            })
                        } else if(modIndex > -1) {
                            damage[modIndex].damageToHeroes += damageEvent.HeroDamageDealt;
                            damage[modIndex].damageToMinions += damageEvent.MinionDamageDealt;
                            damage[modIndex].damageToJungle += damageEvent.JungleMinionDamageDealt;
                            damage[modIndex].damageToTowers += damageEvent.TowerDamageDealt;
                            damage[modIndex].damageToHarvesters += damageEvent.RigDamageDealt;
                            damage[modIndex].damageToInhibitors += damageEvent.InhibitorDamageDealt;
                        }
                    }
                });
                return damage;
            }
        }
    }).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);

    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/event/{replay_id}_replay_details
 * Gets the current overall summary of the match events
 */

Replay.prototype.getReplaySummary = function() {
    var url = REPLAY_URL +'/replay/v2/event/' + this.replayId + '_replay_details';
    return requestify.get(url).then(function (response) {
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            return { code: 0, data: data };
        } else {
            return { code: 1 };
        }
    }).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
    }.bind(this));
};

/*
 * TYPE: GET
 * EP : /replay/v2/replay?user={replayId}
 * Gets info on the game, whether its live and the timestamp it started on
 */

Replay.prototype.isGameLive = function() {
    return new Promise(function(resolve, reject) {
        var url = REPLAY_URL + '/replay/v2/replay?user=' + this.replayId;
        requestify.get(url).then(function(response) {
            var body = response.getBody();
            if(body.hasOwnProperty('replays')) {
                resolve({ isLive: body.replays[0].bIsLive, startedAt: body.replays[0].Timestamp });
            } else {
                reject();
            }
        }).catch(function(err) {
            var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
            this.queueManager.failed(this);
            reject();
        }.bind(this));
    }.bind(this));
};

/*
 * Gets the tower kills and hero kills event feed at a specific given
 * check point
 */
Replay.prototype.getEventFeedForCheckpoint = function(time1, time2) {
    return new Promise(function(resolve, reject) {
        if(this.replayJSON.gameType !== 'solo_ai' && this.replayJSON.gameType !== 'coop_ai') {
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
        } else {
            resolve({ kills: [], towerKills: [] });
        }
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
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = data.events.map(function(event) {
                   return { killer: event.meta, timestamp: event.time1 };
                });
            }
        }
        cb(events);
    }).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=kills&time1=x&time2=y
 */

Replay.prototype.getHeroKillsAtCheckpoint = function(time1, time2) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=kills&time1=' + time1 + '&time2=' + time2;
    return requestify.get(url).then(function (response) {
        var events = [];
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = data.events.map(function(event) {
                    return this.getDataForHeroKillId(event.id).then(function(killInfo) {
                        return { killer: killInfo.killer, killed: killInfo.killed, timestamp: event.time1 };
                    }.bind(this));
                }.bind(this));
            }
        }
        return Promise.all(events);
    }.bind(this)).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
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
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('Killer')) {
                return { killer: data.Killer, killed: data.Killed};
            } else {
                reject();
            }
        }
    }).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
    }.bind(this));
};

/*
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=checkpoint
 * Gets a list of all current check points for this match
 *
 * CODE VALUES:
 * 0 = Another chunk should be taken after this one
 * 1 = Reschedule the scheduler for 3 minutes
 * 2 = There was an error, tell queue manager it failed!
 */

Replay.prototype.getNextCheckpoint = function(previousCheckpointTime) {
    var url = REPLAY_URL + '/replay/v2/replay/' + this.replayId + '/event?group=checkpoint';

    return requestify.get(url).then(function(response) {
        var latestCheckpointTime = response.getBody();
        if(latestCheckpointTime.hasOwnProperty('events') && latestCheckpointTime.events.length > 0) {
            latestCheckpointTime = latestCheckpointTime.events;

            // Set the JSON max check time directly
            this.maxCheckpointTime = latestCheckpointTime[latestCheckpointTime.length-1].time1;

            // Now set our current cp time
            var found = false;
            if(previousCheckpointTime > 0) {
                latestCheckpointTime.some(function(checkpoint) {
                    if(checkpoint.time1 > previousCheckpointTime) {
                        latestCheckpointTime = checkpoint.time1;
                        found = true;
                    }
                    return found;
                });
            } else {
                found = true;
                latestCheckpointTime = latestCheckpointTime[0].time1;
            }

            if(found) {
                //Logger.writeToConsole('new cp time is: '.green, latestCheckpointTime);
                return({ code: 0, previousCheckpointTime: previousCheckpointTime, currentCheckpointTime: latestCheckpointTime });
            } else {
                return({ code: 1 });
            }
        } else {
            return({ code: 2 });
        }
    }.bind(this)).catch(function(err) {
        var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        this.queueManager.failed(this);
    }.bind(this));
};

/*
 * Opens the file for this replay or creates it
 */

Replay.prototype.getFileHandle = function() {
    return new Promise(function(resolve, reject) {
        if(this.replayJSON !== null) {
            resolve();
        } else if(this.mongoconn !== null) {
            this.mongoconn.collection('matches').findOne({ replayId: this.replayId }, function(err, doc) {
                if(err && this.replayJSON === null) {
                    this.replayJSON = Replay.getEmptyReplayObject(this.replayId, this.checkpointTime);
                } else if(doc !== null) {
                    if(doc.replayId === this.replayId) {
                        this.replayJSON = doc;
                        delete this.replayJSON._id;
                    } else if(this.replayJSON === null) {
                        this.replayJSON = Replay.getEmptyReplayObject(this.replayId, this.checkpointTime);
                    }
                } else if(this.replayJSON === null && doc === null) {
                    this.replayJSON = Replay.getEmptyReplayObject(this.replayId, this.checkpointTime);
                }
                resolve();
            }.bind(this));
        } else {
            if(this.replayJSON === null) {
                this.replayJSON = Replay.getEmptyReplayObject(this.replayId, this.checkpointTime);
            }
            resolve();
        }

    }.bind(this));
};

/*
 * STATIC
 * This method gets a list of the 500 most recent games from the Epic API.
 * we only send back the Replay ID as we then create a Replay object for
 * each of these replays
 */

Replay.latest = function(flag, live, recordFrom) {
    var conn = new Connection();
    var url = REPLAY_URL + '/replay/v2/replay';
    if(typeof flag !== 'undefined' && flag !== null) {
        url += '?user=flag_' + flag;
    }
    if(typeof live !== 'undefined' && live !== null) {
        if(url.indexOf('?user=flag_') > -1) {
            url += '&live=' + live;
        } else {
            url += '?live=' + live;
        }
    }
    Logger.writeToConsole('[SCRAPER] Scraping url: '.yellow + url);
    return new Promise(function(resolve, reject) {
        var data = null;
        var isLive = live === 'true' ? 1 : 0;
        requestify.get(url).then(function (response) {
            if (typeof response.body !== 'undefined' && response.body.length > 0) {
                data = JSON.parse(response.body);
                if (data.hasOwnProperty('replays')) {
                    var VALUES = '';
                    data.replays.forEach(function (replay) {
                        if(new Date(replay.Timestamp) >= recordFrom) {
                            // any new items have the highest priority
                            VALUES += "('" + replay.SessionName + "', " + isLive + ", 4), ";
                        }
                    });
                    VALUES = VALUES.substr(0, VALUES.length - 2);
                    if(VALUES !== '') {
                        var query = 'INSERT IGNORE INTO queue (replayId, live, priority) VALUES ' + VALUES;
                        conn.query(query, function() {});

                        resolve(data.replays);
                    } else {
                        reject('No valid replays');
                    }
                }
                reject('0 Replays were on the endpoint');
            } else {
                reject('The body had no replay data.');
            }
        }).catch(function(err) {
            var error = 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
            this.queueManager.failed(this);
            reject();
        }.bind(this));
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
    if(typeof allPlayerDamage[0] !== 'undefined' && allPlayerDamage[0].length > 0) {
        allPlayerDamage[0].some(function (damageData) {
            if (damageData.username === player.username) {
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
    }
    return playerDamage;
};

/*
 * STATIC
 * Return a object which contains the empty JSON structure for a Replay object
 */

Replay.getEmptyReplayObject = function(replayId, checkpointTime) {
    this.attempts = 0;
    this.checkpointTime = 0;
    return {
        replayId: replayId,
        startedAt: null,
        endedAt: null,
        isFeatured: false,
        previousCheckpointTime: checkpointTime,
        latestCheckpointTime: 0,
        gameLength: 0,
        isLive: true, // pertains to Final / Active
        gameType: null, // get from /replay/{streamId}/users -- if flag_pvp = pvp, flag_coop = bot, flag_custom = custom
        players: [],
        playerKills: [], //{ killer: 'bobby', killed: 'jane', timestamp: '' }
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
        elo: 0
    }
};

/*
 * STATIC
 * Checks if a bot was in the game
 */

Replay.isBot = function(playerName) {
    switch(playerName.toUpperCase()) {
        case 'BLUE_DEKKER': return true; case 'RED_DEKKER': return true;
        case 'BLUE_FENG MAO': return true; case 'RED_FENG MAO': return true;
        case 'BLUE_GRIM.EXE': return true; case 'RED_GRIM.EXE': return true;
        case 'BLUE_GADGET': return true; case 'RED_GADGET': return true;
        case 'BLUE_GIDEON': return true; case 'RED_GIDEON': return true;
        case 'BLUE_GREYSTONE': return true; case 'RED_GREYSTONE': return true;
        case 'BLUE_GRUX': return true; case 'RED_GRUX': return true;
        case 'BLUE_HOWITZER': return true; case 'RED_HOWITZER': return true;
        case 'BLUE_IGGY & SCORCH': return true; case 'RED_IGGY & SCORCH': return true;
        case 'BLUE_KALLARI': return true; case 'RED_KALLARI': return true;
        case 'BLUE_KHAIMERA': return true; case 'RED_KHAIMERA': return true;
        case 'BLUE_MURDOCK': return true; case 'RED_MURDOCK': return true;
        case 'BLUE_MURIEL': return true; case 'RED_MURIEL': return true;
        case 'BLUE_RAMPAGE': return true; case 'RED_RAMPAGE': return true;
        case 'BLUE_RIKTOR': return true; case 'RED_RIKTOR': return true;
        case 'BLUE_SEVAROG': return true; case 'RED_SEVAROG': return true;
        case 'BLUE_SPARROW': return true; case 'RED_SPARROW': return true;
        case 'BLUE_STEEL': return true; case 'RED_STEEL': return true;
        case 'BLUE_TWINBLAST': return true; case 'RED_TWINBLAST': return true;
        case 'BLUE_FEY': return true; case 'RED_FEY': return true;
        default: return false;
    }
};

module.exports = Replay;