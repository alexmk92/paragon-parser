var requestify = require('requestify');
var http = require('http');
var fs = require('fs');
var Logger = require('./Logger');
var Connection = require('./Connection');

var REPLAY_URL = 'https://orionreplay-public-service-prod09.ol.epicgames.com';

/**
 * @Replay :
 * ---------
 * Replay object manages which chunk of data we want to stream from the endpoint
 * it has one static method called latest which returns a list of replay ids
 *
 * NOTE:
 * -----
 * All methods that make a request to Epics API will return a Promise, this is because
 * each stage in the processing pipeline depends on the previous event to finish, it wouldn't make sense
 * to process chunk B whilst chunk A is still processing, otherwise data like totalDamage done, or our
 * event feed could become corrupt. Promises allow us to handle all transactions autonomously by processing
 * each chunk sequentially.   Due to nodes non-blocking nature this we can use Promises to callback when
 * each stage is done before processing the rest of the replay without interrupting the
 * executuon of other process workers.
 *
 * @param {object} db - Reference to the Mongo Connection object so we can query inside of this class
 * @param {string} replayId - Id of the replay we are processing to make requests to epic API
 * @param {number} checkpointTime - The time we should start processing this replay from to save on network requests
 * this can be set from a previous worker so that we don't need to fully process all the chunks again
 * @param {number} attempts - The number of failed attempts, if this is too high we remove it from the queue
 * @queue {object} queue - Reference to the parent @OldQueue.js which manages this Replay object
 */

var Replay = function(db, replayId, checkpointTime, attempts, queue) {
    this.mongoconn = db;
    this.replayId = replayId;
    this.replayJSON = null;
    this.maxCheckpointTime = 0;
    this.checkpointTime = 0;
    this.queryAttempts = 0; // Used for when there is a Deadlock in SQL
    this.attempts = attempts;

    this.queueManager = queue;
};

/**
 * @parseDataAtCheckpoint :
 * ------------------------
 * This function is the main brains of the Replay object, it calls itself recursively until all of
 * the replay chunks have been processed, it does this by checking the game current state and compares
 * the previous checkpointTime to the next checkpointTime it gets from an API request from Epic.
 *
 * If this replay has been servied by another queue before this Replay's checkpointTime will be > 0
 * and will start processing chunks where the old replay left off, saving network request cycles, thus
 * improving the performance of the application.
 *
 * If the checkpointTime's match and the game is live it will reschedule this replay to run on @Queue
 * in 1 minute, otherwise it will tell @Queue to remove the item from the queue and upload it for
 * final processing if all chunks were processed with no errors.
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

                if(checkpoint.code === 2) {
                    // this happens when no checkponint data is found
                    this.attempts++;
                    if(this.attempts > 10) {
                        return this.queueManager.removeDeadReplay(this);
                    } else {
                        // TODO: Refactor this in future so its cleaner
                        // We check 5 times (5 minutes) to see if any events have happened, if not increment its failed attempts again
                        if(this.attempts <= 6) {
                            this.getPlayersAndGameType().then(function(matchInfo) {
                                this.replayJSON.players = matchInfo.players;
                                this.replayJSON.gameType = matchInfo.gameType;
                                this.replayJSON.isFeatured = matchInfo.isFeatured;
                                //Logger.writeToConsole('[REPLAY] Replay: '.yellow + this.replayId + ' has no checkpoint data yet, but has been uploaded with empty stats: '.yellow + this.replayId);
                                Logger.writeToConsole('[REPLAY] Replay: '.yellow + this.replayId + ' has no checkpoint data yet, but has been uploaded with empty stats'.yellow);
                                return this.queueManager.schedule(this, 90);
                            }.bind(this), function(isBotGame) {
                                if(isBotGame) {
                                    return this.queueManager.removeBotGame(this);
                                } else {
                                    return this.queueManager.failed(this, 'Unknown error when parsing a PVP replay with no checkpoint data');
                                }
                            }.bind(this));
                        } else {
                            return this.queueManager.failed(this, 'Failed attempts > 6, failing replay');
                        }
                    }
                } else if(checkpoint.code === 1 && data.isLive === true) {
                    // Get all events as its a live game
                    this.getEventFeedForCheckpoint(this.replayJSON.latestCheckpointTime, this.replayJSON.latestCheckpointTime).then(function() {
                        this.updatePlayerStats().then(function(newPlayers) {
                            if(newPlayers !== null) {
                                this.replayJSON.players = newPlayers;
                            }
                            return this.queueManager.schedule(this, 30);
                        }.bind(this));
                    }.bind(this));
                } else {
                    if(checkpoint.code === 0) {
                        // Update the file with the new streaming data
                        if(typeof this.replayJSON.players !== 'undefined' && this.replayJSON.players.length === 0) {
                            this.getPlayersAndGameType().then(function(matchInfo) {
                                this.replayJSON.players = matchInfo.players;
                                this.replayJSON.gameType = matchInfo.gameType;
                                this.replayJSON.isFeatured = matchInfo.isFeatured;

                                this.getEventFeedForCheckpoint(checkpoint.previousCheckpointTime, checkpoint.currentCheckpointTime).then(function() {
                                    this.updatePlayerStats().then(function(newPlayers) {
                                        if(newPlayers !== null) {
                                            this.replayJSON.players = newPlayers;
                                        }
                                        this.parseDataAtCheckpoint();
                                    }.bind(this));
                                }.bind(this));
                            }.bind(this), function(isBotGame) {
                                if(isBotGame) {
                                    return this.queueManager.removeBotGame(this);
                                } else {
                                    return this.queueManager.failed(this, 'Unknown erorr processing a PVP game on line 143');
                                }
                            }.bind(this));
                        } else {
                            this.getEventFeedForCheckpoint(checkpoint.previousCheckpointTime, checkpoint.currentCheckpointTime).then(function() {
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
                            var ms = new Date(this.replayJSON.startedAt).getTime();
                            this.replayJSON.endedAt = new Date(ms + matchResult.gameLength);

                            var diff = (new Date() - this.replayJSON.endedAt) / 60000;

                            // If 2 mins has already parsed or we finished processing, get the final chunks
                            if(this.replayJSON.scheduledBeforeEnd == true || diff > 2) {
                                this.getEventFeedForCheckpoint(this.replayJSON.previousCheckpointTime, this.replayJSON.latestCheckpointTime).then(function() {
                                    this.updatePlayerStats().then(function(newPlayers) {
                                        if(newPlayers !== null) {
                                            this.replayJSON.players = newPlayers;
                                        }
                                        this.replayJSON.winningTeam = matchResult.winningTeam;
                                        this.replayJSON.gameLength = matchResult.gameLength;
                                        this.replayJSON.isLive = false;
                                        this.replayJSON.previousCheckpointTime = this.replayJSON.latestCheckpointTime;

                                        //this.endMatch();
                                        return this.queueManager.removeItemFromQueue(this);
                                    }.bind(this));
                                }.bind(this));
                            } else {
                                // 3 mins hasn't parsed, we're still streaming on PGG, therefore we need to hit this every 30 seconds for a max of 8 times to get all chunks
                                this.replayJSON.isLive = true;
                                // Hit this 8 times (2 minutes 30 seconds) so we get the fully parsed replay on a live replay
                                if(this.replayJSON.endChunksParsed > 5) {
                                    this.replayJSON.scheduledBeforeEnd = true;
                                    this.parseDataAtCheckpoint();
                                } else {
                                    this.replayJSON.endChunksParsed++;
                                    this.getEventFeedForCheckpoint(this.replayJSON.previousCheckpointTime, this.replayJSON.latestCheckpointTime).then(function() {
                                        this.updatePlayerStats().then(function(newPlayers) {
                                            if(newPlayers !== null) {
                                                this.replayJSON.players = newPlayers;
                                            }
                                            return this.queueManager.schedule(this, 30);
                                        }.bind(this));
                                    }.bind(this));
                                }
                            }
                        }.bind(this), function(err) {
                            //Logger.writeToConsole('[REPLAY] Error when getting match result: '.red + err);
                            Logger.writeToConsole('[REPLAY] Error when getting match result for replay: '.red + this.replayId);
                            return this.queueManager.failed(this, 'Error when getting match result for replay');
                        }.bind(this));
                    }
                }
            }.bind(this)).catch(function(err) {
                var error = 'Error in parseDataAtNextCheckpoint line 207: ' + JSON.stringify(err);
                return this.queueManager.failed(this, error);
            }.bind(this));
        }.bind(this), function(httpStatus) {
            if(httpStatus === 404) {
                Logger.writeToConsole('[REPLAY] The replay: '.red + this.replayId + ' has expired.'.red);
                return this.queueManager.removeItemFromQueue(this);
            } else {
                Logger.writeToConsole('[REPLAY] Failed as a http status of: '.red + httpStatus + ' was returned for replay '.red + this.replayId);
                return this.queueManager.failed(this, 'Http status ' + http + ' returned');
            }
        }.bind(this));
    }.bind(this));
};

/**
 * @getMatchResult :
 * -----------------
 * Type: GET
 * EndpointA: /replay/v2/event/{replayId}_replay_details
 * EndpointB: /replay/v2/replay?user={replayId}
 *
 * Gets the result of the match, this will even resolve the promise and send back the
 * games total time in ms as well as which team won.  This method will only be called
 * if this.replayJSON.previousCheckpointTime is equal to this.replayJSON.latestCheckpointTime
 * and if this.replayJSON.isLive is true.
 *
 * If this call succeeds, the returned promise is resolved and will result in @Queue.removeItemFromQueue
 * otherwise @Queue.failed will be called and the replay shall be reprocessed by another queue worker.
 *
 * @return {promise}
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
            return this.queueManager.failed(this, error);
        }.bind(this));
    });
};

/**
 * @getPlayersAndGameType :
 * ------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/users
 *
 * Gets all players in the game as well as the game type (PVP, COOP_AI, AI, CUSTOM), this
 * allows us to determine if we should continue processing the game (we remove this
 * replay from the Queue if its a bot game is its a waste of processing).
 *
 * We also store the players in this.replayJSON.players and send a request to @getPlayersElo
 * so that the players will be saved to Mongo along with this match to allow us to process
 * their ELO.
 *
 * Once this job finishes we return a promise that resolves with an array of players and
 * the game type.
 *
 * @return {promise}
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
                                    resolve(matchDetails);
                                } else {
                                    reject(true);
                                }
                            }
                        } else {
                            reject(false);
                        }
                    }.bind(this)).catch(function(err) {
                        var error = 'Error in getPlayersAndGameType: ' + JSON.stringify(err);
                        return this.queueManager.failed(this, error);
                        //reject(false);
                    }.bind(this));
                }
            } else {
                reject(false);
            }
        }.bind(this));
    }.bind(this));
};

/**
 * @updatePlayersStats :
 * ---------------------
 * Type: GET
 * Endpoint: /replay/v2/event/{streamId}_replay_details
 *
 * Hits the Epic API endpoint and updates the players stats such as damage, kills,
 * deaths and assists.
 *
 * @return {promise}
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
            return this.queueManager.failed(this, error);
        }.bind(this));
    } else {
        // Caller expects a promise to be returned, resolve with null so we can continue
        // execution as normal
        return new Promise(function(resolve, reject) {
            resolve(null);
        });
    }
};

/**
 * @getHeroDamageAtCheckpoint :
 * ----------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/event?group=damage&time1={time1}&time2={time2}
 *
 * Gets all damage events for all heroes between two checkpoint time stamps, when the promise
 * is resolved it is processed in @updatePlayerStats, this call will make calls to
 * @getDamageForCheckpointId which will process all damage which happened between
 * timestamps 1 and 2, this will normally only result in one checkpoint being processed
 * as we only query in 1 minute chunks.
 *
 * @param {number} time1 - The time in ms to start scraping chunks from
 * @param {number} time2 - The time in ms to stop scraping chunks from
 * @return {promise}
 */

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
        var error = 'Error in getHeroDamageAtCheckpoint: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @getDamageForCheckpointId :
 * ---------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/event/{eventId}
 *
 * Gets all damage for the specific event called from @getHeroDamageAtCheckpoint
 *
 * @param {string} eventId - The id of the checkpoint event we want to get damage for
 *
 * @return {promise}
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
        var error = 'Error in getDamageForCheckpointId: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);

    }.bind(this));
};

/**
 * @getReplaySummary :
 * -------------------
 * Type: GET
 * Endpoint: /replay/v2/event/{replayId}_replay_details
 *
 * Gets a list of all players in the game and the current winning team.  The promise
 * resolves with codes:
 *
 * 0 = There were no errors, we return a data key on the resolved payload
 * 1 = There was no data, fail this replay and re-serve it in 2 minutes
 *
 * @return {promise}
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
        var error = 'Error in getReplaySummary: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @isGameLive :
 * -------------
 * Type: GET
 * Endpoint: /replay/v2/replay?user={replayId}
 *
 * Gets info on the game, whether its live and the timestamp it started on
 *
 * @return {promise}
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
            // On fail, try to get players before failing the replay - this is so we don't end up uploading empty replays
            this.getPlayersAndGameType().then(function(matchInfo) {
                this.replayJSON.isLive = true;
                this.replayJSON.players = matchInfo.players;
                this.replayJSON.gameType = matchInfo.gameType;
                this.replayJSON.isFeatured = matchInfo.isFeatured;
                Logger.writeToConsole('[REPLAY] Replay: '.yellow + this.replayId + ' has no checkpoint data yet, but has been uploaded with empty stats'.yellow);
                return this.queueManager.schedule(this, 90);
            }.bind(this), function(isBotGame) {
                if(isBotGame) {
                    return this.queueManager.removeBotGame(this);
                } else {
                    var error = 'Error in isGameLive: ' + JSON.stringify(err);
                    return this.queueManager.failed(this, error);
                }
            }.bind(this));
        }.bind(this));
    }.bind(this));
};

/**
 * @getEventFeedForCheckpoint :
 * ----------------------------
 * Gets the tower and player kills between two checkpoint times. The promise will
 * resolve with an object containing { towerKills : {Array}, kills : {Array} }
 *
 * @param {number} time1 - The first checkpoint time we want to start getting events from
 * @param {number} time2 - The second checkpoint time we want to end getting events from
 *
 * @return {promise}
 */


Replay.prototype.getEventFeedForCheckpoint = function(time1, time2) {
    time2 = time2 + 210000; // Make sure we get event that could be in the future as the API doesn't schedule these, therefore we look 3.5 min in future extra
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
                    if(eventFeed.towerKills.length !== 0 || eventFeed.kills.length !== 0) {
                        eventFeed.towerKills.forEach(function (towerKill) {
                            var found = false;
                            if (this.replayJSON.towerKills.length > 0) {
                                this.replayJSON.towerKills.some(function (tk) {
                                    found = (towerKill.killer === tk.killer && towerKill.timestamp === tk.timestamp);
                                    return found;
                                });
                            }
                            if (!found) this.replayJSON.towerKills.push(towerKill);
                        }.bind(this));

                        eventFeed.kills.forEach(function (kill) {
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
                    resolve();
                }.bind(this));
            }.bind(this));
        } else {
            resolve();
        }
    }.bind(this));
};

/**
 * @getTowerKillsAtCheckpoint :
 * ----------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/event?group=towerKills&time1={time1}&time2={time2}
 *
 * Called from @getEventFeedAtCheckpoint and gets all tower kill events between the two
 * checkpoint times passed down, the promise resolves with an array of objects containing
 * the timestamp of the kill, and the name of the killer allowing us to generate a live
 * event feed on the PGG site.
 *
 * If the request fails, we called @Queue.failed to remove the replay, the execution of this
 * process will stop and be collected by the GC.
 *
 * @param {number} time1 - The first checkpoint time we want to start getting tower kills from
 * @param {number} time2 - The second checkpoint time we want to end getting tower kills from
 * @param {function} callback - Callback to determine when all events have been processed, this allows
 * the promise in @getEventFeedAtCheckpoint to resolve when all events are completed
 */

Replay.prototype.getTowerKillsAtCheckpoint = function(time1, time2, callback) {
    var url = REPLAY_URL +'/replay/v2/replay/' + this.replayId + '/event?group=towerKills&time1=' + time1 + '&time2=' + time2;
    var replay = this.replayJSON;
    requestify.get(url).then(function (response) {
        var events = [];
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = [];
                if(this.replayJSON.towerKills.length === 0) {
                    events = data.events.map(function(event) {
                        return { killer: event.meta, timestamp: event.time1 };
                    });
                } else {
                    data.events.forEach(function(event) {
                        var found = false;
                        this.replayJSON.towerKills.some(function(towerEvent) {
                            found = towerEvent.timestamp === event.time1 && towerEvent.killer === event.meta;
                            return found;
                        });
                        if(!found) {
                            events.push({ killer: event.meta, timestamp: event.time1 });
                        }
                    }.bind(this));
                }
            }
        }
        return callback(events);
    }.bind(this)).catch(function(err) {
        var error = 'Error in getTowerKillsAtCheckpoint: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @getTowerKillsAtCheckpoint :
 * ----------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/event?group=kills&time1={time1}&time2={time2}
 *
 * Called from @getEventFeedAtCheckpoint and gets all player kill events between the two
 * checkpoint times passed down, the promise resolves with an array of objects containing
 * the timestamp of the kill, and the name of the killers name and the name of the
 * person who was killed allowing us to generate a live event feed on the PGG site.
 *
 * If the request fails, we called @Queue.failed to remove the replay, the execution of this
 * process will stop and be collected by the GC.
 *
 * @param {number} time1 - The first checkpoint time we want to start getting tower kills from
 * @param {number} time2 - The second checkpoint time we want to end getting tower kills from
 *
 * @return {promise}
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
        var error = 'Error in getHeroKillsAtCheckpoint: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @getDataForHeroKillId :
 * -----------------------
 * Type: GET
 * Endpoint: /replay/v2/replay/{replayId}/event/{killId}
 *
 * Called from @getHeroKillsAtCheckpoint, this takes the id of a kill and gets the name of the
 * killer and the name of the killed person.  The timestamp is recorded in @getHeroKillsAtCheckpoint
 *
 * If the promise rejects then @getHeroKillsAtCheckpoint will fail and remove the replay from the
 * queue, this will also happen if requestify can't hit the endpoint.
 *
 * @param {number} id - The id of the kill event
 *
 * @return {promise}
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
        var error = 'Error in getDataForHeroKillId: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @getNextCheckpoint :
 * --------------------
 * TYPE: GET
 * EP: /replay/v2/replay/{replayId}/event?group=checkpoint
 *
 * Gets a list of all current check points for this match, we can then compare these to
 * our previously recorded max checkpoint time and determine if the game needs to be rescheduled
 * for later, or if it has been completed and needs to be uploaded to mongo and removed
 * from the queue.
 *
 * The promise resolve with one of the following codes:
 *
 * CODE VALUES:
 * 0 = Once we finish processing this chunk, get the next chunk and check if we're at the end of processing
 * 1 = Reschedule the work on this item for 1 minute, this worker will be disposed and put back on the queue
 * 2 = There was an error, tell queue manager it failed, remove from mongo and reschedule if we have enough attempts
 *
 * @param {number} previousCheckpointTime - The previous recorded match time in ms
 *
 * @return {promise}
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
        var error = 'Error in getNextCheckpoint: ' + JSON.stringify(err);
        return this.queueManager.failed(this, error);
    }.bind(this));
};

/**
 * @getFileHandle :
 * ----------------
 * This method will resolve a promise that holds the current state of the replay object for the
 * given replayId.  If it hasn't been processed before an empty replay object is returned,
 * otherwise if we can find a record in mongo its contents will be loaded into replay JSON ready
 * for parsing.
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

/**
 * @getFileHandle (static method):
 * -------------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay?user=flag_{flag}&live={live}
 *
 * This method gets a list of the 500 most recent games from the Epic API.
 * we only send back the Replay ID as we then create a Replay object for
 * each of these replays, optional flags are supplied by the user to
 * filter the response from the api.
 *
 * On response from the API, this method will insert rows that do not exist
 * in the database, by using an UNIQUE key constraint on replayId
 *
 * @param {string} flag - Either 'custom' or 'featured' to filter by game types
 * @param {boolean} live - Either true or false to filter by live games
 * @param {date} recordFrom - Specify the oldest date we can scrape games from
 *
 * @return {promise} - Rejects if there were any errors, else returns the array of rows which
 * were inserted
 */

Replay.latest = function(flag, live, recordFrom) {
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
                    var SELECT_STRING = '';
                    var VALUES = '';
                    var replays = [];

                    data.replays.forEach(function (replay) {
                        if(new Date(replay.Timestamp) >= recordFrom) {
                            // any new items have the highest priority
                            SELECT_STRING += '"' + replay.SessionName + '", ';
                            VALUES += "('" + replay.SessionName + "', " + isLive + ", 3), ";
                            replays.push(replay);
                        }
                    });
                    SELECT_STRING = SELECT_STRING.substr(0, SELECT_STRING.length -2);
                    if(SELECT_STRING !== '') {
                        var query = 'SELECT replayId FROM queue WHERE replayId IN (' + SELECT_STRING + ')';
                        var conn = new Connection();
                        conn.query(query, function(rows) {
                            conn = new Connection();
                            if(typeof rows !== 'undefined' && rows) {
                                // Reset the values string
                                if(rows.length > 0) {
                                    VALUES = '';
                                    replays.forEach(function(replay) {
                                        var found = false;
                                        rows.some(function(row) {
                                            found = row.replayId === replay.SessionName;
                                            return found;
                                        });
                                        if(!found) {
                                            VALUES += "('" + replay.SessionName + "', " + isLive + ", 3), ";
                                        }
                                    });
                                }
                                if(VALUES !== '') {
                                    VALUES = VALUES.substr(0, VALUES.length - 2);
                                    query = 'INSERT INTO queue (replayId, live, priority) VALUES ' + VALUES;
                                    conn.query(query, function(rows) {
                                        if(typeof rows !== 'undefined' && rows && rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
                                            Logger.writeToConsole('Inserted: '.green + rows.affectedRows + ' replays'.green);
                                        }
                                    });
                                    resolve(rows);
                                } else {
                                    conn.end();
                                    reject('No valid replays');
                                }
                            } else {
                                conn.end();
                            }
                        });
                    }
                }
                reject('0 Replays were on the endpoint');
            } else {
                reject('The body had no replay data.');
            }
        }).catch(function(err) {
            var error = 'Error in latest: ' + JSON.stringify(err);
            return this.queueManager.failed(this, error);
        }.bind(this));
    });
};

/**
 * @getDamageForPlayer (static method):
 * ------------------------------------
 * Type: GET
 * Endpoint: /replay/v2/replay?user=flag_{flag}&live={live}
 *
 * Calculates damage at a specific index based on the given player
 *
 * @param {object} player - The player who's new damage we want to compute
 * @param {Array} allPlayerDamage - The array of players so we can find the existing players current damage
 * and accumulate it by adding the new damage
 *
 * @return {object} - Returns the new players damage array so we can update the original array on the caller
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

/**
 * @getEmptyReplayObject (static method):
 * --------------------------------------
 * Returns an empty replay object for new or failed replays
 *
 * @param {string} replayId - The id of the replay to be processed
 * @param {number} checkpointTime - The checkpoint time to start from
 *
 * @return {object} - Returns replay object with the set replayId and checkpointTime
 */

Replay.getEmptyReplayObject = function(replayId, checkpointTime) {
    this.attempts = 0;
    this.checkpointTime = 0;
    return {
        replayId: replayId,
        startedAt: null,
        endedAt: null,
        scheduledBeforeEnd: false,
        endChunksParsed: 0, // The replay is 3 mins behind, we have to parse 3 more times to be up to date with the replay and get all events
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

/**
 * @getEmptyPlayerObject (static method):
 * --------------------------------------
 * Returns an empty player object for each player in game
 *
 * @return {object} - Returns the empty player object
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