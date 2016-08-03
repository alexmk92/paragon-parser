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

var Replay = function(replayId, checkpointTime, queue) {
    this.replayId = replayId;
    this.scheduledTime = new Date();
    this.replayJSON = null;
    this.checkpointTime = 0;
    this.isRunningOnQueue = false;
    this.isScheduledInQueue = false; // flag to determine whether the Queue should increment the schedule property
    this.failed = false;

    this.queueManager = queue;
};

/*
 * Kicks off the stream from the constructor once the client program invokes it on this
 * specific instance
 */

Replay.prototype.parseDataAtCheckpoint = function() {

    if(this.failed || this.scheduledTime.getTime() > new Date().getTime()) {
        this.isRunningOnQueue = false;
        return;
    }

    // Get a handle on the old file:
    this.getFileHandle().then(function() {
        // We're no longer scheduled, we can now run this
        this.isScheduledInQueue = false;
        this.isRunningOnQueue = true;
        
        // Get the header and check if the game has actually finished
        // TODO Optimise so if the game status is false then we dont waste API requests
        this.isGameLive().then(function(data) {
            this.replayJSON.isLive = data.isLive;
            this.replayJSON.startedAt = new Date(data.startedAt);

            // Keep getting the latest check point
            this.getNextCheckpoint(this.replayJSON.newCheckpointTime).then(function(checkpoint) {
                if(typeof checkpoint.lastCheckpointTime !== 'undefined' && typeof checkpoint.currentCheckpointTime !== 'undefined') {
                    this.replayJSON.lastCheckpointTime = checkpoint.lastCheckpointTime;
                    this.replayJSON.newCheckpointTime = checkpoint.currentCheckpointTime;
                }

                var status = this.replayJSON.isLive ? 'ACTIVE' : 'FINAL';
                var query = 'UPDATE replays SET status="' + status + '", checkpointTime=' + this.replayJSON.newCheckpointTime + ' WHERE replayId="' + this.replayId + '"';
                conn.query(query, function() {
                    //console.log('updated the item');
                });
                //console.log(this.replayJSON.lastCheckpointTime);
                //console.log(this.replayJSON.currentCheckpointTime);

                if(checkpoint.code === 1 && data.isLive === true) {
                    // Schedule the queue to come back to this item in 1 minute
                    console.log('up to date');
                    this.queueManager.schedule(this, 45000);
                    Logger.append(LOG_FILE, new Date() + ' we are already up to date with the replay: ' + this.replayId + ', reserving this replay in 1 minute');
                } else {
                    if(checkpoint.code === 0) {
                        // Update the file with the new streaming data
                        if(typeof this.replayJSON.players !== 'undefined' && this.replayJSON.players.length === 0) {
                            this.getPlayersAndGameType(this.replayId).then(function(matchInfo) {
                                this.replayJSON.players = matchInfo.players;
                                this.replayJSON.gameType = matchInfo.gameType;

                                this.getEventFeedForCheckpoint(checkpoint.lastCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
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
                                        if(this.replayJSON.kills.length > 0) {
                                            this.replayJSON.kills.some(function(k) {
                                                found = kill.killer === k.killer && kill.timestamp === k.timestamp && kill.killed === k.killed;
                                                return found;
                                            });
                                        }
                                        if(!found) this.replayJSON.kills.push(kill);
                                    }.bind(this));

                                    this.updatePlayerStats().then(function(newPlayers) {
                                        this.replayJSON.players = newPlayers;
                                        fs.writeFileSync('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON));
                                        this.parseDataAtCheckpoint();
                                    }.bind(this));
                                }.bind(this));
                            }.bind(this));
                        } else {
                            this.getEventFeedForCheckpoint(checkpoint.lastCheckpointTime, checkpoint.currentCheckpointTime).then(function(events) {
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
                                    if(this.replayJSON.kills.length > 0) {
                                        this.replayJSON.kills.some(function(k) {
                                            found = kill.killer === k.killer && kill.timestamp === k.timestamp && kill.killed === k.killed;
                                            return found;
                                        });
                                    }
                                    if(!found) this.replayJSON.kills.push(kill);
                                }.bind(this));
                                this.updatePlayerStats().then(function(newPlayers) {
                                    this.replayJSON.players = newPlayers;
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
                                    Logger.append(LOG_FILE, err);
                                } else {
                                    this.queueManager.removeItemFromQueue(this);
                                    Logger.append(LOG_FILE, new Date() + ' finished processing replay: ' + this.replayId);
                                }
                            }.bind(this));
                        }.bind(this));
                    }
                }
            }.bind(this)).catch(function(err) {
                console.log('Error in parseDataAtNextCheckpoint: ', err);
                var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
                Logger.append(LOG_FILE, error);
                this.queueManager.failed(this);
            }.bind(this));
        }.bind(this), function(httpStatus) {
            if(httpStatus === 404) {
                Logger.append(LOG_FILE, "The replay id: " + this.replayId + " has expired.");
                this.queueManager.removeItemFromQueue(this);
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
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('WinningTeam')) {
                return data['WinningTeam'];
            }
        }
    }).catch(function(err) {
        var error = new Date() + 'Error in getMatchResult: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
        this.queueManager.failed(this);
    }.bind(this));
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
                                var solo_bot = false;
                                var coop_bot = false;

                                // Get the game type
                                for(var j = 0; j < data.users.length; j++) {
                                    if(data.users[j].toUpperCase().trim() === 'FLAG_CUSTOM') {
                                        custom = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_PVP') {
                                        pvp = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_FEATURED') {
                                        featured = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_SOLO') {
                                        solo_bot = true;
                                    } else if(data.users[j].toUpperCase().trim() === 'FLAG_COOP') {
                                        coop_bot = true;
                                    }
                                }
                                if(custom && featured) {
                                    matchDetails.gameType = 'CUSTOM FEATURED';
                                }  else if(pvp && custom) {
                                    matchDetails.gameType = 'CUSTOM PVP';
                                } else if(custom) {
                                    matchDetails.gameType = 'CUSTOM';
                                } else if(pvp && featured) {
                                    matchDetails.gameType = 'FEATURED PVP';
                                } else if(featured) {
                                    matchDetails.gameType = 'FEATURED';
                                } else {
                                    matchDetails.gameType = 'PVP';
                                }
                                if(solo_bot) {
                                    matchDetails.gameType = 'SOLO AI';
                                } else if(coop_bot) {
                                    matchDetails.gameType = 'COOP AI';
                                }

                                var playersArray = [];
                                var botsArray = [];

                                // Remove bots from master array and store in temp array
                                payload.data['UserDetails'].forEach(function(user, i) {
                                    var player = Replay.getEmptyPlayerObject();
                                    if(coop_bot || solo_bot) {
                                        if(Replay.isBot(user.Nickname)) {
                                            player.accountId = 'bot';
                                            player.username = payload.data['UserDetails'][i].Nickname;
                                            player.kills = payload.data['UserDetails'][i].HeroLastHits;
                                            player.towerLastHits = payload.data['UserDetails'][i].TowerLastHits;
                                            player.deaths = payload.data['UserDetails'][i].Deaths;
                                            player.assists = payload.data['UserDetails'][i].Assists;
                                            player.heroLevel = payload.data['UserDetails'][i].Level;
                                            player.team = payload.data['UserDetails'][i].Team;
                                            player.hero = payload.data['UserDetails'][i].HeroName;
                                            botsArray.push(player);
                                        } else {
                                            player.username = payload.data['UserDetails'][i].Nickname;
                                            player.kills = payload.data['UserDetails'][i].HeroLastHits;
                                            player.towerLastHits = payload.data['UserDetails'][i].TowerLastHits;
                                            player.deaths = payload.data['UserDetails'][i].Deaths;
                                            player.assists = payload.data['UserDetails'][i].Assists;
                                            player.heroLevel = payload.data['UserDetails'][i].Level;
                                            player.team = payload.data['UserDetails'][i].Team;
                                            player.hero = payload.data['UserDetails'][i].HeroName;
                                            playersArray.push(player);
                                        }
                                    } else {
                                        player.username = payload.data['UserDetails'][i].Nickname;
                                        player.kills = payload.data['UserDetails'][i].HeroLastHits;
                                        player.towerLastHits = payload.data['UserDetails'][i].TowerLastHits;
                                        player.deaths = payload.data['UserDetails'][i].Deaths;
                                        player.assists = payload.data['UserDetails'][i].Assists;
                                        player.heroLevel = payload.data['UserDetails'][i].Level;
                                        player.team = payload.data['UserDetails'][i].Team;
                                        player.hero = payload.data['UserDetails'][i].HeroName;
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

                                resolve(matchDetails);
                            }
                        } else {
                            Logger.append(LOG_FILE, 'No response body for getPlayersAndGameType');
                            reject();
                        }
                    }.bind(this)).catch(function(err) {
                        var error = new Date() + 'Error in getPlayersAndGameType: ' + JSON.stringify(err);
                        Logger.append(LOG_FILE, error);
                        this.queueManager.failed(this);
                    }.bind(this));
                }
            } else {
                Logger.append(LOG_FILE, 'No response body for getReplaySummary');
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
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if(data.hasOwnProperty('UserDetails')) {
                var startTime = this.replayJSON.lastCheckpointTime;
                var endTime = this.replayJSON.newCheckpointTime;

                return this.getHeroDamageAtCheckpoint(startTime, endTime).then(function(allPlayerDamage) {
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
                }.bind(this), function(err) {
                    var error = new Date() + 'Error in getPlayersAndGameType: ' + JSON.stringify(err);
                    Logger.append(LOG_FILE, error);
                }.bind(this));
            }
        } else {
            Logger.append(LOG_FILE, 'No response body for getPlayersAndGameType');
        }
    }.bind(this)).catch(function(err) {
        var error = new Date() + 'Error in updatePlayerStats: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
        this.queueManager.failed(this);
    }.bind(this));
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
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
    }).catch(function(err) {
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
                Logger.append(LOG_FILE, "No replays property on the isGameLive object");
                reject();
            }
        }).catch(function(err) {
            var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
            Logger.append(LOG_FILE, error);
            this.queueManager.failed(this);
        }.bind(this));
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
        if (typeof response.body !== 'undefined' && response.body.length > 0) {
            var data = JSON.parse(response.body);
            if (data.hasOwnProperty('events')) {
                events = data.events.map(function(event) {
                   return { killer: event.meta, timestamp: event['time1'] };
                });
            }
        }
        cb(events);
    }).catch(function(err) {
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
                        return { killer: killInfo.killer, killed: killInfo.killed, timestamp: event['time1'] };
                    }.bind(this));
                }.bind(this));
            }
        }
        return Promise.all(events);
    }.bind(this)).catch(function(err) {
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
                Logger.append(LOG_FILE, 'There was no Killer property on the kills array, API may have changed');
                reject();
            }
        }
    }).catch(function(err) {
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
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
 * 2 = There was an error, delete this?
 */

Replay.prototype.getNextCheckpoint = function(lastCheckpointTime) {
    var url = `${REPLAY_URL}/replay/v2/replay/${this.replayId}/event?group=checkpoint`;

    return requestify.get(url).then(function(response) {
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
                return({ code: 0, lastCheckpointTime: lastCheckpointTime, currentCheckpointTime: newCheckpointTime });
            } else {
                return({ code: 1 });
            }
        } else {
            Logger.append('./logs/log.txt', 'events was not a valid key for the checkpoints array');
            return cb({ code: 1 });
        }
    }).catch(function(err) {
        var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
        Logger.append(LOG_FILE, error);
        this.queueManager.failed(this);
    }.bind(this));
};

/*
 * Opens the file for this replay or creates it
 */

Replay.prototype.getFileHandle = function() {
    return new Promise(function(resolve, reject) {
        try {
            this.replayJSON = JSON.parse(fs.readFileSync('./out/replays/' + this.replayId + '.json'));
            resolve();
        } catch(e) {
            this.replayJSON = Replay.getEmptyReplayObject(this.replayId, this.checkpointTime);
            fs.writeFile('./out/replays/' + this.replayId + '.json', JSON.stringify(this.replayJSON), function(err) {
                if(err) {
                    Logger.append(LOG_FILE, e);
                    reject();
                } else {
                    resolve();
                }
            });
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
        var url = `${REPLAY_URL}/replay/v2/replay`;
        var data = null;
        requestify.get(url).then(function (response) {
            if (typeof response.body !== 'undefined' && response.body.length > 0) {
                data = JSON.parse(response.body);
                if (data.hasOwnProperty('replays')) {
                    data.replays.map(function (replay) {
                        // Insert into SQL
                        var query = 'INSERT INTO replays (replayId, status) VALUES ("' + replay.SessionName + '", "UNSET")';
                        conn.query(query, function() {
                            Logger.append(LOG_FILE, 'Inserted ' + replay.SessionName + ' into replays table');
                        });
                    });
                    resolve(data.replays);
                }
                reject('0 Replays were on the endpoint');
            } else {
                reject('The body had no replay data.');
            }
        }).catch(function(err) {
            var error = new Date() + 'Error in parseDataAtNextCheckpoint: ' + JSON.stringify(err);
            Logger.append(LOG_FILE, error);
            this.queueManager.failed(this);
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
    return {
        replayId: replayId,
        startedAt: null,
        lastCheckpointTime: checkpointTime,
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
        case 'BLUE_IGGY AND SCORCH': return true; case 'RED_IGGY AND SCORCH': return true;
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