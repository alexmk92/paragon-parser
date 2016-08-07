CREATE TABLE replays (
    id int not null auto_increment,
    replayId varchar(50) not null,
    status varchar(25) not null DEFAULT "UNSET",
    checkpointTime int DEFAULT 0,
    live bool not null DEFAULT false,
    completed bool DEFAULT false,
    primary key (id)
);
ALTER TABLE replays ADD UNIQUE KEY `replayId` (`replayId`);

CREATE TABLE queue (
    id int not null auto_increment,
    replayId varchar(50) not null,
    attempts int not null DEFAULT 0,
    reserved bool not null DEFAULT false,
    reserved_at TIMESTAMP DEFAULT '0000-00-00 00:00:00',
    completed bool not null DEFAULT false,
    priority int not null DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled TIMESTAMP DEFAULT '0000-00-00 00:00:00',
    primary key (id)
);
ALTER TABLE queue ADD UNIQUE KEY `replayId` (`replayId`);

-- ALTER TABLE queue ADD COLUMN  reserved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- reserved = only one worker can access it if true
-- priority =
-- scheduled = Doesn't let jobs happen that are in the future