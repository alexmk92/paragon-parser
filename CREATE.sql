CREATE TABLE queue (
    id int not null auto_increment,
    replayId varchar(50) not null,
    attempts int not null DEFAULT 0,
    reserved bool not null DEFAULT false,
    reserved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed bool not null DEFAULT false,
    priority int not null DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checkpointTime int DEFAULT 0,
    live bool not null DEFAULT false,
    primary key (id)
);

ALTER TABLE replays ADD UNIQUE KEY `replayId` (`replayId`);

-- ALTER TABLE queue ADD COLUMN completed_at TIMESTAMP DEFAULT null;

-- reserved = only one worker can access it if true
-- priority =
-- scheduled = Doesn't let jobs happen that are in the future