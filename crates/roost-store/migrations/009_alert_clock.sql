CREATE TABLE alert_clock (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    elapsed INTEGER NOT NULL CHECK(elapsed >= 0)
);
INSERT INTO alert_clock VALUES (1, 0);

-- Give a legacy charge its full window on the verified elapsed-time clock.
UPDATE alert_cooldowns SET until_at = 300;
UPDATE alert_restart_attempts SET charged_at = 0;
PRAGMA user_version = 9;
