CREATE TABLE default.`foo-bar` (
  `event-date` Date,
  `user_id` UInt64
)
ENGINE = Memory;

CREATE VIEW default.`view-with-dash` AS
SELECT *
FROM default.`foo-bar`;
