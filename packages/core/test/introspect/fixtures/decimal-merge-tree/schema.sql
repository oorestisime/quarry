CREATE TABLE default.partner_event (
  `event_date` Date,
  `partner_id` UInt64,
  `dynamic_amount` Decimal(12, 6)
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (partner_id, event_date);

CREATE VIEW default.partner_event_months AS
SELECT
  partner_id,
  toYYYYMM(event_date) AS event_yyyymm
FROM default.partner_event;
