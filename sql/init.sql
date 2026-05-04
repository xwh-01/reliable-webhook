CREATE TABLE IF NOT EXISTS events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_key VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_event_key (event_key)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id BIGINT NOT NULL,
    target_url VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    last_error TEXT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    locked_until TIMESTAMP NULL,
    replay_of_delivery_id BIGINT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_deliveries_event_id FOREIGN KEY (event_id) REFERENCES events(id),
    KEY idx_deliveries_event_id (event_id),
    KEY idx_deliveries_status (status),
    KEY idx_deliveries_status_next_retry_at (status, next_retry_at),
    KEY idx_deliveries_locked_until (locked_until),
    KEY idx_deliveries_replay_of_delivery_id (replay_of_delivery_id)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    delivery_id BIGINT NOT NULL,
    attempt_no INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_message TEXT NULL,
    response_status INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_delivery_attempts_delivery_id
        FOREIGN KEY (delivery_id) REFERENCES deliveries(id),
    KEY idx_delivery_attempts_delivery_id (delivery_id)
);
