CREATE TABLE IF NOT EXISTS watchlist_symbols (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL UNIQUE,
    is_active TINYINT(1) DEFAULT 1,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS detector_state (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    detector_name VARCHAR(64) NOT NULL,
    last_triggered_at DATETIME NULL,
    last_payload_hash VARCHAR(128) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uniq_symbol_detector (symbol, detector_name)
);

CREATE TABLE IF NOT EXISTS alerts_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    detector_name VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    sent_at DATETIME NOT NULL,
    raw_event_json JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    INDEX idx_sent_at (sent_at)
);
