CREATE TABLE IF NOT EXISTS security_events (
    id SERIAL PRIMARY KEY,
    protocol_type VARCHAR(10),
    service VARCHAR(50),
    flag VARCHAR(20),
    label VARCHAR(50),
    is_attack BOOLEAN,
    attack_type VARCHAR(50),
    timestamp DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
