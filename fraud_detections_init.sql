-- fraud_detections table initialization script
-- This table is pre-created for candidate fraud detection services
-- to store their fraud detection results

CREATE TABLE IF NOT EXISTS fraud_detections (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    fraud_detected BOOLEAN NOT NULL,
    fraud_type VARCHAR(50) NOT NULL, -- 'velocity' or 'geographic'
    detection_reason TEXT,
    confidence_score DECIMAL(5,2), -- Optional confidence score 0.00-1.00
    metadata JSONB, -- Additional detection metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fraud_detections_user_id ON fraud_detections(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_detections_fraud_type ON fraud_detections(fraud_type);
CREATE INDEX IF NOT EXISTS idx_fraud_detections_detected_at ON fraud_detections(detected_at);

-- Grant permissions to the database user
GRANT ALL PRIVILEGES ON TABLE fraud_detections TO postgres;
GRANT USAGE, SELECT ON SEQUENCE fraud_detections_id_seq TO postgres;