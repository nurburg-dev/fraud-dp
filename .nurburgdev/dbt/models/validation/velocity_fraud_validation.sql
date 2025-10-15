-- Velocity Fraud Detection Validation
-- Tests if the candidate's fraud detector correctly identified velocity fraud cases
-- Velocity fraud: >10 purchases in 1 minute from the same user

WITH known_velocity_fraud AS (
  SELECT * FROM {{ ref('known_fraud_cases') }}
  WHERE fraud_type = 'velocity_fraud' AND should_detect = true
),

candidate_velocity_detections AS (
  SELECT DISTINCT user_id
  FROM fraud_detections
  WHERE fraud_detected = true 
  AND fraud_type = 'velocity'
),

validation_metrics AS (
  SELECT 
    COUNT(*) as total_velocity_cases,
    COUNT(cvd.user_id) as detected_velocity_cases,
    ROUND(COUNT(cvd.user_id)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) as velocity_detection_rate
  FROM known_velocity_fraud kvf
  LEFT JOIN candidate_velocity_detections cvd ON kvf.user_id = cvd.user_id
)

-- STANDARDIZED VALIDATION CONTRACT - Required columns for DBT validation framework
SELECT 
  'velocity_fraud_detection' as test_name,
  CASE WHEN velocity_detection_rate >= 80.0 THEN true ELSE false END as passed,
  velocity_detection_rate as score,
  json_build_object(
    'total_velocity_cases', total_velocity_cases,
    'detected_velocity_cases', detected_velocity_cases,
    'velocity_detection_rate', velocity_detection_rate,
    'threshold_percent', 80.0,
    'description', 'Validates detection of velocity fraud (>10 purchases in 1 minute)',
    'fraud_type', 'velocity'
  ) as metadata,
  CURRENT_TIMESTAMP as timestamp
FROM validation_metrics