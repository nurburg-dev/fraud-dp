-- Geographic Fraud Detection Validation  
-- Tests if the candidate's fraud detector correctly identified geographic fraud cases
-- Geographic fraud: login from new country + immediate purchase (< 5 minutes)

WITH known_geographic_fraud AS (
  SELECT * FROM {{ ref('known_fraud_cases') }}
  WHERE fraud_type = 'geographic_fraud' AND should_detect = true
),

candidate_geographic_detections AS (
  SELECT DISTINCT user_id
  FROM fraud_detections
  WHERE fraud_detected = true 
  AND fraud_type = 'geographic'
),

validation_metrics AS (
  SELECT 
    COUNT(*) as total_geographic_cases,
    COUNT(cgd.user_id) as detected_geographic_cases,
    ROUND(COUNT(cgd.user_id)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) as geographic_detection_rate
  FROM known_geographic_fraud kgf
  LEFT JOIN candidate_geographic_detections cgd ON kgf.user_id = cgd.user_id
)

-- STANDARDIZED VALIDATION CONTRACT - Required columns for DBT validation framework
SELECT 
  'geographic_fraud_detection' as test_name,
  CASE WHEN geographic_detection_rate >= 75.0 THEN true ELSE false END as passed,
  geographic_detection_rate as score,
  json_build_object(
    'total_geographic_cases', total_geographic_cases,
    'detected_geographic_cases', detected_geographic_cases,
    'geographic_detection_rate', geographic_detection_rate,
    'threshold_percent', 75.0,
    'description', 'Validates detection of geographic fraud (login from new country + immediate purchase)',
    'fraud_type', 'geographic'
  ) as metadata,
  CURRENT_TIMESTAMP as timestamp
FROM validation_metrics