WITH
  raw_date_performance_metric as (
    SELECT  
      date_key,
      customer_id,
      original_code,
      SUM(nmv) as nmv
    FROM 
      `order_device_utm`
    WHERE 
      1=1
      AND date_key BETWEEN DATE_SUB('{{ macros.localtz.ds(ti) }}', INTERVAL 1 YEAR) AND '{{ macros.localtz.ds(ti) }}'
    GROUP BY
      1,2,3    
  ),
  raw_data_rfm as (
    SELECT
      customer_id,
      MAX(date_key) as last_active_date,
      DATE_DIFF(DATE('{{ macros.localtz.ds(ti) }}'),DATE(MAX(date_key)), DAY) as recency,
      COUNT(DISTINCT original_code) as frequency,
      SUM(nmv) as monetary
    FROM 
      raw_date_performance_metric
    GROUP BY 
      1
  ),
  raw_data_rfm_segment as (	
    SELECT
      customer_id,
      last_active_date,
      recency,
      frequency,
      monetary,
      NTILE(10) OVER (ORDER BY recency) AS total_percentile_recency,
      NTILE(10) OVER (ORDER BY frequency) AS total_percentile_frequency,
      NTILE(10) OVER (ORDER BY monetary) AS total_percentile_monetary,
    FROM 
      raw_data_rfm
  ),
  raw_data_rfm_rank as (	
    SELECT
      customer_id,
      last_active_date,
      recency,
      frequency,
      monetary,
      total_percentile_recency,
      total_percentile_frequency,
      total_percentile_monetary,
      CASE
        WHEN recency >= 64 THEN 1
        WHEN recency >= 29 AND recency <=63 THEN 2
        WHEN recency <=28 THEN 3
        ELSE 0
      END as recency_rank,
      CASE
        WHEN total_percentile_frequency = 10 THEN 3
        WHEN total_percentile_frequency = 9 THEN 2
        WHEN total_percentile_frequency <= 8 THEN 1
        ELSE 0
      END as frequency_rank,		
      CASE
        WHEN total_percentile_monetary = 10 THEN 3
        WHEN total_percentile_monetary = 9 THEN 2
        WHEN total_percentile_monetary <= 8 THEN 1
        ELSE 0
      END as monetary_rank,		
    FROM 
      raw_data_rfm_segment
  )
  SELECT 
    DATE('{{ macros.localtz.ds(ti) }}') as date_key,
    customer_id,
    last_active_date,
    recency,
    frequency,
    monetary,
    total_percentile_recency,
    total_percentile_frequency,
    total_percentile_monetary,
    recency_rank,
    frequency_rank,
    monetary_rank,
    CONCAT(recency_rank,frequency_rank,monetary_rank) as rfm_rank
  FROM 
    raw_data_rfm_rank
