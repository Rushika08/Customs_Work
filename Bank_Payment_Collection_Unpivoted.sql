SELECT 
      Date,
      Year,
      Month,
      [TOTAL] AS Value,
      'Collected' AS Flag
INTO InsightStaging.Bank_Payment_Collection_Unpivoted
FROM InsightStaging.Bank_Payment_Collection

UNION ALL

SELECT
      Date,
      Year,
      Month,
      [DAILY TARGET] AS Value,
      'Estimate' AS Flag
FROM InsightStaging.Bank_Payment_Collection;
