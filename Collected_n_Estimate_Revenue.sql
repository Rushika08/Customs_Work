SELECT 
    Year,
    Month,
    Revenue_Code,
    Revenue_Source,
    Value,
    'Collected' AS Flag
INTO InsightStaging.Collected_n_Estimate_Revenue
FROM InsightStaging.Actual_Revenue

UNION ALL

SELECT 
    Year,
    Month,
    Revenue_Code,
    Revenue_Source,
    Value,
    'Estimate' AS Flag
FROM InsightStaging.Estimate_Revenue;
