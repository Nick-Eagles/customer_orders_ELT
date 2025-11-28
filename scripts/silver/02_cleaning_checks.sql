-- I want to use 'cust_id' as the key for customers, since it best matches
-- other tables. Here I check what's going on with duplicate keys; it turns out
-- in rare cases customers have multiple records with different dates, and in
-- all such cases the latest date provide the most information.
SELECT t.*
FROM bronze.crm_cust_info AS t
JOIN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    GROUP BY cst_id
    HAVING COUNT(*) > 1
) AS dup
    ON t.cst_id = dup.cst_id
WHERE t.cst_id IS NOT NULL;

-- To be extra cautious about invalid date entries, I just checked if non-null
-- entries could be cast to dates; there were no such invalid entries.
SELECT *
FROM bronze.crm_cust_info
WHERE cst_create_date IS NOT NULL
    AND TO_DATE(cst_create_date, 'YYYY-MM-DD') IS NULL;
