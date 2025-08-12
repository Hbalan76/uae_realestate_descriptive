SELECT *
FROM silver.uae_realestate_2024



-- the distinct types of properties listed (type)

SELECT
type,
COUNT(*) AS count
FROM silver.uae_realestate_2024
GROUP BY type
ORDER BY count DESC

--NO. of listing that are currently verified (verified = true)


SELECT
COUNT(verified) AS varified
FROM silver.uae_realestate_2024
WHERE verified = 1 

--NO. of listing that are verified or not varified
SELECT
verified,
COUNT(*) AS count
FROM silver.uae_realestate_2024
GROUP BY verified
ORDER BY count DESC

--The average price of listings with at least 2 bedrooms
SELECT 
ROUND(AVG(CAST(price AS FLOAT)), 2) AS avg_price
FROM silver.uae_realestate_2024
WHERE bedrooms = 2


--- the top 5 most common furnishing statuses (furnishing)
SELECT furnishing, COUNT(*) AS count
FROM silver.uae_realestate_2024
WHERE furnishing IS NOT NULL AND furnishing <> ''
GROUP BY furnishing
ORDER BY count DESC
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY


--The average price per bedroom for each property type

SELECT
	type,
	AVG(
		CASE	
			WHEN bedrooms > 0 THEN CAST(price AS FLOAT) / bedrooms
			ELSE NULL
		END) AS avg_price_per_bedrooms
FROM silver.uae_realestate_2024
WHERE price IS NOT NULL AND bedrooms IS NOT NULL
GROUP BY type
ORDER BY avg_price_per_bedrooms DESC


--Display addresses that have the highest number of listings
SELECT
displayAddress,
COUNT(*) AS listing_count
FROM silver.uae_realestate_2024
WHERE displayAddress IS NOT NULL AND displayAddress <> '' 
GROUP BY displayAddress
ORDER BY listing_count DESC

--listings that have both bathrooms and bedrooms greater than 2

SELECT
type,
COUNT(*) AS listing_count
FROM silver.uae_realestate_2024
WHERE bedrooms > 2 AND bathrooms > 2
GROUP BY type
ORDER BY listing_count DESC


--The distribution of listings by priceDuration (e.g., per month, per week)?

SELECT
	priceDuration,
	COUNT(*) AS listing_count,
	ROUND(AVG(CAST(price AS FLOAT)), 2) AS avg_price
FROM silver.uae_realestate_2024
WHERE priceDuration IS NOT NULL AND priceDuration <> ''
GROUP BY priceDuration
ORDER BY listing_count DESC;


-- The average sizeMin for furnished vs. unfurnished properties
SELECT
	furnishing AS furnishing_status,
	COUNT(*) AS listing_count,
	AVG(CAST(REPLACE(sizeMin , ' sqft', '') AS INT)) AS avg_size_min
FROM silver.uae_realestate_2024
WHERE furnishing IN (0, 1)
	AND sizeMin IS NOT NULL 
GROUP BY furnishing
ORDER BY avg_size_min DESC


-- Trend Analysis: the moving average of prices over time

SELECT
	CAST(addedOn AS DATE),
	price,
	AVG(price) OVER (
		ORDER BY CAST(addedOn AS DATE)
		ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
	)AS moving_avg_7_day
FROM silver.uae_realestate_2024
WHERE TRY_CAST(addedOn AS DATE) IS NOT NULL AND price IS NOT NULL
ORDER BY CAST(addedOn AS DATE)


-- IMPORTANT SQL QUERY ON STANDERD DEVIATION (STDV)
SELECT
*,
ROUND(price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0), 2) AS price_per_sqft,
CASE 
	WHEN price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0) > (
		SELECT AVG(price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0)) + 2 * STDEV(price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0))
		FROM silver.uae_realestate_2024
	) THEN 'OVERPRICED'
	WHEN price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0) < (
		SELECT AVG(price / NULLIF(CAST(REPLACE(sizeMin , ' sqft', '') AS FLOAT), 0)) - 2 * STDEV(price / NULLIF(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT), 0))
		FROM silver.uae_realestate_2024
	) THEN 'UNDERPRICED'
	ELSE 'NORMAL'
END AS priceing_flage
FROM silver.uae_realestate_2024
WHERE sizeMin IS NOT NULL AND price IS NOT NULL


-- Price per bedroom/bathroom analysis:

SELECT
	type,
	ROUND(AVG(CASE
				 WHEN bathrooms > 0 THEN CAST(price AS FLOAT) / bathrooms
			ELSE NULL
		END), 2) AS avg_price_per_bathroom,
	ROUND(AVG(CASE
				WHEN bedrooms > 0 THEN CAST(price AS FLOAT) / bedrooms
			ELSE NULL
		END), 2) AS avg_price_per_bedroom
FROM silver.uae_realestate_2024
WHERE bathrooms IS NOT NULL AND price IS NOT NULL
GROUP BY type


-- Z-SCORE FORMULA ((price - avg(price)) / stdv(price))
SELECT *,
	ROUND((CAST(price AS FLOAT) - AVG(CAST(price AS FLOAT)) OVER()) / NULLIF(STDEV(CAST(price AS FLOAT)) OVER(), 0), 2) AS price_z,
	ROUND((CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT) - AVG(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT)) OVER()) / NULLIF(STDEV(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT)) OVER(), 0), 2) AS size_z
FROM silver.uae_realestate_2024



/*Min-max normalization transforms your data so that all values 
fall within a specific range—usually 0 to 1. 
This is especially helpful when different columns have different scales */

-- Min-Max FORMULA --- Normalized Value = (Value - Minimum) / (Maximum - Minimum)
SELECT *,
ROUND((CAST(price AS FLOAT) - MIN(CAST(price AS FLOAT)) OVER()) / NULLIF((MAX(CAST(price AS FLOAT)) OVER() - MIN(CAST(price AS FLOAT)) OVER()), 0), 2) AS price_scaled,
ROUND((CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT) - MIN(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT)) OVER()) / NULLIF((MAX(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT)) OVER() - MIN(CAST(REPLACE(sizeMin, ' sqft', '') AS FLOAT)) OVER()), 0), 2) AS size_scaled
FROM silver.uae_realestate_2024



-- LISTING VELOCITY OVER TIME USING addedOn

 -- Extract Date Part
	SELECT
		DATEFROMPARTS(YEAR(addedOn), MONTH(addedOn), 1) AS listing_month,
		COUNT(*) AS listing_added
	FROM silver.uae_realestate_2024
	GROUP BY DATEFROMPARTS(YEAR(addedOn), MONTH(addedOn), 1)
	ORDER BY listing_month;

-- Weekly Listing Count
	SELECT
		DATEADD(WEEK, DATEDIFF(WEEK, 0, addedOn), 0) AS listing_week,
		COUNT(*) AS listings_added
	FROM silver.uae_realestate_2024
	GROUP BY DATEADD(WEEK, DATEDIFF(WEEK, 0, addedOn), 0)
	ORDER BY listing_week;

-- Calculate Rolling Averages(Optional)
	SELECT
		DATEFROMPARTS(YEAR(addedOn), MONTH(addedOn), 1) AS listing_month,
		COUNT(*) AS listing_added,
		AVG(COUNT(*)) OVER (ORDER BY DATEFROMPARTS(YEAR(addedOn), MONTH(addedOn), 1) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg
	FROM silver.uae_realestate_2024
	GROUP BY DATEFROMPARTS(YEAR(addedOn), MONTH(addedOn), 1)
	ORDER BY listing_month

SELECT 
*
FROM silver.uae_realestate_2024