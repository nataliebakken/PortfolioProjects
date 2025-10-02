/* -------------------------------------------------------------------------
   DATA CLEANING AND EXPLORATORY ANALYSIS OF GLOBAL LAYOFFS DATASET
   Purpose: Clean a raw layoffs dataset and uncover insights about global 
            layoffs by company, country, industry, and time period.
   ------------------------------------------------------------------------- */

/* -----------------------
   1. CREATE STAGING TABLE
 ------------------------- */
SELECT * FROM layoffs;

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

/* -----------------------
   2. REMOVE DUPLICATES
 ------------------------- */
 -- Identify duplicate rows based on company, industry, layoffs, and date.
SELECT * FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
SELECT * , ROW_NUMBER() OVER (
	PARTITION BY company, industry, total_laid_off,`date`) 
    AS row_num
FROM layoffs_staging) 

SELECT * FROM duplicate_cte
WHERE row_num > 1;
-- No duplicates found in dataset.
    
/* -----------------------
   3. STANDARDIZE DATA
 ------------------------- */
-- Convert string-based dates into DATE format.
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- Clean up empty industries (set blanks to NULL for easier handling).
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- Check:
SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Populate missing industries where the same company has valid data.
SELECT *
FROM layoffs_staging t1
JOIN layoffs_staging t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging t1
JOIN layoffs_staging t2
	ON t1.company = t2.company
	SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Standardize country names (remove extra periods).
UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;

/* -----------------------
   4. STANDARDIZE DATA
 ------------------------- */
-- Drop rows where layoffs data is completely missing (not useful for EDA).
DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

/* ------------------------------
   Exploratory Data Analysis (EDA)
 -------------------------------- */
SELECT * FROM world_layoffs.layoffs_staging;

-- Which companies had 100 percent layoffs
SELECT * FROM world_layoffs.layoffs_staging
WHERE  percentage_laid_off = 1;
-- Insight: Mostly startups went out of business during this time.

-- Biggest single-day layoffs
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- Insight: Identifies layoff events that let go of thousands of employees at once. 
-- Biggest one-day layoffs: Google (12,000), Meta (11,000), Amazon (10,000), Microsoft (10,000)

-- Companies with the most layoffs overall (all time)
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;
-- Insight: Reveals which firms cut the most jobs overall.
-- Most layoffs overall: Amazon (18,150), Google (12,000), Meta (11,000)

-- Locations most impact
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;
-- San Francisco and Seattle had the most layoffs.

-- Country totals
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY country
ORDER BY 2 DESC;
-- U.S. dominates layoffs (~85% of total), followed by India and the UK. 

-- Yearly totals
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY YEAR(date)
ORDER BY 1 ASC;
-- 2022 = peak layoff year (161,711 employees cut). Layoffs slowed in 2023 but remained high.

-- Industry totals
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;
-- Industries most impacted: Other Services (28k+), Consumer (16k+), Retail (14k+)

-- Stage of companies
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;
-- Post-IPO companies had the largest layoffs, followed by acquired.

-- Top 3 companies per year
WITH Company_Year AS 
(SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM world_layoffs.layoffs_staging
  GROUP BY company, YEAR(date))
, Company_Year_Rank AS 
(SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
-- Each year's companies with the most layoffs: 2022: Playtika, Doma, Pluralsight; 2023: Google, Microsoft, Ericsson

-- Rolling monthly total layoffs
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging
GROUP BY dates
ORDER BY dates ASC;

WITH DATE_CTE AS 
(SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging
GROUP BY dates
ORDER BY dates ASC)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
-- Rolling totals reveal a increase in late 2022 and early 2023, passing 120k cumulative layoffs within 12 months.