-- Data Cleaning using SQL
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022?resource=download
SELECT *
FROM world_layoffs_latest.layoffs;

-- Creating copy of RAW data to work on
CREATE TABLE world_layoffs_latest.layoffs_staging
LIKE world_layoffs_latest.layoffs;

INSERT world_layoffs_latest.layoffs_staging
SELECT *
FROM world_layoffs_latest.layoffs;

SELECT *
FROM world_layoffs_latest.layoffs_staging;

-- 1. Remove Duplicates.
-- 2. Standardize the Data.
-- 3. Null Values and Blank Values.
-- 4. Remove any Columns that aren't needed.

-- 1. Remove Duplicates.
-- Since there are no ID column, include row_number to discover duplicates. 
SELECT company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off,`date`) as row_num
FROM world_layoffs_latest.layoffs_staging;

-- Identifying duplicates (row_num > 1)
WITH Duplicate_CTEs AS
(
SELECT company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off,`date`) as row_num
FROM world_layoffs_latest.layoffs_staging
)
SELECT *
FROM Duplicate_CTEs
WHERE row_num > 1; -- Cannot make update/delete function on rows in existing table with CTE. 
				   -- So create another table and delete duplicates

CREATE TABLE `world_layoffs_latest`.`layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
); 

SELECT * 
FROM world_layoffs_latest.layoffs_staging2;

INSERT INTO world_layoffs_latest.layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off,`date`) as row_num
FROM world_layoffs_latest.layoffs_staging;

-- Deleting the duplicate entries
DELETE
FROM world_layoffs_latest.layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM world_layoffs_latest.layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize the Data.
SELECT distinct company, trim(company)
FROM world_layoffs_latest.layoffs_staging2;

UPDATE world_layoffs_latest.layoffs_staging2 -- Removing spaces before and after company name 
SET company = TRIM(company);

SELECT distinct industry #Looks fine
FROM world_layoffs_latest.layoffs_staging2
ORDER BY 1;

SELECT distinct location #Looks fine
FROM world_layoffs_latest.layoffs_staging2;

SELECT distinct country #Looks fine
FROM world_layoffs_latest.layoffs_staging2
order by 1;

-- Data type of 'date' column is in text. Changing it to Date datatype.
SELECT `date`
FROM world_layoffs_latest.layoffs_staging2;

ALTER TABLE world_layoffs_latest.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Converting total_laid_off column data type from 'text' to Integer (signed) - Identified in Data Exploration
UPDATE world_layoffs_latest.layoffs_staging2 
SET total_laid_off = NULL
WHERE total_laid_off = '';

SELECT total_laid_off
FROM world_layoffs_latest.layoffs_staging2
WHERE total_laid_off = '';

-- With Update statement, only values in column are affected. To change the data type of col, use alter
UPDATE world_layoffs_latest.layoffs_staging2 
SET total_laid_off = CONVERT(total_laid_off, SIGNED)
WHERE total_laid_off REGEXP '^[0-9]+$' AND total_laid_off IS NOT NULL;

ALTER TABLE world_layoffs_latest.layoffs_staging2 
MODIFY COLUMN total_laid_off INT;


-- 3. Null Values and Blank Values.
SELECT *
FROM world_layoffs_latest.layoffs_staging2;
-- WHERE industry IS NULL or industry = '';  #company 'Appsmith' has blank industry

SELECT *
FROM world_layoffs_latest.layoffs_staging2
WHERE company = 'Appsmith'; #No other entries for company so cannot modify

/* Deleting rows where total_laid_off and percentage_laid_off are both null as 
we cannot work with them - Useless data*/
SELECT *
FROM world_layoffs_latest.layoffs_staging2
WHERE (total_laid_off IS NULL or total_laid_off = '')
AND (percentage_laid_off IS NULL or percentage_laid_off = '');

DELETE
FROM world_layoffs_latest.layoffs_staging2
WHERE (total_laid_off IS NULL or total_laid_off = '')
AND (percentage_laid_off IS NULL or percentage_laid_off = '');

SELECT *
FROM world_layoffs_latest.layoffs_staging2;

-- 4. Remove any Columns that aren't needed.
-- Getting rid of the 'row_num' column
ALTER TABLE world_layoffs_latest.layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM world_layoffs_latest.layoffs_staging2;