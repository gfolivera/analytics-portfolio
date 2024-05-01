-- original dataset: "owid-covid-data-old.csv" downloaded at 2024-04-24. available on https://github.com/owid/covid-19-data/tree/master/public/data

USE PortfolioProject;
GO

-- dataset definition
GO

-- create new table from slice of original dataset

DROP TABLE IF EXISTS dbo.ACovidDeaths
SELECT  *
INTO dbo.ACovidDeaths
FROM
	PortfolioProject..CovidDeaths
WHERE
	date >= '20200128 00:00:00.000' AND
	date <= '20210430 00:00:00.000'
GO

-- total deaths is nvarchar, change to integer
ALTER TABLE PortfolioProject..ACovidDeaths
ADD new_total_deaths INT;

UPDATE PortfolioProject..ACovidDeaths
SET new_total_deaths = TRY_CAST(total_deaths AS int);

SELECT new_total_deaths
FROM PortfolioProject..ACovidDeaths

ALTER TABLE PortfolioProject..ACovidDeaths
DROP COLUMN total_deaths
GO


EXECUTE sp_RENAME 	'ACovidDeaths.new_total_deaths','total_deaths', 'COLUMN';

SELECT TOP 1 * from PortfolioProject..ACovidDeaths
GO



DROP TABLE IF EXISTS ACovidVaccinations

SELECT  *
INTO ACovidVaccinations
FROM
	PortfolioProject..CovidVaccinations
WHERE
	date >= '20200128 00:00:00.000' AND
	date <= '20210430 00:00:00.000'
GO


-- COVID DEATHS
GO



-- death percentage by countries at the latest date of the sample (April 30, 2021) -  using RANK
SELECT t1.location, date,
	total_cases,
	total_deaths,
	(total_deaths/total_cases)*100 as death_percentage,
	ROW_NUMBER() OVER(ORDER BY (total_deaths/total_cases)*100 DESC) as ranking
FROM 
	PortfolioProject..ACovidDeaths t1
INNER JOIN
(
	SELECT 
		location,
		MAX(date) as max_date
	FROM 
		PortfolioProject..ACovidDeaths
	WHERE continent != ''
	GROUP BY
		location
	
) t2 
ON
	t1.location = t2.location AND
	t1.date = t2.max_date
ORDER BY
	death_percentage DESC
GO

-- comparison between United States and Brazil covid death rates evolution
SELECT * , US.rate-BR.rate as 'states-brazil'
FROM 
(
SELECT
	location,
	 date,
	ROUND((total_deaths/total_cases)*100,2) as rate
FROM
	PortfolioProject..ACovidDeaths
WHERE location = 'United States'
) US
INNER JOIN (SELECT
	location,
	 date,
	ROUND((total_deaths/total_cases)*100,2) as rate
FROM
	PortfolioProject..ACovidDeaths
WHERE location = 'Brazil'
) BR ON US.date = BR.date
ORDER BY US.date
GO


-- death count by continent
USE PortfolioProject;

SELECT location, MAX(total_deaths) as death_count
FROM dbo.ACovidDeaths
WHERE continent = '' AND
location NOT IN
	('World',
	'High income',
	'Upper middle income',
	'European Union',
	'Lower middle income',
	'Low income',
	'International')
GROUP BY location
ORDER BY death_count DESC
GO

-- cases by date around the world
SELECT date,
	SUM(new_cases) as total_cases,
	SUM(CAST(new_deaths AS int)) as total_deaths
FROM ACovidDeaths
WHERE continent != ''
GROUP BY date
ORDER BY date
GO

--overall cases and deaths
SELECT SUM(new_cases) as total_cases,
	SUM(CAST(new_deaths AS int)) as total_deaths,
	ROUND((SUM(CAST(new_deaths AS int))/SUM(new_cases))*100,3) as death_percentage
FROM ACovidDeaths
GO



-- COVID VACCINATIONS
GO

-- vaccinated population growth on locations overtime. using WINDOW FUNCTIONS

SELECT location, 
	date,
	new_vaccinations,
	SUM(CAST(new_vaccinations as float)) OVER (PARTITION BY location ORDER BY location, date) as vaccinated_population
FROM
	ACovidVaccinations
WHERE 
	continent != ''
ORDER BY location, date
GO



-- comparison between United States and Brazil covid vaccination rates evolution
-- using CTE
WITH JoinedTables AS
(
	SELECT d.location, d.date, d.new_deaths, d.total_deaths, d.continent, d.population,
		 v.new_vaccinations, v.total_vaccinations
	FROM ACovidDeaths AS d
	JOIN ACovidVaccinations AS v
	ON
		d.location = v.location AND
		d.date = v.date
	WHERE d.continent != ''
)
SELECT 
	us.location,
	us.date,
	us.new_vaccinations as us_new_vaccinations,
	SUM(CAST(us.new_vaccinations AS FLOAT)) OVER (PARTITION BY us.location ORDER BY us.location, us.date) AS us_vaccinated,
	ROUND((CAST(us.total_vaccinations AS FLOAT)/us.population)*100,4) AS us_vax_pop_percentage,
	br.location,
	br.new_vaccinations as br_new_vaccinations,
	br.br_vaccinated,
	br.br_vax_pop_percentage
FROM JoinedTables AS us
JOIN (
	SELECT
		jt.location,
		jt.date,
		jt.new_vaccinations,
		SUM(CAST(jt.new_vaccinations AS FLOAT)) OVER (PARTITION BY jt.location ORDER BY jt.location, jt.date) AS br_vaccinated,
		ROUND((CAST(total_vaccinations AS FLOAT)/jt.population)*100,4) AS br_vax_pop_percentage
	FROM 
		JoinedTables jt
	WHERE
		jt.location = 'Brazil'
	) AS br
ON
	us.date = br.date
WHERE us.location = 'United States'	
GO




-- vaccination count by continent
SELECT location, MAX(CAST(total_vaccinations AS FLOAT)) as total_vaccination
FROM ACovidVaccinations
WHERE continent = '' AND
location NOT IN
	('World',
	'High income',
	'Upper middle income',
	'European Union',
	'Lower middle income',
	'Low income',
	'International')
GROUP BY location
ORDER BY total_vaccination DESC
GO


-- positive rates by location at last date of dataset
SELECT location, positive_rate
FROM ACovidVaccinations
WHERE date = (SELECT MAX(date) from ACovidVaccinations)
ORDER BY location
GO


-- start vaccination date by location
SELECT
	location,
	MIN(date) AS vaccination_start
FROM
	ACovidVaccinations
WHERE CAST(new_vaccinations AS FLOAT) != 0
GROUP BY location
ORDER BY location
GO



-- TEMP TABLE

USE PortfolioProject;
GO

DROP TABLE IF EXISTS #DeathAndVaccines
GO

CREATE TABLE  #DeathAndVaccines
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
new_cases float,
total_cases float,
new_deaths float,
total_deaths float,
new_tests float,
positive_rate float,
total_tests float,
new_vaccinations float,
total_vaccinations float
)

INSERT INTO #DeathAndVaccines
SELECT
	d.continent,
	d.location,
	d.date, 
	d.new_cases,
	d.total_cases,
	CAST(d.new_deaths AS FLOAT) AS new_deaths,
	CAST(d.total_deaths AS FLOAT) AS total_deaths,
	CAST(v.new_tests AS FLOAT) AS new_tests,
	CAST(v.positive_rate AS FLOAT) AS positive_rate,
	CAST(v.total_tests AS FLOAT) AS total_tests,
	CAST(v.new_vaccinations AS FLOAT) AS new_vaccinations,
	CAST(v.total_vaccinations AS FLOAT) AS total_vaccinations
FROM ACovidDeaths d
JOIN ACovidVaccinations v
ON
	d.location = v.location AND
	d.date = v.date
GO

SELECT * FROM #DeathAndVaccines
ORDER BY location, date
GO



--VIEW

CREATE VIEW CovidDeathVaccinationsCountries AS
SELECT
	d.continent,
	d.location,
	d.date, 
	d.new_cases,
	d.total_cases,
	CAST(d.new_deaths AS FLOAT) AS new_deaths,
	CAST(d.total_deaths AS FLOAT) AS total_deaths,
	CAST(v.new_tests AS FLOAT) AS new_tests,
	CAST(v.positive_rate AS FLOAT) AS positive_rate,
	CAST(v.total_tests AS FLOAT) AS total_tests,
	CAST(v.new_vaccinations AS FLOAT) AS new_vaccinations,
	CAST(v.total_vaccinations AS FLOAT) AS total_vaccinations
FROM ACovidDeaths d
JOIN ACovidVaccinations v
ON
	d.location = v.location AND
	d.date = v.date
WHERE d.continent != ''
GO

SELECT * FROM CovidDeathVaccinationsCountries
ORDER BY location, date
GO