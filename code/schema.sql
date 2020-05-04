DROP TABLE IF EXISTS Employment;
CREATE TABLE Employment(
    areaType VARCHAR(127),
    area VARCHAR(127),
    NAICS INT,
    NAICSTitle VARCHAR(127),
    year INT,
    Establishments INT,
    averageEmployment INT,
    totalWage BIGINT,
    annualAverageSalary BIGINT
);

DROP TABLE IF EXISTS Population;
CREATE TABLE Population(
	year INT,
	ageGroupCode INT,
	ageGroupDescription VARCHAR(127),
	genderCode INT,
	genderDescription VARCHAR(127),
	raceCode INT,
	raceDescription VARCHAR(127),
	countyCode INT,
	countyName VARCHAR(127),
	population INT
);