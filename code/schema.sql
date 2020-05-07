CREATE TABLE IF NOT EXISTS Employment
(
    areaType VARCHAR (127),
    area VARCHAR (127),
    NAICS INT,
    NAICSTitle VARCHAR (127),
    year INT,
    Establishments INT,
    averageEmployment INT,
    totalWage BIGINT,
    annualAverageSalary BIGINT,
    PRIMARY KEY (area, year, NAICS)
	
);

CREATE TABLE IF NOT EXISTS Population
(
	year INT,
	ageGroupCode INT,
	ageGroupDescription VARCHAR (127),
	genderCode INT,
	genderDescription VARCHAR (127),
	raceCode INT,
	raceDescription VARCHAR (127),
	countyCode INT,
	countyName VARCHAR (127),
	population INT,
	PRIMARY KEY (countyName, year)
);