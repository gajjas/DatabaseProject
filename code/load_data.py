import psycopg2
import json
from psycopg2.extras import execute_values

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data

# Creates a dictionary for each row in d
def createEmploymentDictionary(areaType, area, naics, naicsTitle, year, establishment, averageEmployment, totalWage, annualAverageSalary):
    return {
        "Area Type":areaType,
        "Area":area,
        "NAICS":naics,
        "NAICS Title":naicsTitle,
        "Year":year,
        "Establishments":establishment,
        "Average Employment":averageEmployment,
        "Total Wage":totalWage,
        "Annual Average Salary":annualAverageSalary
    }

# Creates a dictionary for each row in population
def createPopulationDictionary(year,ageGroupCode, ageGroupDescription, genderCode, genderDescription, raceCode, raceDescription, countyCode, countyName, population):
    return {
        "Year":year,
        "Age Group Code":ageGroupCode,
        "Age Group Description":ageGroupDescription,
        "Gender Code":genderCode,
        "Gender Description":genderDescription,
        "Race/Ethnicity Code":raceCode,
        "Race/Ethnicity Description":raceDescription,
        "County Code":countyCode,
        "County Name":countyName,
        "Population":population
    }

# Returns a dictionary of the data cutting out the unimportant
def loadEmploymentData():
    # Data is organized as a list with each row being a list element
    # Row list is organized as followed
        # rows[8] = Area Type
        # rows[9] = Area
        # rows[10]  = NAICS
        # rows[11]  = NAICS Title
        # rows[12]  = Year
        # rows[13]  = Establishments
        # rows[14]  = Average Employment
        # rows[15]  = Total Wage
        # rows[16]  = Annual Average Salary
    
    employmentFile = open('datasets/rows.json', 'r')
    employmentData = json.load(employmentFile)['data']

    dictionaryData = []
    for d in employmentData:
        dictionaryData.append(createEmploymentDictionary(d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16]))

    return dictionaryData


# Returns a dictionary of the data cutting out the unimportant
def loadPopulationData():
    # Data is organized as a list with each row being a list element
    # Row list is organized as followed
        # rows[8] = Year
        # rows[9] = Age Group Code
        # rows[10]  = Age Group Description
        # rows[11]  = Gender Code
        # rows[12]  = Gender Description
        # rows[13]  = Race/Ethnicity Code
        # rows[14]  = Race/Ethnicity Description
        # rows[15]  = County Code
        # rows[16]  = County Name
        # rows[17]  = Population
    populationFile = open('datasets/rows (1).json')    
    populationData = json.load(populationFile)['data']

    dictionaryData = []
    for d in populationData:
        dictionaryData.append(createPopulationDictionary(d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16], d[17]))

    return dictionaryData

def main():
    # TODO invoke your code to load the data into the database   
    # d = loadEmploymentData()

    datasets = [
        ('rows.json', 8, 17, 'Employment (areaType, area, NAICS, NAICSTitle, year, Establishments, averageEmployment, totalWage, annualAverageSalary)'),
        ('rows (1).json', 8, 18, 'Population (year, ageGroupCode, ageGroupDescription, genderCode, genderDescription, raceCode, raceDescription, countyCode, countyName, population)')
    ]

    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            for (filename, minCol, maxCol, schema) in datasets:
                with open('datasets/' + filename) as f:
                    data = [tuple(r[minCol:maxCol]) for r in json.load(f)['data']]
                insert_query = f"INSERT INTO {schema} values %s";
                execute_values (cursor, insert_query, data, template=None, page_size=100)

            conn.commit()

if __name__ == "__main__":
    main()
