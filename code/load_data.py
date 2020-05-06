import psycopg2
import psycopg2.extras
import json

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
conn = psycopg2.connect(connection_string)

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
        "Race Code":raceCode,
        "Race Description":raceDescription,
        "County Code":countyCode,
        "County Name":countyName,
        "Population":population
    }

# Inserts the Employment Data
def insertEmploymentData(employmentData):
    print("Inserting Employment Data ...", end="\n\n")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    #  Checking if duplicates exist
    existsQuery = "SELECT EXISTS(SELECT * FROM Employment WHERE areaType=%(Area Type)s \
        AND area=%(Area)s AND NAICS=%(NAICS)s AND NAICSTitle=%(NAICS Title)s AND year=%(Year)s \
            AND Establishments=%(Establishments)s AND averageEmployment=%(Average Employment)s \
                AND totalWage=%(Total Wage)s AND annualAverageSalary=%(Annual Average Salary)s)"
    
    # The query to insert all elements
    insertQuery = "INSERT INTO Employment (areaType, area, NAICS, NAICSTitle, year,\
        Establishments, averageEmployment, totalWage, annualAverageSalary) VALUES (\
            %(Area Type)s, %(Area)s, %(NAICS)s, %(NAICS Title)s, %(Year)s, \
                %(Establishments)s, %(Average Employment)s, \
                    %(Total Wage)s, %(Annual Average Salary)s)"

    for row in employmentData:
        cursor.execute(existsQuery, row)

        # If duplicate doesnt exist for the row
        if cursor.fetchone()[0] == False:
            cursor.execute(insertQuery, row)
            print("Inserted Data: ", row)
            
    conn.commit()
    print("Insert Committed")
    print("\n\nFinished Inserting Employment Data", end="\n\n")
        

# Inserts the population data
def insertPopulationData(populationData):
    print("Inserting Population Data ...", end="\n\n")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Checking if duplicates exist
    existsQuery = "SELECT EXISTS(SELECT * FROM Population WHERE year=%(Year)s AND ageGroupCode=%(Age Group Code)s\
        AND ageGroupDescription=%(Age Group Description)s AND genderCode=%(Gender Code)s\
            AND genderDescription=%(Gender Description)s AND raceCode=%(Race Code)s\
                AND raceDescription=%(Race Description)s AND countyCode=%(County Code)s\
                    AND countyName=%(County Name)s AND population=%(Population)s)"
    
    # The query to insert all elements
    insertQuery = "INSERT INTO population (year, ageGroupCode, ageGroupDescription, genderCode,\
        genderDescription, raceCode, raceDescription, countyCode, countyName, population) VALUES (%(Year)s,\
            %(Age Group Code)s, %(Age Group Description)s, %(Gender Code)s, %(Gender Description)s,\
                %(Race Code)s, %(Race Description)s, %(County Code)s, %(County Name)s, %(Population)s)"

    for row in populationData:
        cursor.execute(existsQuery, row)

        # If duplicate doesnt exist for the row
        if cursor.fetchone()[0] == False:
            cursor.execute(insertQuery, row)
            print("Inserted Data: ", row)
            
    conn.commit()
    print("Insert Committed")
    print("\n\nFinished Inserting Population Data", end="\n\n")

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
        data = createEmploymentDictionary(d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16])

        # Get data where 
            # Area Type = County
            # Year >= 2003
            # NAICS is 0 or 1
                # In other words where NAICS Title is Total, All Private or Total, All Industries
        if data['Area Type'] == 'County'and int(data['Year']) >= 2003 and (int(data['NAICS']) == 0 \
            or int(data['NAICS']) == 1):
            dictionaryData.append(data)

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
        data = createPopulationDictionary(d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16], d[17])
        
        if int(data["Age Group Code"])== 0 and int(data["Gender Code"]) == 0 and int(data["Race Code"]) == 0:
            dictionaryData.append(data)

    return dictionaryData

# Create the schema
def createSchema():
    cursor = conn.cursor()
    setup_queries = open('schema.sql', 'r').read()
    cursor.execute(setup_queries)
    conn.commit()

def main():
    # TODO invoke your code to load the data into the database   

    # Creates the schema
    createSchema()

    # Retrieves the data from the datasets
    employmentData = loadEmploymentData()
    populationData = loadPopulationData()

    insertEmploymentData(employmentData)
    insertPopulationData(populationData)

if __name__ == "__main__":
    main()
