import psycopg2
import psycopg2.extras



class Queries:
    def __init__(self, connection_string):
        self.conn = psycopg2.connect(connection_string)

    def populationChange(self, county, year1, year2):
        #this query takes a county and 2 years and return the difference in population between the 2 years. Make sure to check that the users are entering valid years and county names.
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT Population.population - pop2.population from \
            Population inner join Population as pop2 on Population.countyName = pop2.countyName where upper(Population.countyName) \
                like upper(%s) and Population.year = %s and pop2.year = %s and Population.ageGroupCode = %s and pop2.ageGroupCode = %s \
                    and Population.genderCode = %s and pop2.genderCode = %s and Population.raceCode = %s and pop2.raceCode = %s"
        cursor.execute(query, (county, year1, year2, 0, 0, 0, 0, 0, 0,))
        records = cursor.fetchone()
        return records

    def employmentChange(self, county, year1, year2):
        #this query takes a county and 2 years and returns the difference in avgEmployment between the 2 years. Make sure to check that users are entering a valid county name and valid years.
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        county = county.strip() + " County"
        cursor.execute("SELECT Employment.averageEmployment - emp2.averageEmployment from (Employment join Employment as emp2 on Employment.area = emp2.area and upper(Employment.area) like upper(%s) and Employment.year = %s and emp2.year = %s and Employment.NAICS = %s and emp2.NAICS = %s)", (county, year1, year2, 0, 0,))
        records = cursor.fetchone()
        return records

    def max(self):
        # query used to see what year had the highest population and what year had the highest avg employment.
        # this query will return a list in which the 0th val is the max population in the dataset, the 1st val is the max avgEmployment in the set, the 2nd val is the year of the largest population, and the 3rd val is the year of the largest avgEmployment
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT Population.population, Employment.averageEmployment, Population.year, Employment.year from Population, Employment where Population.population = (SELECT MAX(population.population) from Population where countyCode > 1) and Employment.averageEmployment = (SELECT MAX(Employment.averageEmployment) from Employment) limit 1")
        records = cursor.fetchone()
        return records
    
    def min(self):
        # query used to see what year had the lowest population and what year had the lowest avg employment.
        # this query will return a list in which the 0th val is the max population in the dataset, the 1st val is the min avgEmployment in the set, the 2nd val is the year of the smallest population, and the 3rd val is the year of the smallest avgEmployment
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT Population.population, Employment.averageEmployment, Population.year, Employment.year from Population, Employment where Population.population = (SELECT MIN(population.population) from Population where countyCode > 1) and Employment.averageEmployment = (SELECT MIN(Employment.averageEmployment) from Employment) limit 1")
        records = cursor.fetchone()
        return records
    
    def getPopulationByCounty(self, county):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * FROM Population where upper(countyName) like upper(%s)"
        cursor.execute(query, (county,))
        records = cursor.fetchall()
        return records

    def getEmploymentByCounty(self, county):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        county = county.strip() + " County"
        query = "SELECT * FROM Employment where upper(area) like upper(%s)"
        cursor.execute(query, (county,))
        records = cursor.fetchall()
        return records