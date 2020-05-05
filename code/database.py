import psycopg2
import psycopg2.extras



class Queries:

	def __init__(self, connection_string):
        self.conn = psycopg2.connect(connection_string)

    def populationChange(self, county, year1, year2):
        #this query takes a county and 2 years and return the difference in population between the 2 years. Make sure to check that the users are entering valid years and county names.
    	cursor = self.conn.cursor()
    	cursor.execute("SELECT Population.population - pop2.population from Population inner join Population as pop2 on Population.countyName = pop2.countyName where upper(Population.countyName) like upper(%s) and Population.year = %d and pop2.year = %d and Population.ageGroupCode = %d and pop2.ageGroupCode = %d and Population.genderCode = %d and pop2.genderCode = %d and Population.raceCode = %d and pop2.raceCode = %d", (county, year2, year1, 0, 0, 0, 0, 0, 0,))
    	records = cursor.fetchall()
    	result = [records]
    	return result[0]

    def employmentChange(self, county, year1, year2):
        #this query takes a county and 2 years and returns the difference in avgEmployment between the 2 years. Make sure to check that users are entering a valid county name and valid years.
    	cursor = self.conn.cursor()
        county += "county"
        cursor.execute("SELECT Employment.averageEmployment - emp2.averageEmployment from Employment inner join Employment as emp2 on Employment.area = emp2.area where upper(Employment.area) like upper(%s) and Employment.year = %d and emp2.year = %d and Employment.NAICS = %d and emp2.NAICS = %d", (county, year2, year1, 0, 0,))
        records = cursor.fetchall()
        result = [records]
        return result[0]

    def max(self):
        # query used to see what year had the highest population and what year had the highest avg employment.
        # this query will return a list in which the 0th val is the max population in the dataset, the 1st val is the max avgEmployment in the set, the 2nd val is the year of the largest population, and the 3rd val is the year of the larges avgEmployment
        cursor = self.conn.cursor()
        cursor.execute("SELECT Population.population, Employment.averageEmployment, Population.year, Employment.year from Population where Population.population = (SELECT MAX(population.population) from Population) and Employment.averageEmployment = (SELECT MAX(Employment.averageEmployment) from Employment) limit 1")
        record = cursor.fetchall()
        result = [records]
        return result
