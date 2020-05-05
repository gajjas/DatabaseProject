import database as d

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
q = d.Queries(connection_string)

print(q.employmentChange("Albany", 2018, 2017))
print(q.populationChange("Albany", 2017, 2016))

print(q.max())
print(q.min())

print(q.getPopulationByCounty("Albany"))
print(q.getEmploymentByCounty("Albany"))