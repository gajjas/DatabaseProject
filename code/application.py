import re
import database

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
q = database.Queries(connection_string)

def validate_county(string: str) -> str:
  if type(string) is not str:
    raise ValueError("a string")

  return string

def validate_year(string: str, start: int=2000, end: int=2019) -> int:
  if not re.fullmatch(r"\d{4}", string):
    raise ValueError("a 4 digit number")

  if not int(string) >= start:
    raise ValueError(f"greater or equal to {start}") 

  if not int(string) <= end:
    raise ValueError(f"less or equal to {end}")

  return int(string)

# List of queries
# query is represented by a tuple with the following values
# (
#   name: str,
#   parameters: list,
#   method: func,
#   outputFormatter: func|None
# )
# name - Name of query, shown in list of options
# parameters - List of parameters needed for `method`
#   Each parameter is a tuple containing
#   name - name of parameter, must match the name of the function parameter
#   parser - function that takes in a string and returns the required type
#             Also validates the input and throws ValueError if the string is not valid
# method - Function that runs the query
# outputFormatter - Function that is given the output of `method` and should return
#                   a string
options = [
  (
    'Change in Employment', 
    [
      ('county_name', validate_county), 
      ('start_year', validate_year), 
      ('end_year', validate_year)
    ], 
    q.employmentChange,
    None
  ),
  (
    'Change in Population', 
    [
      ('county_name', validate_county),
      ('start_year', validate_year), 
      ('end_year', validate_year)
    ],
    q.populationChange,
    None
  ),
  (
    'Find Employment by County', 
    [
      ('county_name', validate_county),
    ],
    q.getEmploymentByCounty,
    None
  ),
  (
    'Find Population by County', 
    [
      ('county_name', validate_county),
    ],
    q.getPopulationByCounty,
    None
  ),
  (
    'Find Highest Employment and Population',
    [],
    q.max,
    lambda result: f" \
    Highest population: {result[0]} ({result[2]})\n \
    Highest average employment: {result[1]} ({result[3]})\n \
    "
  ),
  (
    'Find Lowest Employment and Population',
    [],
    q.min,
    lambda result: f" \
    Lowest population: {result[0]} ({result[2]})\n \
    Lowest average employment: {result[1]} ({result[3]})\n \
    "
  )
]

def print_options():
  print("These are the available commands")
  for index, option in enumerate(options):
    print(f"{index + 1} - {option[0]}")
  print("Q - Exit the program")

def main():
  print("Hello! This is the final project for Database Systems Spring 2020")
  print("It was created by Jack Yannes, Sumanth Gajjala, and Joshua Wu")
  print()

  print("This tool explores the population and employment in New York State over the past two decades")
  print()

  while True:
    print_options()
    print()

    print("Please enter the number of the query you wish to execute")
    user_input = input().strip()
    print()

    if re.match(f"^[1-{len(options)}]$", user_input):
      # User has chosen one of the queries
      index = int(user_input) - 1
      option_name, option_parameters, option_method, option_output_parser = options[index]
      print(f"You have chosen '{index + 1} - {option_name}'")

      # Collect parameter inputs from user
      parameter_inputs = {}
      for parameter in option_parameters:
        parameter_name, parameter_parser = parameter
        parameter_name = parameter_name.replace("_", " ")
        while True: 
          parameter_input = input(f"Please enter the {parameter_name}: ").strip()

          try:
            # Parse parameter and validate input
            parameter_input = parameter_parser(parameter_input)
          except ValueError as e:
            # Invalid format for input
            print(f"ERROR: {parameter_name} must be {str(e)}")
          else:
            # Store parsed value
            parameter_inputs[parameter_name] = parameter_input
            break
      print()

      # Query database with parameters
      result = option_method(**parameter_inputs)
      
      # Use outputFormatter if provided
      if option_output_parser is not None:
        print(option_output_parser(result))
      # else default output formats
      elif not result:
        print("No results")
      else:
        for row in result:
          print(" " * 3, row)
      
      print()

    # Exit the program when user types 'q'
    elif user_input.lower() == "q":
      print("Exiting...")
      break

    # Unknown command
    else:
      print_options()
      print()

      print("Unknown command, please try again.")

# print(q.employmentChange("Albany", 2018, 2017))
# print(q.populationChange("Albany", 2017, 2016))

# print(q.max())
# print(q.min())

# print(q.getPopulationByCounty("Albany"))
# print(q.getEmploymentByCounty("Albany"))

if __name__=="__main__":
  main()