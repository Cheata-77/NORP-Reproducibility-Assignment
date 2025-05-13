from google import genai

client = genai.Client(api_key="")

# model = genai.GenerativeModel("gemini-2.0-flash")

# adding schema

schema = """
You are a MySQL database expert.

You are given the following tables: 

Table: nonprofit_employment_2020
    `Geographic Title` VARCHAR(50) NOT NULL, 
    `NAICS` VARCHAR(5) NOT NULL, 
    `Industry Title` VARCHAR(100) NOT NULL, 
    `Average Establishments` DECIMAL(38, 0) NOT NULL, 
    `Annual Average Employment` DECIMAL(38, 0) NOT NULL, 
    `Total Annual Wages (in thousands)` DECIMAL(38, 0) NOT NULL, 
    `Annual Wages Per Employee` DECIMAL(38, 0) NOT NULL, 
    `Average Weekly Wage` DECIMAL(38, 0) NOT NULL, 
    `Percent Employment 501(c)(3)` DECIMAL(38, 1) NOT NULL, 
    `Wage Ratio` DECIMAL(38, 2) NOT NULL,
    PRIMARY KEY (`Geographic Title`, `NAICS`)

Table: charities_2020 
    `Employee Identification Number` DECIMAL(38, 0) NOT NULL, 
    `Other Employee Benefit` DECIMAL(38, 0) NOT NULL, 
    `Other Salaries and Wages - Total` DECIMAL(38, 0) NOT NULL, 
    `Total Revenue` DECIMAL(38, 0) NOT NULL, 
    `Direct Expenses` DECIMAL(38, 0) NOT NULL, 
    `Total Contributions` DECIMAL(38, 0) NOT NULL, 
    `Number of Employees` DECIMAL(38, 0) NOT NULL, 
    `State` VARCHAR(50) NOT NULL,
    PRIMARY KEY (`Employee Identification Number`)

"""
question = """Based on this schema, write a MySQL query list the total number of charities in each state. Only select states from the 50 states of the USA. Please use backticks (`) for column names that contain spaces."""

prompt = f"{schema}\n\n{question}"

response = client.models.generate_content(
    model="gemini-2.0-flash", contents=prompt
)
print(response.text)
