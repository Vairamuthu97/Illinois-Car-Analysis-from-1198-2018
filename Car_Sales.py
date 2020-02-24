"""
Project Name: Car Analysis from 1196-2018
Project by  : Vairamuthu Jayakumar
NOTE        : Every time the program runs, the File will update automatically 
              and the graphs will save in the designated folder. 
              Please read AboutMe.txt for to learn about this project. 
              Thank you.
    
"""

import sqlite3 as SQL
import numpy as np
import matplotlib.pyplot as plt
import os
from itertools import chain
from tabulate import tabulate


# creating a Database Table
connect = SQL.connect("Car_Sales\Car_Sales_DataBase.db")
C = connect.cursor()

def create_Table():
    SQL1 = """  CREATE TABLE IF NOT EXISTS Project_Vehicle_Licenses (
              Model TEXT, 
              Model_Year INT,
              Fuel_Source TEXT,
              City TEXT,
              Vehicle_Type TEXT,
              Status TEXT,
              Color TEXT,
              Car_Company_name TEXT
              )"""

    C.execute(SQL1)
    C.execute("DELETE FROM Project_Vehicle_Licenses")
    connect.commit()

# creating a Database Columns
def Data_Entry():
    lineNumber = 0
    with open ('Car_Sales\Public_Passenger_Vehicle_Licenses.csv','r') as CSV_file:
        for line in CSV_file:
            if not line.startswith("Vehicle Model"):
                lineNumber = lineNumber + 1

                L = line.split(",")
                Vehicle_Type = L[0]
                Model = L[3]
                Model_Year = L[4]
                Fuel_Source = L[6]
                City = L[8]
                Status = L[1]
                Color = L[5]
                Car_Company_name = L[2]

                SQL2 = """ INSERT INTO Project_Vehicle_Licenses (Model, Model_Year,Fuel_Source,City,Vehicle_Type,Status,Color,Car_Company_name)
                        VALUES (:Model, :Model_Year,:Fuel_Source,:City,:Vehicle_Type,:Status,:Color,:Car_Company_name)"""

                SQL_D = {"Model":Model,"Model_Year":Model_Year,"Fuel_Source":Fuel_Source,"City":City,"Vehicle_Type":Vehicle_Type,"Status":Status,"Color":Color,"Car_Company_name":Car_Company_name}
                print("inserting ",lineNumber,line)
                C.execute(SQL2,SQL_D)
                connect.commit()

# Fetching all values from SQL 

def FETCH_ALL ():
    SQL3 = "SELECT * FROM Project_Vehicle_Licenses"
    FETCH = C.execute(SQL3)
    DATA = FETCH.fetchall()
    for i in DATA : 
        print(i)
    print("\n \t\t\t<<< ALL DATA TRANSFERRED TO DATABASE >>>\n")

#--------------------------------------------------------------------------------------#

def NOTE():
    NOTE = """ 
    Project Name: Car Analysis from 1196-2018
    Project by: Vairamuthu Jayakumar
    NOTE: Every time the program runs, the File will update automatically 
      and the graphs will save in the designated folder. 
      Please read AboutMe.txt for to learn about this project. 
      Thank you.
      """
    return NOTE

def convert_bytes(Zise):
    for x in ['bytes','KB','MB','GB','TB']:
        if Zise < 1024.0:
            return "%3.1f %s" %(Zise,x)
        Zise/= 1024.0

def check_FileSize():
    Orginal_CSV_file_info = os.stat('Car_Sales\Public_Passenger_Vehicle_Licenses.csv')
    py_file_info = os.stat('Car_Sales\Car_Sales.py')
    return f"""\n \t\t\t<<< 1 >>>
            \n Datbase File Size: {convert_bytes(Orginal_CSV_file_info.st_size)} \n Python File Size: {convert_bytes(py_file_info.st_size)}"""

def Types_fuel():
    C.execute("SELECT Fuel_Source, count(Fuel_Source) FROM Project_Vehicle_Licenses GROUP BY Fuel_Source")
    all_fuel = C.fetchall()[:9]

    C.execute("SELECT count(*) FROM Project_Vehicle_Licenses")
    tot = C.fetchone()[0]

    C.execute("SELECT count(*) FROM Project_Vehicle_Licenses WHERE Fuel_Source=?", ("Hybrid",))
    ful = C.fetchone()[0]

    C.execute("""  SELECT Fuel_Source  FROM Project_Vehicle_Licenses 
                    GROUP BY Fuel_Source ORDER BY count(Fuel_Source) DESC LIMIT 1 """)
    Top_Ful = C.fetchone()[0]

    percent = ( ful / tot ) * 100
    
    return f"""\n\t\t\t<<< 2 >>> \n {(tabulate(all_fuel,headers=["Fuel Type","Numer of Vehicles uses"],tablefmt='psql'))}
                \n Total fuel source: {tot} 
                \n Highly uses fuel Source: {Top_Ful} about {round(percent,1)}% people uses {Top_Ful}
                """

def DifferentTypes_Car():

    SQl = """SELECT DISTINCT Car_Company_name
             FROM Project_Vehicle_Licenses 
        """
    C.execute(SQl)
    all_car_name = C.fetchall()[2:]

    SQL = """
            SELECT Car_Company_name as Company, Model, count(*) as qty
            FROM Project_Vehicle_Licenses
            GROUP BY Car_Company_name, Model 
            ORDER BY Car_Company_name, Model DESC LIMIT -1 OFFSET 1 ;
            """
    C.execute(SQL)
    Car_Name_and_Model = C.fetchall() 
    return f"""\n
    
    \t\t\t<<< 3 >>> \n vehicle company names and Number of models \n 
    {(tabulate(all_car_name,headers=["vehicle Company Names","Number of vehicles in the Database"],tablefmt='psql'))} 
    
    \n vehicle company names, Vehincal Model Name and Number of models \n 
    {(tabulate(Car_Name_and_Model,headers=["vehicle Company Names","vehicle Model Names","Number of vehicles Models in the Database"],tablefmt='psql'))} 
    """

def Common_Model():
 
    SQl = "select model, count (model) From Project_Vehicle_Licenses where ifnull(model, '') != '' group by model order by count (model) desc limit 1"
    C.execute(SQl)
    row = C.fetchone()
    car = row[0]
    count = row[1]
    
    SQl = """
        WITH TOTAL AS
        ( SELECT Car_Company_name, Model, Model_Year, COUNT(*) as Total_Models 
        FROM Project_Vehicle_Licenses
        GROUP BY Car_Company_name, Model )

        SELECT Car_Company_name, MODEL, Model_Year from TOTAL 
        GROUP BY Car_Company_name
        HAVING Total_Models = MAX(Total_Models);
        """
    C.execute(SQl)
    Best_Model = C.fetchall()[1:]
    return f"""\n\t\t\t<<< 4 >>> \n Most common/preferred Vehicle model: {car} \n Number of {car} runing in Illinois: {count} 
                \n Popular Vehicle models from each company: \n 
                {(tabulate(Best_Model,headers=["vehicle Company Names","vehicle Model Names","vehicle Model Year"],tablefmt='psql'))}
                
    
    """

def Preferred_Color():
     
    SQl = """
    WITH TOTAL AS
    (
    SELECT DISTINCT Color, (COUNT(*)*100.0/(SELECT COUNT(*) OVER() 
    FROM Project_Vehicle_Licenses)) 
    as percentage  FROM Project_Vehicle_Licenses
    GROUP BY Color
    ORDER BY percentage  )

    SELECT Color, (ROUND(percentage,1))as Preferred_Color_in_Percentage  FROM total
    GROUP BY Color, percentage
    ORDER BY percentage DESC
    ;
    """
    C.execute(SQl)
    color = C.fetchall()[:5]
    return f"""\n\t\t\t<<< 5 >>> \n Top 5 Customer Preferred Color : 
                \n
            {(tabulate(color,headers=["Color","Preferred color in %"],tablefmt='psql'))}  
            """

def year():
    SQL = """
            SELECT Model_Year,count(*) From Project_Vehicle_Licenses
            GROUP BY Model_Year
            ORDER BY Model_Year DESC LIMIT -1 OFFSET 2;
    """
    C.execute(SQL)
    Year = C.fetchall()[:-1]
    YEAR = []
    values = []
    for row in Year:
        a = []
        YEAR.append(row[0])
        values.append(float(row[1]))
    print (Year)

    plt.legend(Year)
    pos = np.arange(len(Year))

    plt.barh(pos,values,color='blue',edgecolor='red')
    plt.yticks(pos,YEAR)
    plt.xlabel('Car Sold',fontsize=16)
    plt.ylabel('YEAR',fontsize=16)
    plt.title('vehicles sold by year (1196-2018)',fontsize=20)
    plt.show()

def Vehicle_type():
    SQL = """
            SELECT DISTINCT Vehicle_Type,count(*)
            FROM Project_Vehicle_Licenses
            GROUP BY Vehicle_Type
    """
    C.execute(SQL)
    TYPE = C.fetchall()[:-1]
    car_type = []
    values = []
    for row in TYPE:
        a = []
        car_type.append(row[0])
        values.append(float(row[1]))
    print (TYPE)

    y_pos = np.arange(len(car_type))

    plt.bar(y_pos,values,color=(0.2,0.4,0.6,0.6))
    plt.xticks(y_pos, car_type)
    plt.xlabel('Types of Vehicle',fontsize=16)
    plt.ylabel('Number of Vehicle',fontsize=16)
    plt.title('Types of vehicle used from 1196-2018 ',fontsize=20)
    plt.show()



def Main_FunctionRun():

    Note = NOTE()
    size = check_FileSize()
    fuel = Types_fuel()
    car_type = DifferentTypes_Car()
    car_model = Common_Model()
    color = Preferred_Color()
    

    return Note + '\n' + size + '\n' + fuel + "\n" + car_type + '\n' + car_model + '\n' + color 



def main():
    
#----------------- PLEASE COMMAND THESE FUNCTIONS AFTER THE FIRST RUN ----------------#
    # create_Table() #SQL query creating fresh Database
    # Data_Entry() #SQL query creating Entry Database
    # FETCH_ALL() #SQL query Fetching all datas from orginal given  Database
#--------------------------------------------------------------------------------------#
    print (Main_FunctionRun())

    year()
    Vehicle_type()

    W_file = open('Car_Sales\Carproject_FinalResult.txt','w') 
    W_file.write(str(Main_FunctionRun()))
    W_file.close()

main()