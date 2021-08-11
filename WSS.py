import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password, databasename):   ###### Server Connection Method
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=databasename
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute(connection, query):   ###### Database creation Method
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")



####################################################################################################     WEB SCRAPING PART

URL = "https://en.wikipedia.org/wiki/Scorpions_discography"

html_code = requests.get(URL) #getting the URL

file_page = open("Scorpions_Discography_HTML.txt","a+",encoding='utf-8')
file_page.writelines(html_code.text) #writing the HTML content to a txt file


soup = BeautifulSoup(html_code.content, "html.parser")

results = soup.find(class_="wikitable plainrowheaders") #getting the class that contains the album table
disc_elements = results.find("tbody") #getting the table contents


dis_el_df=pd.read_html(str(results))
# convert list to dataframe
dis_el_df=pd.DataFrame(dis_el_df[0])


final_data = dis_el_df[['Title','Album details']]

final_data_list = final_data.values.tolist()

final_data_tuple=tuple(final_data_list) # if you want to convert it into a tuple

print(final_data_tuple)


########################################################################################## WEB SCRAPING PART IS DONE NOW LET'S WRITE THE DATA TO A DATABASE



mydb=create_server_connection("localhost","*******","*******","Scorpions_Discography")#creating server connection

#Sc_DB="CREATE DATABASE Scorpions_Discography"      ##########creating the database Scorpions_Discography for the first time
#execute(mydb, Sc_DB)


mydb_cursor=mydb.cursor()

#mydb_cursor.execute("CREATE TABLE Discography_S (TITLE varchar(250) not null, INFORMATION varchar(250) not null)")    ########creating the table Discography_S for the first time
#mydb.commit()


insert_info = """
INSERT INTO Discography_S
(TITLE, INFORMATION)
VALUES ( %s, %s )
"""

with mydb.cursor() as cursor:
    cursor.executemany(insert_info, final_data_list)
    mydb.commit()

#### PRINTING PART

mydb_cursor.execute("SHOW DATABASES")

## 'fetchall()' method fetches all the rows from the last executed statement
databases = mydb_cursor.fetchall() ## it returns a list of all databases present

## printing the list of databases
print(databases)

## showing one by one database
for database in databases:
    print(database)


####

mydb_cursor.execute("SHOW TABLES")

tables = mydb_cursor.fetchall() ## it returns list of tables present in the database

## showing all the tables one by one
for table in tables:
    print(table)


####


## defining the Query
query = "SELECT * FROM Discography_S"

## getting records from the table
mydb_cursor.execute(query)

## fetching all records from the 'cursor' object
records = mydb_cursor.fetchall()

## Showing the data
for record in records:
    print(record)
