

# importing necessary libraries
import mysql.connector
import pandas as pd

def connect_db():
     # DB Connection
    try:
        database = mysql.connector.connect(
            host="localhost",
            user="root",
            database="hospital",
            password= "akashyap")
        return database
    except Exception as e:
        print("[ERROR] Error in connecting database error :- {}".format(str(e)))
        return None

def get_country_df(country):
    database = connect_db()
    if database:
        df = pd.read_sql('SELECT * FROM patients', con=database)  # fitting into pandas dataframe
        country_df = df[df['Country'] == country]
        return country_df

def get_file(country):
    data = get_country_df(country)
    file_name = str(country)
    data.to_csv('C:/Users/AKASH/Desktop/incubyte/' + country + ".csv")  # replace path with your desired path
    print("File has been created to the specified path")


# Call get_file function to get country wise file
get_file("IND")









