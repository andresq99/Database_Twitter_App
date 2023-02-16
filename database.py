from pymongo import MongoClient
from dotenv import load_dotenv
import os

print("<------------ DATABASE.PY EXECUTION ------------>\n")
def credential_database():
    load_dotenv()
    USERNAME = os.getenv("USER_NAME")
    PASSWORD = os.getenv("PASSWORD")
    myclient = MongoClient(f"mongodb+srv://{USERNAME}:{PASSWORD}@pasoslibres.ywniq.mongodb.net/?retryWrites=true&w=majority")

    try: 
        print("Bases de datos existentes: ", myclient.list_database_names())
        print("CONEXION A LA BASE DE DATOS REALIZADA \n")
        return myclient
    except Exception as err:
        print("Error al conectar a la base de datos")
        print("Detalles: ", err.details)
        print(err)
        print("\n")

myclient = credential_database()


# DATABASE CREATION
try:
    db = myclient["twitter_data"]
    # Collections
    collection_tweets = db["Tweets"] #Tweets
    collection_errors = db["Errors"] # Errores
    collection_keywords = db["Keywords"] # Palabras claves
    print("La base de datos y sus colecciones fueron creadas")
    print("EJECUCIÓN REALIZADA. CONTINUAR CON LAS CONSULTAS \n")
except Exception as dtr:
    print("Error al crear las colecciones en la base de datos")
    print("PARA CONTINUAR EL PROGRAMA, VERIFIQUE QUE LAS COLECCIONES EXISTAN")

