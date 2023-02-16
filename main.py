# Database System

# Database document connection
from database import collection_tweets, collection_errors, collection_keywords
#Librarty Security
from dotenv import load_dotenv
# Dependencies and libraries
from pandas.io.common import uses_relative
from numpy.ma.extras import unique
# Library Tweet Extraction
from twarc import Twarc2, expansions
# Database
from pymongo import errors
from pymongo import MongoClient
# Extra libraries
import os
import pandas as pd 
import datetime 

print("<------------ MAIN.PY EXECUTION ------------>\n")

# CREDENTIAL / TWITTER API
def call_twarc():
    try:
        load_dotenv()
        CREDENTIAL= os.getenv("BEAR_TOKEN")
        client = Twarc2(bearer_token=f"{CREDENTIAL}")
        print("Credenciales correctamente ingresadas")
        return client
    except:
        print("Error en el ingreso de credenciales")

# KEYWORDS
def palabras_claves():
  # READ FILE
  df = pd.read_json("./keywords.json")
  new_df = df['data']
  # VARIABLE TO STORAGE THE KEYWORDS
  hashtag = [] 
  # INSERT IN THE DATABASE
  for i in new_df:
    ing = i['Hashtag']
    hashtag.append(ing)

  if collection_keywords.count_documents({}) == 30:
    print("La base de datos de palabras claves esta COMPLETA \n")
  else:
    # DATABASE INSERTION
    for j in new_df:
        try:
            collection_keywords.insert_one(j) # DATABASE INSERTION
        except Exception as mist:
            print('Error al insertar las palabras claves en la base de datos')
            print("Detalles: ", mist.details)
  return hashtag

# SEARCH
def search_full(keywords,client):
  start_time = datetime.datetime(2022, 7, 1, 0, 0, 0, 0, datetime.timezone.utc)
  end_time = datetime.datetime(2022, 8, 30, 0, 0, 0, 0, datetime.timezone.utc)
  search_results = client.search_all(query=keywords + ' lang:es -is:retweet', start_time=start_time, end_time=end_time, max_results=100)
  print("|---- PALABRA CLAVE ")
  print("El query es generado a partir de la palabra: ", keywords)
  palabra = keywords
  return(search_results,palabra)

# MAIN / UPLOAD DATABASE
def main(hashtag):
    #Cantidad maxima de tweets a extraer en total (approximado dependiendo de los max_results de cada loop)
    # Queries
    k = 0
    for i in range(0,len(hashtag)):
        search_total,palabras = search_full(keywords[i],client)
        # Generar la metadata sin tratar en un archivo JSON
        maximo = 200  #NUMERO MAXIMO POR CADA CICLO 30*100=TOTAL
        i = 1
        j = 0
        for page in search_total:
            # Obtener toda la informacion en un solo archivo JSON
            result = expansions.flatten(page)
            #Limitar la cantidad de tweets extraidos, si llega a pasar el limite maximo, termina la ejecucion
            if j>maximo:
                print("Termina la ejecución")
                print("\n")
                break
            cant = len(result)
            #Loops por cada max_results(La extraccion la hace por bloques dependiendo el max) 
            print("|---- INGRESO DE LOS TWEETS")
            print("Loops: ", i)
            print("Len max (max_result): ",cant)
            i = i+1
            #Guardar los datos en un diccionario
            for user in result:
                j = j+1
                k = k+1
                #Ingresar la palabra de busqueda
                user['_id'] = user['id']
                user['palabra'] = palabras
                user['inserted_at_database'] = datetime.datetime.now()
                # resultados["data"].append(user)
                # Insertar en la base de datos 
                try: 
                    collection_tweets.insert_one(user) # Colección Tweet
                    print("ID Tweet Ingresado: ", user['_id'])
                #except errors.DuplicateKeyError as duer:
                except Exception as duer:
                    print("Error al ingreso del campo TWEETS en la base de datos")
                    print("Detalles: ", duer.details)
                    print("Codigo: ", duer.code)
                    collection_errors.insert_one({ 'tweet_id': user['_id'], 'error': duer.details})
    
    print("\n")
    print("|------ DESCRIPCION BASE DE DATOS --------|")
    print("Tweets extraidos: ", k)
    print("Documentos en colección TWEETS: ", collection_tweets.count_documents({}))
    print("Documentos en colección KEYWORDS: ", collection_keywords.count_documents({})) 
    print("Documentos en colección ERRORS: ", collection_errors.count_documents({})) 
    print("|-----------------------------------------|")
    print("\n")


  
# CALL EACH FUNCTION -> FULL SEARCH

client = call_twarc() # Call Credentials
keywords = palabras_claves() # Storage keywords
resultados = main(keywords) # Full search (Main function)