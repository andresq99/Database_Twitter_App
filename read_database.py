from pymongo import MongoClient
from pprint import pprint
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()
USERNAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")

# URI de conexión a MongoDB Atlas, usando f-string para interpolar las variables
mongo_uri = f"mongodb+srv://{USERNAME}:{PASSWORD}@pasoslibres.ywniq.mongodb.net/"

# Conéctate al cliente de MongoDB
client = MongoClient(mongo_uri)

# Accede a la base de datos
db = client['twitter_data']

# Accede a la colección
collection = db['Tweets']

# Recupera 20 documentos de la colección
documentos = collection.find().limit(20)

# Códigos de escape ANSI para texto en negrita
bold_start = "\033[1m"
bold_end = "\033[0m"

# Función para formatear y mostrar los datos de forma ordenada
def mostrar_tweet(tweet):
    print(f"{bold_start}ID del Tweet:{bold_end} {tweet.get('_id')}")
    print(f"{bold_start}Texto:{bold_end} {tweet.get('text', 'No disponible')}")
    
    # Autor
    author = tweet.get('author', {})
    print(f"{bold_start}Autor:{bold_end} {author.get('name', 'No disponible')}")
    print(f"{bold_start}Username:{bold_end} {author.get('username', 'No disponible')}")
    print(f"{bold_start}Ubicación:{bold_end} {author.get('location', 'No disponible')}")
    
    # Métricas públicas
    public_metrics = tweet.get('public_metrics', {})
    print(f"{bold_start}Retweets:{bold_end} {public_metrics.get('retweet_count', 0)}")
    print(f"{bold_start}Likes:{bold_end} {public_metrics.get('like_count', 0)}")
    print(f"{bold_start}Respuestas:{bold_end} {public_metrics.get('reply_count', 0)}")
    
    # Otros campos
    print(f"{bold_start}Fecha de Creación:{bold_end} {tweet.get('created_at', 'No disponible')}")
    print(f"{bold_start}Idioma:{bold_end} {tweet.get('lang', 'No disponible')}")
    
    # Hashtag y metadata
    print(f"{bold_start}Hashtag:{bold_end} {tweet.get('hashtag', 'No disponible')}")
    print(f"{bold_start}Insertado en la base de datos:{bold_end} {tweet.get('inserted_at_database', 'No disponible')}")
    
    print("="*70)  # Separador para cada tweet

# Muestra los documentos en un formato ordenado
for documento in documentos:
    mostrar_tweet(documento)

# Cierra la conexión
client.close()
