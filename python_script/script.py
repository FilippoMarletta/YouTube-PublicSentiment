import sys
import time
import requests
from collections import deque
from googleapiclient.discovery import build
from urllib.parse import urlparse, parse_qs
import Secrets


# Configura le credenziali delle API
API_KEY = Secrets.DEVELOPER_KEY
PARTS = 'snippet,replies'
ORDER = 'time'  # Preleva dal più recente al più vecchio
MAX_RESULTS = 100
ID_LIMIT = 500 # Numero massimo di ID da mantenere in memoria

# URL dell'istanza di Logstash
LOGSTASH_URL = 'http://host.docker.internal:9090'

# Inizializza il servizio delle API di YouTube
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Deque per tenere traccia degli ID dei commenti già letti
processed_comment_ids = deque(maxlen=ID_LIMIT)

def get_video_id(url):
    try:
        # Estrai l'ID del video dall'URL
        query = urlparse(url).query
        params = parse_qs(query)
        video_id = params.get('v')
        if video_id:
            return video_id[0]
        else:
            raise ValueError('URL del video non valido')
    except Exception as e:
        print(f"Errore nell'estrazione dell'ID del video: {e}")
        sys.exit(1)


def get_comments(video_id, page_token=None):
    try:
        # Richiedi i commenti del video
        request = youtube.commentThreads().list(
            part=PARTS,
            videoId=video_id,
            order=ORDER,
            pageToken=page_token,
            maxResults=MAX_RESULTS
        )
        response = request.execute()
        return response
    except Exception as e:
        print(f"Errore nella richiesta dei commenti: {e}")
        return None


def send_to_logstash(comment):
    try:
        # Invia i commenti a Logstash
        response = requests.post(LOGSTASH_URL, json=comment)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Errore nell'invio a Logstash: {e}")
        
def process_comment(comment):
    global comment_count
    try:
        snippet = comment['snippet']['topLevelComment']['snippet']
        id = comment['id']
        author = snippet['authorDisplayName']
        text = snippet['textDisplay']
        published_at = snippet['publishedAt']
        like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']

        comment_data = {
            'type': 'comment',
            'id': id,
            'published_at': published_at,
            'author': author,
            'text': text,
            'like_count': like_count
        }
          
        print(f"{id} - {published_at} - {author}: {text}")

        # Invia il commento a Logstash
        send_to_logstash(comment_data)

        # Aggiungi l'ID del commento alla deque
        processed_comment_ids.append(comment['id'])
        comment_count += 1
    except KeyError as e:
        print(f"Errore nel processare un commento: {e}")

if __name__ == '__main__':
    try:
        if len(sys.argv) != 2:
            print('Uso: python script.py [url]')
            sys.exit(1)
    
        video_url = sys.argv[1]
        video_id = get_video_id(video_url)
        print(f"ID del video: {video_id}")
        next_page_token = None
        stop = False
        global comment_count
        comment_count = 0
        prev_comment_count = 0
        
        while True:
            response = get_comments(video_id, next_page_token)
            if response is None:
                print("Errore nel recupero dei commenti, riprovo dopo 10 minuti...")
                time.sleep(600)
                continue

            comments = response.get('items', [])
            
            for comment in comments:
                comment_id = comment['id']
                if comment_id in processed_comment_ids:
                    stop = True
                    break 
                prev_comment_count = comment_count
                process_comment(comment)

            #print(f"comment_count: {comment_count} prev_comment_count: {prev_comment_count}")
            if comment_count > prev_comment_count:
                print(f"Numero totale di commenti ritirati fino ad ora: {comment_count}")
            prev_comment_count = comment_count
            
            if stop:   # abbiamo finito di leggere i commenti futuri per il momento
                next_page_token = None
                stop = False
                time.sleep(10) 
            elif not stop and next_page_token:    # stiamo leggendo i commenti scritti prima dell'avvio dello script
                next_page_token = response.get('nextPageToken')
            elif not stop and not next_page_token:  # stiamo iniziando a leggere i commenti futuri
                next_page_token = None  
                time.sleep(10)  
    except Exception as e:
        print(f"Errore critico: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Script interrotto dall'utente")
        sys.exit(0)
