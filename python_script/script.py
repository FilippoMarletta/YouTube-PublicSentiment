import sys
import time
import requests
import html
import re
from googleapiclient.discovery import build
from urllib.parse import urlparse, parse_qs
from get_docker_secret import get_docker_secret


# Configura le credenziali delle API
API_KEY = get_docker_secret('youtube_api_key')
PARTS = 'snippet,replies'
ORDER = 'time'  # Ordina i commenti per data di creazione dal più recente all'Meno recente
MAX_RESULTS = 30


# URL dell'istanza di Logstash
LOGSTASH_URL = 'http://logstash-container:9090'

# Inizializza il servizio delle API di YouTube
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Deque per tenere traccia degli ID dei commenti già letti
processed_comment_ids = set()


def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

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
        if id in processed_comment_ids:
            return

        author = snippet['authorDisplayName']
        text = remove_html_tags(html.unescape(snippet['textDisplay']))
        published_at = snippet['publishedAt']
        like_count = snippet['likeCount']

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
        processed_comment_ids.add(id)
        comment_count += 1
        return True
    except KeyError as e:
        print(f"Errore nel processare un commento: {e}")
        return False

if __name__ == '__main__':
    try:
        # Controllo argomenti
        if len(sys.argv) < 2:
            print("Usage: python script.py <url>")
            sys.exit(1)
        
        video_url = sys.argv[1]
        print(f"URL del video: {video_url}")
        
        video_id = get_video_id(video_url)
        print(f"ID del video: {video_id}")
        
        global comment_count
        comment_count = 0
        page_token = None
        time.sleep(30)
        while True:
            response = get_comments(video_id, page_token)
            if response is None:
                print("Errore nel recupero dei commenti, riprovo dopo 10 minuti...")
                time.sleep(600)
                continue

            comments = response.get('items', [])
            
            if not comments:
                print("Nessun nuovo commento trovato. Attendo 35 secondi...")
                time.sleep(30)
                page_token = None
                continue

         

            new_comments = []
            for comment in comments:  # Invertiamo l'ordine per processare dal più vecchio al più recente
                if comment['id'] not in processed_comment_ids:
                    new_comments.append(comment)
                else:
                    break  # Abbiamo raggiunto un commento già processato

            for comment in new_comments:
                process_comment(comment)
                
            print(f"Numero totale di commenti ritirati fino ad ora: {comment_count}")

            if len(new_comments) == len(comments):  # Tutti i commenti erano nuovi
                page_token = response.get('nextPageToken')
            else:
                page_token = None  # Ricomincia dall'inizio per cercare nuovi commenti
            
            time.sleep(30)  # Breve pausa per non sovraccaricare l'API
    except Exception as e:
        print(f"Errore critico: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Script interrotto dall'utente")
        sys.exit(0)
