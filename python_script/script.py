import sys
import time
import requests
import html
import re
import os
from googleapiclient.discovery import build
from urllib.parse import urlparse, parse_qs
from get_docker_secret import get_docker_secret


# configuration variables for YouTube API
API_KEY = get_docker_secret('youtube_api_key')
PARTS = 'snippet,replies'
ORDER = 'time'  # Order comments by creation date from most recent to least recent
MAX_RESULTS = 30


# URL of Logstash instance
LOGSTASH_URL = 'http://logstash-container:9090'

# Initialize YouTube API service
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Deque to keep track of processed comment IDs
processed_comment_ids = set()


def remove_html_tags(text):
    """Remove HTML tags from a string"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def get_video_id(url):
    try:
        # Extract video ID from URL
        query = urlparse(url).query
        params = parse_qs(query)
        video_id = params.get('v')
        if video_id:
            return video_id[0]
        else:
            raise ValueError('Invalid video URL')
    except Exception as e:
        print(f"Error extracting video ID: {e}")
        sys.exit(1)


def get_comments(video_id, page_token=None):
    try:
        # Request video comments
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
        print(f"Error requesting comments: {e}")
        return None


def send_to_logstash(comment):
    try:
        # Send comments to Logstash
        response = requests.post(LOGSTASH_URL, json=comment)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error sending to Logstash: {e}")
        
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

        # Send the comment to Logstash
        send_to_logstash(comment_data)

        # Add the comment ID to the deque
        processed_comment_ids.add(id)
        comment_count += 1
        return True
    except KeyError as e:
        print(f"Error processing a comment: {e}")
        return False

if __name__ == '__main__':
    try:
        # Get video URL from environment variable
        video_url = os.getenv('VIDEO_URL')
        if not video_url:
            print("Error: VIDEO_URL environment variable not set")
            sys.exit(1)
            
        print(f"Video URL: {video_url}")
        
        video_id = get_video_id(video_url)
        print(f"Video ID: {video_id}")
        
        global comment_count
        comment_count = 0
        page_token = None
        time.sleep(60)
        while True:
            response = get_comments(video_id, page_token)
            if response is None:
                print("Error retrieving comments, retrying after 10 minutes...")
                time.sleep(600)
                continue

            comments = response.get('items', [])
            
            if not comments:
                print("No new comment found. Waiting for 30 seconds...")
                time.sleep(30)
                page_token = None
                continue

            new_comments = []
            for comment in comments[::-1]:  # Process comments from oldest to newest
                if comment['id'] not in processed_comment_ids:
                    new_comments.append(comment)
                else:
                    break  # Reached a processed comment

            for comment in new_comments:
                process_comment(comment)
                
            print(f"Total number of comments retrieved so far: {comment_count}")

            if len(new_comments) == len(comments):  # All comments were new
                page_token = response.get('nextPageToken')
            else:
                page_token = None  # Start from the beginning to search for new comments
            
            time.sleep(30)  # Short pause to avoid overloading the API
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Script interrupted by user")
        sys.exit(0)

