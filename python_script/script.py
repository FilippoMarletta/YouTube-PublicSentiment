import json
import httpx
import asyncio
import sys
import requests
import Secrets

api_key = Secrets.DEVELOPER_KEY
api_service_name = "youtube"
api_version = "v3"

async def get_youtube_data(url):
    
    query = url.split("?")[1]
    params = dict([item.split("=") for item in query.split("&")])
    video_id = params.get("v")

    async with httpx.AsyncClient() as client:
        async with client.stream(
            "GET",
            f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&maxResults=100&key={api_key}",
            headers={"accept": "application/json"},
        ) as response:
            block = ""
            async for line in response.aiter_lines():
                block += str(line)
                if line[0] == "}":
                    data = json.loads(block)
                    block = ""
                    yield data
            
            

def push_to_logstash(data):
    try:
        response = requests.post("http://host.docker.internal:9090", json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Error sending data to Logstash:", e)
    


async def loop_data(url):
    async for d in get_youtube_data(url):
        push_to_logstash(d)
    


if __name__ == "__main__":
    
    if(len(sys.argv) < 2):
        print("Usage: python script.py <url>")
        exit(1)
        
    print("Youtube data gatherer started") 
       
    url = sys.argv[1]
    asyncio.run(loop_data(url))
    print("Youtube data gathering...")
    
    
    
    
   
