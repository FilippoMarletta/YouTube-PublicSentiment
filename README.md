# YouTube-PublicSentiment

**YouTube-PublicSentiment** is a tool designed for sentiment analysis, entity sentiment analysis, and emotion recognition of public comments left under YouTube videos. The primary goal of this tool is to provide content creators, businesses, and even curious users with valuable insights into the audience's perception and the emotions expressed in the comments.

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)


## Setup

1. **Clone the Repository:**
 ```bash
   git clone https://github.com/FilippoMarletta/YouTubePublicSentiment.git
   cd YouTube-PublicSentiment
```
2. Create a `.secret` file in `python_script/` directory and add your YouTube API key by executing this snippet in the main directory:
```bash
    echo "[YOUR_API_KEY]" > python_script/.secret
```
3. Create another `.secret` file in `spark/` directory and add your Google Cloud Natural Language API access token in json format, It should look something like this:
```json
    {
    "type": "service_account",
    "project_id": "project-id",
    "private_key_id": "some_number",
    "private_key": "-----BEGIN PRIVATE KEY-----\n....
    =\n-----END PRIVATE KEY-----\n",
    "client_email": "<api-name>api@project-id.iam.gserviceaccount.com",
    "client_id": "...",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://accounts.google.com/o/oauth2/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/...<api-name>api%40project-id.iam.gserviceaccount.com"
    }
```

## Usage
                              

1. Specify the YouTube Video URL:
   Modify the `VIDEO_URL` environment variable in `docker-compose.yml`:
    ```
        services:
        youtube_comments:
            hostname: youtube_comments
            build: ./python_script
            container_name: youtube_comments
            volumes:
            - ./python_script/script.py:/app/script.py
            secrets:
            - youtube_api_key
            environment:
            - VIDEO_URL=https://www.youtube.com/watch?v=YourVideoID
            entrypoint:
            - "python"
            - "/app/script.py"
            tty: true
            depends_on:
            logstash:
                condition: service_healthy
    ```
    Replace `https://www.youtube.com/watch?v=YourVideoID` with the URL of the YouTube video you wish to analyze.

2. start all container with this command:
```bash
    docker compose up
```
3. Open Kibana in your browser to see the dashboard:
    - ![kibana](https://127.0.0.1:5601)

## Actually supported lenguages:
- English(`en`)






