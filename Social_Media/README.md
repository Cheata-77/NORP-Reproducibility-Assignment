# Social Media Assignment

* There are 3 scraper files present in this repo which are:
    * bluesky_scraper.py - This file scrapes the posts from the Bluesky social media platform.
    * reddit_scraper.py - This file scrapes the posts from the Reddit social media platform.
    * youtube_scraper.py - This file scrapes the posts from the Youtube social media platform.

* Please choose any 2 of the above files and run them to scrape posts regarding ngos using the ngos_list.py file which is used in all the 3 scrapers.
* The files are designed to scrape posts as per specific time_intervals.

### Deliverables

* Please provide a report of the scraping process of the 2 files you have chosen and the brief description of the data you have scraped for a chosen time interval.

### Environment Setup

* Library requirements
    * praw
    * pandas
    * python-dotenv 
    * requests
    * pytest
    * lxml
    * thefuzz
    * google-api-python-client

* Prior to running the scrapers, the .env file has to be populated with the respective API keys, the instructions below:

#### Reddit API Credentials

1. **Create a Reddit App**:

   - Go to [Reddit App Preferences](https://www.reddit.com/prefs/apps).
   - Click **Create App** or **Create Another App**.
   - Fill out the form:
     - **Name**: Choose a name for your app (e.g., "Reddit Scraper").
     - **App type**: Select **script**.
     - **Redirect URI**: Enter `http://localhost:8080`.

2. **Retrieve Your Credentials**:

   - After creating the app, copy your `client_id`, `client_secret`, and `user_agent`.

#### YouTube API Credentials

1. **Create a Google Cloud Project**:

   - Visit the [Google Cloud Console](https://console.cloud.google.com/).
   - Create a new project or select an existing one.

2. **Enable YouTube Data API v3**:

   - Navigate to **APIs & Services** > **Library**.
   - Search for **YouTube Data API v3** and enable it.

3. **Create API Credentials**:

   - Go to **APIs & Services** > **Credentials**.
   - Click **Create Credentials** > **API Key**.
   - Copy the generated API key.

### .env File

Create a `.env` file in the root directory to store your API credentials. You can reference the `.env.sample` file provided in the repository for the required variables.

```env
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
YOUTUBE_API_KEY=your_youtube_api_key
OPENAI_API_KEY=your_openai_api_key
```

### Credit

* This project was done by Kathie Huynh and Hsiang-Pin Ko.

