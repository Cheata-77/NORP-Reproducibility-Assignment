# youtube/youtube_scraper.py

from googleapiclient.discovery import build
import pandas as pd
import argparse
import os
from dotenv import load_dotenv
import re
from thefuzz import fuzz, process
import json
import sys
import random
from collections import defaultdict
from datetime import datetime, timedelta

from ngos_list import ngos_list  # Import NGOs list from the separate file

# Set a constant for Youtube platform_id.
PLATFORM_ID = 2

# Load API credentials from the .env file
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

# Check if API_KEY is set
if not API_KEY:
    raise ValueError("Please provide a valid YouTube API Key in the .env file.")

# Initialize the YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Define the maximum number of unique channels to scrape by default
DEFAULT_MAX_CHANNELS = 10

# Define the maximum number of comments to scrape per comment depth
DEFAULT_MAX_COMMENTS = 30

# Define whether to fetch nested comments or not
FETCH_NESTED_COMMENTS = False

def get_channel_id_by_name(channel_name):
    """
    Resolve the YouTube channel ID from the channel name.
    Returns None if the channel cannot be found.
    """
    request = youtube.search().list(
        part='snippet',
        q=channel_name,
        type='channel',
        maxResults=1
    )
    try:
        response = request.execute()
    except Exception as e:
        print(f"Error executing search request for channel '{channel_name}': {e}")
        return None

    if response.get('items'):
        return response['items'][0]['snippet']['channelId']
    else:
        return None  # Instead of raising, return None to handle gracefully

def get_video_details(video_id):
    """
    Get details of a specific video by video ID.
    """
    request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=video_id
    )
    try:
        response = request.execute()
    except Exception as e:
        print(f"Error fetching details for video ID '{video_id}': {e}")
        return []

    video_details = []
    for item in response.get('items', []):
        # Convert publishedAt to desired format
        published_at = pd.to_datetime(item['snippet']['publishedAt']).strftime('%Y-%m-%d %H:%M:%S')
        video_data = {
            'external_content_id': video_id,
            'platform_id': PLATFORM_ID,
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'author': item['snippet']['channelTitle'],
            'published_at': published_at,
            'view_count': int(item['statistics'].get('viewCount', 0)),
            'like_count': int(item['statistics'].get('likeCount', 0)),
            'comment_count': int(item['statistics'].get('commentCount', 0)),
            'content_type': 'Video',
            'metadata': json.dumps({
                'duration': item.get('contentDetails', {}).get('duration', None)
            })
        }
        video_details.append(video_data)

    

    return video_details

def get_comment_from_video(video_id, max_results=DEFAULT_MAX_COMMENTS, fetchNested=FETCH_NESTED_COMMENTS):
    """
    Get comments from a specific YouTube video.
    """
    comments = []
    next_page_token = None

    while len(comments) < max_results:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=min(100, max_results - len(comments)),  # Max 100 per request
            order='relevance',
            textFormat='plainText',
            pageToken=next_page_token
        )
        try:
            response = request.execute()
        except Exception as e:
            print(f"Error fetching comments for video ID '{video_id}': {e}")
            break

        items = response.get('items', [])
        for item in items:
            root_comment = item['snippet']['topLevelComment']
            snippet = root_comment['snippet']
            
            # For top-level comments, parent_id should be NULL and reply_count can be extracted if available.
            comment_data = {
                'comment_id': root_comment['id'],
                'post_external_id': video_id,
                'author': snippet.get('authorDisplayName'),
                'body': snippet.get('textDisplay'),
                'created_at': snippet.get('publishedAt'),
                'like_count': snippet.get('likeCount', 0),
                'reply_count': snippet.get('totalReplyCount', 0),
                'parent_id': None,
                'metadata': json.dumps({})
            }
            comments.append(comment_data)

            if fetchNested and 'replies' in item:
                reply_list = item['replies']['comments']
                max_reply_count = min(max_results, len(reply_list))

                for reply in reply_list[:max_reply_count]:
                    reply_snippet = reply['snippet']
                    reply_data = {
                        'comment_id': reply['id'],
                        'post_external_id': video_id,
                        'author': reply_snippet.get('authorDisplayName'),
                        'body': reply_snippet.get('textDisplay'),
                        'created_at': reply_snippet.get('publishedAt'),
                        'like_count': reply_snippet.get('likeCount', 0),
                        'reply_count': 0,  # Replies to a reply do not exist.
                        'parent_id': reply_snippet.get('parentId'),
                        'metadata': json.dumps({})
                    }
                    comments.append(reply_data)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # No more pages

    return comments

def get_videos_from_channel(channel_id, max_results=20, target_date=None, date_range=14):
    """
    Get a list of videos from a specific YouTube channel.
    """
    video_ids = []
    next_page_token = None
    publishedAfter=None
    publishedBefore=None
    
    if target_date:
        target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
        start_date = target_datetime - timedelta(days=date_range)
        end_date = target_datetime + timedelta(days=date_range)

        publishedAfter=start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        publishedBefore=end_date.strftime("%Y-%m-%dT%H:%M:%SZ")


    while len(video_ids) < max_results:
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=min(50, max_results - len(video_ids)),  # Max 50 per request
            order='date',
            type='video',
            pageToken=next_page_token,
            publishedAfter=publishedAfter,
            publishedBefore=publishedBefore
        )
        try:
            response = request.execute()
        except Exception as e:
            print(f"Error fetching videos for channel ID '{channel_id}': {e}")
            break

        items = response.get('items', [])
        for item in items:
            if item['id']['kind'] == 'youtube#video':
                video_ids.append(item['id']['videoId'])
                if len(video_ids) >= max_results:
                    break

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # No more pages

    return video_ids

def find_ngos_in_video(video_data, ngos_list, fuzzy_threshold=90):
    """
    Check if any NGO keywords are in the video's title or description.
    For acronym keywords, match only complete words.
    Includes fuzzy matching with a specified threshold.
    """
    ngos_found = set()
    video_text = f"{video_data['title']} {video_data['description']}"

    # Prepare a mapping from keywords to NGO IDs
    keyword_to_ngo = {}
    for ngo in ngos_list:
        for keyword in ngo['keywords']:
            keyword_to_ngo[keyword] = ngo['ngo_id']

    # Exact and Fuzzy Matching with special handling for acronyms
    for keyword, ngo_id in keyword_to_ngo.items():
        if keyword.isupper():
            # For acronyms, perform a whole-word match using regex.
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, video_text, re.IGNORECASE):
                ngos_found.add(ngo_id)
        else:
            # For non-acronym keywords, use an exact substring match.
            if keyword.lower() in video_text.lower():
                ngos_found.add(ngo_id)
            else:
                # Apply fuzzy matching if an exact match is not found.
                ratio = fuzz.partial_ratio(keyword.lower(), video_text.lower())
                if ratio >= fuzzy_threshold:
                    ngos_found.add(ngo_id)

    return list(ngos_found)

def append_to_csv(df, file_path, columns):
    """
    Append DataFrame to a CSV file without overwriting if the file exists.
    """
    if os.path.exists(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig', columns=columns)
    else:
        df.to_csv(file_path, mode='w', header=True, index=False, encoding='utf-8-sig', columns=columns)

def main():
    # Argument parsing to accept multiple channel names and max results from the command line
    parser = argparse.ArgumentParser(description="YouTube Channel Scraper for Non-profits")
    parser.add_argument(
        "--channel_names",
        type=str,
        nargs='*',
        help="One or more YouTube Channel Names to scrape videos from. If not provided, the scraper will use the NGOs defined in ngos_list."
    )
    parser.add_argument(
        "--max_results",
        type=int,
        default=20,
        help="Number of videos to scrape per channel (default is 20)"
    )
    parser.add_argument(
        "--target_date",
        type=str,
        default=None,
        help="Target date for video publication (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--date_range",
        type=int,
        default=14,
        help="Number of days before and after the target date to search for videos"
    )
    args = parser.parse_args()

    # Determine which channels to scrape
    if args.channel_names:
        # Scrape channels provided via CLI
        channels_to_scrape = []
        for channel_name in args.channel_names:
            print(f"\nStarting to scrape channel: {channel_name}")
            channel_id = get_channel_id_by_name(channel_name)
            if channel_id:
                channels_to_scrape.append({'ngo_id': None, 'channel_name': channel_name, 'channel_id': channel_id})
                print(f"Resolved channel name '{channel_name}' to channel ID: {channel_id}")
            else:
                print(f"Could not resolve channel ID for '{channel_name}'. Skipping.")

        if not channels_to_scrape:
            print("No valid channels provided via CLI. Exiting.")
            return

    else:
        # Use NGOs from ngos_list to determine channels
        channels_to_scrape = []
        ngos_shuffled = ngos_list.copy()
        random.shuffle(ngos_shuffled)  # Shuffle the list to ensure randomness
        print("No channels provided via CLI. Using NGOs defined in ngos_list to determine channels.")

        for ngo in ngos_shuffled:
            ngo_id = ngo['ngo_id']
            ngo_name = ngo['name']
            channel_id = get_channel_id_by_name(ngo_name)

            if channel_id:
                channels_to_scrape.append({'ngo_id': ngo_id, 'channel_name': ngo_name, 'channel_id': channel_id})
                print(f"Resolved NGO '{ngo_name}' to channel ID: {channel_id}")
            else:
                # Attempt to resolve using keywords
                found = False
                for keyword in ngo['keywords']:
                    channel_id_keyword = get_channel_id_by_name(keyword)
                    if channel_id_keyword:
                        channels_to_scrape.append({'ngo_id': ngo_id, 'channel_name': keyword, 'channel_id': channel_id_keyword})
                        print(f"Resolved NGO '{ngo_name}' using keyword '{keyword}' to channel ID: {channel_id_keyword}")
                        found = True
                        break  # Stop after finding the first matching keyword
                if not found:
                    print(f"Could not resolve a YouTube channel for NGO '{ngo_name}' using name or keywords.")

            # Check if we've reached the desired number of channels
            if len(channels_to_scrape) >= DEFAULT_MAX_CHANNELS:
                print(f"Reached the limit of {DEFAULT_MAX_CHANNELS} channels to scrape.")
                break

        if not channels_to_scrape:
            print("No channels could be resolved from ngos_list. Exiting.")
            return

    try:
        all_video_details = []
        all_comments = []
        ngo_content_data = []
        ngos_found_overall = set()

        for channel_info in channels_to_scrape:
            ngo_id = channel_info['ngo_id']
            channel_name = channel_info['channel_name']
            channel_id = channel_info['channel_id']
            print(f"\nScraping channel: {channel_name} (Channel ID: {channel_id})")

            try:
                # Get the list of video IDs from the channel
                video_ids = get_videos_from_channel(channel_id, max_results=args.max_results, target_date=args.target_date, date_range=args.date_range)
                print(f"Found {len(video_ids)} videos in channel '{channel_name}'.")

                # Get details for each video and check for NGO mentions
                for video_id in video_ids:
                    video_details = get_video_details(video_id)
                    for video_data in video_details:
                        ngos_found_in_video = find_ngos_in_video(video_data, ngos_list, fuzzy_threshold=90)
                        all_video_details.extend(video_details)
                        if ngos_found_in_video:
                            for found_ngo_id in ngos_found_in_video:
                                ngos_found_overall.add(found_ngo_id)
                                ngo_content_data.append({
                                    'ngo_id': found_ngo_id,
                                    'external_content_id': video_id
                                })

                    # Get comments for each video
                    comments = get_comment_from_video(video_id, max_results=DEFAULT_MAX_COMMENTS)   
                    all_comments.extend(comments)                      

                print(f"Completed scraping channel: {channel_name}")

            except Exception as e:
                print(f"An error occurred while scraping channel '{channel_name}': {e}")

        # Remove duplicates from all_video_details based on 'external_content_id'
        unique_video_ids = set()
        unique_video_details = []
        for video in all_video_details:
            if video['external_content_id'] not in unique_video_ids:
                unique_video_ids.add(video['external_content_id'])
                unique_video_details.append(video)

        # Save all content details to CSV, appending if file exists
        if unique_video_details:
            df_content = pd.DataFrame(unique_video_details)
            df_content = df_content[['external_content_id', 'platform_id', 'title', 'description', 'url', 'author', 
                                     'published_at', 'view_count', 'like_count', 'comment_count', 'content_type', 'metadata']]
            append_to_csv(df_content, 'content.csv', [
                'external_content_id', 'platform_id', 'title', 'description', 'url', 'author', 
                'published_at', 'view_count', 'like_count', 'comment_count', 'content_type', 'metadata'
            ])
            print(f"\nScraped {len(unique_video_details)} unique videos and saved to content.csv")
        else:
            print("\nNo video details to save to content.csv")

        # Save comments to CSV, appending if file exists
        if all_comments:
            df_comments = pd.DataFrame(all_comments)
            # Ensure the DataFrame columns match the schema: comment_id, post_external_id, author, body, created_at, like_count, reply_count, parent_id, metadata
            df_comments = df_comments[['comment_id', 'post_external_id', 'author', 'body', 'created_at', 'like_count', 'reply_count', 'parent_id', 'metadata']]
            append_to_csv(df_comments, 'comments.csv', ['comment_id', 'post_external_id', 'author', 'body', 'created_at', 'like_count', 'reply_count', 'parent_id', 'metadata'])
            print(f"Scraped {len(all_comments)} comments and saved to comments.csv")
        else:
            print("No comments to save to comments.csv")

        # Save NGO mentions to CSV, appending if file exists
        if ngo_content_data:
            df_ngo_content = pd.DataFrame(ngo_content_data)
            df_ngo_content.drop_duplicates(inplace=True)  # Remove duplicate mappings
            df_ngo_content = df_ngo_content[['ngo_id', 'external_content_id']]
            append_to_csv(df_ngo_content, 'ngo_content.csv', ['ngo_id', 'external_content_id'])
            print(f"NGO-related data has been saved to ngo_content.csv")
            print(f"Total unique NGOs found across all channels: {len(ngos_found_overall)}")
        else:
            print("No NGO-related content found. No ngo_content.csv file created.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
