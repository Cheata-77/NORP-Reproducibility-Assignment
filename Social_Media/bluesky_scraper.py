import os
import sys
import re
import requests
import argparse
import pandas as pd
from datetime import datetime, timedelta

from ngos_list import ngos_list

# Set the BlueSky platform_id.
# IMPORTANT: Update this value to match your database. For example, if you set BlueSky to id 3, then leave it as is.
PLATFORM_ID = 3

def setup_parser():
    """
    Set up and return parser to retrieve command line arguments.
    """
    parser = argparse.ArgumentParser(description="BlueSky scraper for non-profit organizations")
    parser.add_argument(
        "--keyword",
        type=str,
        nargs=1,
        help=("Keyword to search posts for (exact match). If provided, the scraper will use that keyword "
              "and attempt to map it to an NGO if applicable. If not provided, the scraper iterates over "
              "all NGOs from ngos_list.")
    )
    parser.add_argument(
        "--max_results",
        type=int,
        default=10,
        help="Number of posts to retrieve for each keyword. Default is 10."
    )
    parser.add_argument(
        "--sort_method",
        choices=["top", "latest"],
        default="latest",
        help=("Retrieve posts by most recently posted ('latest') or most popular ('top'). "
              "Default is 'latest'.")
    )
    parser.add_argument(
        "--target_date",
        type=str,
        nargs=1,
        default=None,
        help="Target date in YYYY-MM-DD format. Posts will be filtered around this date."
    )
    parser.add_argument(
        "--date_range",
        type=int,
        default=0,
        help=("Number of days before and after target_date to include. Defaults to 0, meaning only the given day is scraped.")
    )
    return parser

def extract_url(uri):
    """
    Extract URL of post from its URI.
    Expected format: "at://did:plc:{DID}/app.bsky.feed.post/{postID}"
    Returns: "https://bsky.app/profile/{DID}/post/{postID}"
    """
    if not uri.startswith("at://did:plc:"):
        print("Invalid URI – cannot parse URL for:", uri)
        return ""
    uri_parts = uri.split("/")
    if len(uri_parts) < 5:
        print("Invalid URI structure for:", uri)
        return ""
    DID = uri_parts[2]
    post_id = uri_parts[-1]
    return f"https://bsky.app/profile/{DID}/post/{post_id}"

def get_api_url(keyword, max_results, sort_method, target_date, date_range):
    """
    Construct and return the BlueSky API URL for a given keyword and date range.
    """
    if not keyword:
        print("Keyword not provided")
        return None
    if target_date:
        try:
            target_date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        except Exception as e:
            print("Invalid target_date. Use YYYY-MM-DD format.")
            return None
        if date_range < 0:
            print("Date range cannot be negative.")
            return None
        start_date = (target_date_obj - timedelta(days=date_range)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = (target_date_obj + timedelta(days=date_range)).strftime("%Y-%m-%dT%H:%M:%SZ")
        api_url = (
            f"https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=\"{keyword}\""
            f"&limit={max_results}&since={start_date}&until={end_date}"
        )
        print(f"Got API URL for keyword '{keyword}' for dates {start_date} to {end_date}. ", end="")
    else:
        api_url = (
            f"https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=\"{keyword}\""
            f"&limit={max_results}&sort={sort_method}"
        )
    return api_url

def parse_bsky_date(raw_date):
    """
    Parse BlueSky's raw date string and return it formatted as "YYYY-MM-DD HH:MM:SS".
    Handles fractional seconds (truncates to 6 digits) and timezone offsets.
    Returns an empty string on failure.
    """
    if not raw_date:
        return ""
    
    # Use regex to truncate any extra fractional digits to 6 digits
    # Example: "2025-04-14T13:50:14.61812900+00:00" -> "2025-04-14T13:50:14.618129+00:00"
    pattern = r"(.*\.\d{6})\d*(.*)"
    raw_date = re.sub(pattern, r"\1\2", raw_date)

    try:
        # Convert 'Z' notation to a timezone offset that fromisoformat() can handle
        if raw_date.endswith("Z"):
            raw_date = raw_date.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw_date)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error parsing date '{raw_date}':", e)
        return ""

def scrape_posts(keyword, max_results, sort_method, target_date=None, date_range=None):
    """
    Scrape posts from BlueSky using the given keyword and optional date parameters.
    Returns a list of records following the unified schema.
    """
    api_url = get_api_url(keyword, max_results, sort_method, target_date, date_range)
    if not api_url:
        return None
    print("Scraping posts...", end="")
    try:
        response = requests.get(api_url)
        data = response.json()
    except Exception as e:
        print("Error during API request:", e)
        return None

    posts_data = []
    if 'posts' not in data:
        print(f"API response did not include 'posts' key for URL: {api_url}")
        return None

    for post in data['posts']:
        try:
            if not post.get('uri'):
                print("Post missing URI; skipping.")
                continue
            text_content = post['record'].get('text')
            if not text_content:
                print("Post missing text content; skipping.")
                continue

            raw_date = post['record'].get('createdAt')
            formatted_date = ""
            if raw_date and raw_date != "0000-00-00 00:00:00":
                formatted_date = parse_bsky_date(raw_date)

            record = {
                'external_content_id': post['uri'],
                'platform_id': PLATFORM_ID,
                'title': "",                          # BlueSky posts don't have a title
                'description': text_content,
                'url': extract_url(post['uri']),
                'author': post['author']['handle'],
                'published_at': formatted_date,
                'view_count': 0,                      # Default value
                'like_count': post.get('likeCount', 0),
                'comment_count': post.get('replyCount', 0),
                'content_type': 'Post',
                'metadata': "{}"                      # Empty JSON object as string
            }
            posts_data.append(record)
        except Exception as e:
            print("Error processing a post:", e)
            continue
    print(f"Found {len(posts_data)} posts.")
    return posts_data

def save_content_to_csv(posts_data, filename="content.csv"):
    """
    Save the scraped posts data to 'content.csv' following the unified schema.
    Filters out posts with invalid or missing published_at dates.
    """
    if not posts_data:
        print("No posts found; content CSV not created.")
        return

    # Filter out records that do not have a valid published_at date.
    valid_posts = []
    for post in posts_data:
        if post['published_at'] and post['published_at'] != '0000-00-00 00:00:00':
            valid_posts.append(post)
            
    invalid_count = len(posts_data) - len(valid_posts)
    if invalid_count:
        print(f"Warning: {invalid_count} posts with invalid or missing dates were filtered out.")

    if not valid_posts:
        print("No valid posts found; content CSV not created.")
        return

    df = pd.DataFrame(valid_posts)
    cols = [
        'external_content_id', 'platform_id', 'title', 'description', 'url', 'author',
        'published_at', 'view_count', 'like_count', 'comment_count', 'content_type', 'metadata'
    ]
    df = df[cols]
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Content data saved to {filename}")

def save_ngo_content_to_csv(ngo_content_data, filename="ngo_content.csv"):
    """
    Save NGO-to-post mapping data to 'ngo_content.csv'.
    Expected columns: ngo_id, external_content_id.
    """
    if not ngo_content_data:
        print("No NGO-content mappings found; ngo_content CSV not created.")
        return
    df = pd.DataFrame(ngo_content_data)
    df = df[['ngo_id', 'external_content_id']].drop_duplicates()
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"NGO-content mapping data saved to {filename}")

def save_comments_to_csv(comments_data, filename="comments.csv"):
    """
    Save comments data to 'comments.csv'.
    Since BlueSky does not currently provide comments data in this example, this may remain empty.
    """
    if not comments_data:
        print("No comments data collected; creating an empty comments CSV.")
        df = pd.DataFrame(columns=[
            'comment_id', 'post_external_id', 'author', 'body',
            'created_at', 'like_count', 'reply_count', 'parent_id', 'metadata'
        ])
    else:
        df = pd.DataFrame(comments_data)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Comments data saved to {filename}")

def main():
    parser = setup_parser()
    args = parser.parse_args()

    all_posts = []
    all_ngo_content = []
    all_comments = []  # BlueSky comments are not scraped in this example; will remain empty.

    # If a keyword is provided via command line, use that and try to identify the related NGO.
    if args.keyword:
        kw = args.keyword[0]
        posts = scrape_posts(kw, args.max_results, args.sort_method, 
                             args.target_date[0] if args.target_date else None,
                             args.date_range)
        if posts:
            all_posts.extend(posts)
            # For mapping, check each NGO to see if this keyword is among its keywords.
            for ngo in ngos_list:
                if kw in ngo.get('keywords', []):
                    for post in posts:
                        all_ngo_content.append({
                            'ngo_id': ngo['ngo_id'],
                            'external_content_id': post['external_content_id']
                        })
    else:
        # If no specific keyword provided, iterate over all NGOs in the list.
        print("No keyword provided; iterating over all NGOs and their keywords.")
        for ngo in ngos_list:
            for kw in ngo.get('keywords', []):
                posts = scrape_posts(kw, args.max_results, args.sort_method, 
                                     args.target_date[0] if args.target_date else None,
                                     args.date_range)
                if posts:
                    all_posts.extend(posts)
                    for post in posts:
                        all_ngo_content.append({
                            'ngo_id': ngo['ngo_id'],
                            'external_content_id': post['external_content_id']
                        })
    
    # Save the collected data into CSV files.
    save_content_to_csv(all_posts, "content.csv")
    save_ngo_content_to_csv(all_ngo_content, "ngo_content.csv")
    save_comments_to_csv(all_comments, "comments.csv")

if __name__ == "__main__":
    main()
