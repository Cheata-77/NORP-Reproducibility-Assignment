# reddit/reddit_scraper.py

import re
import praw
import pandas as pd
from dotenv import load_dotenv
import os
import argparse
import sys
from thefuzz import fuzz, process
import json

# Set a constant for Reddit platform_id.
PLATFORM_ID = 1

# Define a global constant for the maximum number of posts per subreddit
MAX_POSTS_PER_SUBREDDIT = 300

# Define a global constant for the maximum number of comments per post per depth
MAX_COMMENTS_PER_DEPTH = 3

# Define a global constant for the maximum depth of comments replies to scrape per post
MAX_DEPTH_PER_POST = 2

# Define a global list of relevant subreddits to iterate through if no CLI args are provided
DEFAULT_SUBREDDITS = [
    "nonprofit",
    "charity",
    "charities",
    "CharitableDonations",
    "NGOs",
    "socialgood",
    "volunteer",
    "Philanthropy",
    "fundraiser",
    "community",
    "GlobalDevelopment",
    "humanrights",
    "Assistance",
    "Fundraisers",
    "GoFundMe",
    "MutualAid",
    "helpit",
    "humanitarian",
    "Donation",
    "donate",
    "volunteering",
    "nycvolunteers",
    "volunteertoronto"
]

from ngos_list import ngos_list

def load_credentials():
    """
    Load Reddit API credentials from the .env file.
    """
    load_dotenv()
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")
    
    if not all([client_id, client_secret, user_agent]):
        raise ValueError("Missing Reddit API credentials in the .env file.")
    
    return client_id, client_secret, user_agent

def initialize_reddit_client(client_id, client_secret, user_agent):
    """
    Initialize the Reddit API client using PRAW.
    """
    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)
    return reddit

def scrape_subreddit_posts(reddit, subreddit_name, ngos_list, fuzzy_threshold=90, target_date=None, date_range=3):
    """
    Scrape the top posts from a specified subreddit and identify associated NGOs using exact and fuzzy matching.
    Only posts matched to at least one NGO are saved.
    """
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []
    comments_data = []
    ngo_content_data = []
    ngos_found = set()
    
    # List of all NGO names for fuzzy matching
    ngo_names = [ngo['name'] for ngo in ngos_list]
    
    try:
        submissions = subreddit.top(limit=MAX_POSTS_PER_SUBREDDIT)
    except Exception as e:
        print(f"Error accessing subreddit '{subreddit_name}': {e}")
        return posts_data, ngo_content_data, ngos_found

    for idx, submission in enumerate(submissions, start=1):
        post_content = f"{submission.title} {submission.selftext}"
        matched_ngos = set()
        metadata = json.dumps({
            'url': submission.url,
            'author': submission.author.name if submission.author else None
        })

        # Filter posts based on target date and date range
        created_at_dt = pd.to_datetime(submission.created_utc, unit='s')
        if target_date:
            target_date_dt = pd.to_datetime(target_date, format='%Y-%m-%d')
            date_diff = (created_at_dt - target_date_dt).days
            if abs(date_diff) > date_range:
                continue
        
        # First, attempt exact matching using keywords.
        for ngo in ngos_list:
            for keyword in ngo['keywords']:
                if keyword.isupper():
                    # For acronyms, match only if the keyword appears as a whole word.
                    pattern = rf"\b{re.escape(keyword)}\b"
                    if re.search(pattern, post_content):
                        matched_ngos.add(ngo['ngo_id'])
                        break  # Stop checking keywords for this NGO
                else:
                    # For non-acronym keywords, perform a simple case-insensitive substring check.
                    if keyword.lower() in post_content.lower():
                        matched_ngos.add(ngo['ngo_id'])
                        break  # Stop checking keywords for this NGO

        # If no exact match found, attempt fuzzy matching on non-acronym NGOs only.
        if not matched_ngos:
            non_acronym_ngos = [ngo for ngo in ngos_list if not ngo['name'].isupper()]
            non_acronym_names = [ngo['name'] for ngo in non_acronym_ngos]
            if non_acronym_names:
                best_match = process.extractOne(post_content, non_acronym_names, scorer=fuzz.partial_ratio)
                if best_match:
                    match_name, score = best_match
                    if score >= fuzzy_threshold:
                        # Find the corresponding NGO ID.
                        ngo_id = next((ngo['ngo_id'] for ngo in non_acronym_ngos if ngo['name'] == match_name), None)
                        if ngo_id:
                            matched_ngos.add(ngo_id)

        
        # Proceed only if at least one NGO is matched
        if matched_ngos:
            ngos_found.update(matched_ngos)
            created_at = pd.to_datetime(submission.created_utc, unit='s').strftime('%Y-%m-%d %H:%M:%S')

            # Attempt to retrieve 'ups'; fallback to 'score' if 'ups' is unavailable
            like_count = submission.ups if hasattr(submission, 'ups') else submission.score
            
            posts_data.append({
                'external_content_id': submission.id,
                'platform_id': PLATFORM_ID,
                'title': submission.title,
                'description': submission.selftext,
                'url': submission.url,
                'author': submission.author.name if submission.author else None,
                'published_at': created_at,
                'view_count': submission.score,
                'like_count': like_count,
                'comment_count': submission.num_comments,
                'content_type': 'Post',
                'metadata': metadata
            })
            
            for ngo_id in matched_ngos:
                ngo_content_data.append({
                    'ngo_id': ngo_id,
                    'external_content_id': submission.id  # To be mapped during import
                })

            # Scrape comments for the post
            scraped_comment_tree = scrape_post_comments(submission.comments)
            comments_data.extend(scraped_comment_tree)

        if idx % 100 == 0:
            print(f"Processed post {idx}/{MAX_POSTS_PER_SUBREDDIT} in subreddit '{subreddit_name}'")
    
    print(f"Total matched posts in subreddit '{subreddit_name}': {len(posts_data)}")
    print(f"Total unique NGOs found in subreddit '{subreddit_name}': {len(ngos_found)}")
    
    return posts_data, ngo_content_data, ngos_found, comments_data

def scrape_post_comments(comment_forest, limit=MAX_COMMENTS_PER_DEPTH, depth=MAX_DEPTH_PER_POST):
    """
    Recursively scrape comments from a Reddit post.
    Set the 'limit' parameter to control the number of comments to scrape per depth.
    Set the 'depth' parameter to control the number of nested replies to scrape.
    """
    if depth == 0:
        return []
    
    comment_forest.replace_more(limit=0)
    comments = comment_forest.list()[:limit]

    comments_data = []

    for comment in comments:

        comment_data = {
            'comment_id': comment.id,
            'post_external_id': comment.submission.id,
            'author': comment.author.name if comment.author else None,
            'body': comment.body,
            'created_at': pd.to_datetime(comment.created_utc, unit='s').strftime('%Y-%m-%d %H:%M:%S'),
            'like_count': comment.ups,
            'reply_count': len(comment.replies),
            'parent_id': comment.parent_id,
            'metadata': json.dumps({
                'permalink': comment.permalink
            })
        }

        comments_data.append(comment_data)

        child_comments = scrape_post_comments(comment.replies, limit, depth-1)
        comments_data.extend(child_comments)

    return comments_data

def save_content_to_csv(posts_data, filename="content.csv"):
    """
    Save the content data to a CSV file.
    """
    if not posts_data:
        print("No posts matched with any NGO. No CSV file will be created for content.")
        return
    
    df_content = pd.DataFrame(posts_data)
    df_content = df_content[['external_content_id', 'platform_id', 'title', 'description', 'url', 'author', 'published_at', 'view_count', 'like_count', 'comment_count', 'content_type', 'metadata']]
    df_content.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Content data has been saved to {filename}")

def save_ngo_content_to_csv(ngo_content_data, filename="ngo_content.csv"):
    """
    Save the NGO-Content mapping data to a CSV file.
    """
    if not ngo_content_data:
        print("No NGO-content mappings found. No CSV file will be created for ngo_content.")
        return
    
    df_ngo_content = pd.DataFrame(ngo_content_data)
    df_ngo_content.drop_duplicates(inplace=True)  # Remove duplicate mappings
    df_ngo_content = df_ngo_content[['ngo_id', 'external_content_id']]
    df_ngo_content.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"NGO-Content mapping data has been saved to {filename}")

def save_comments_to_csv(comments_data, filename="comments.csv"):
    """
    Save the comments data to a CSV file.
    """
    if not comments_data:
        print("No comments found. No json file will be created for comments.")
        return
    
    df_comments = pd.DataFrame(comments_data)
    df_comments.to_csv(filename, index=False, encoding='utf-8-sig')

    print(f"Comments data has been saved to {filename}")

def main():
    """
    Main function to orchestrate the scraping process.
    """
    parser = argparse.ArgumentParser(description="Reddit Scraper")
    parser.add_argument(
        "--subreddit",
        type=str,
        nargs='*',
        help="One or more subreddits to scrape posts from. If not provided, the scraper will use a predefined list of relevant subreddits."
    )

    parser.add_argument(
        "--target_date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format. Posts will be filtered around this date."
    )

    parser.add_argument(
        "--date_range",
        type=int,
        default=3,
        help="Number of days before and after target_date to include."
    )
    
    args = parser.parse_args()
    
    # Determine which subreddits to scrape
    if args.subreddit:
        subreddits_to_scrape = args.subreddit
    else:
        subreddits_to_scrape = DEFAULT_SUBREDDITS
        print("No subreddits provided via CLI. Using predefined list of subreddits.")

    # check if date format is correct
    if args.target_date:
        try:
            pd.to_datetime(args.target_date, format='%Y-%m-%d')
        except ValueError:
            print("Please provide a valid date in the format YYYY-MM-DD")
            sys.exit(1)
    
    try:
        client_id, client_secret, user_agent = load_credentials()
        reddit = initialize_reddit_client(client_id, client_secret, user_agent)
        
        all_posts_data = []
        all_ngo_content_data = []
        all_ngos_found = set()
        all_comments_data = []
        
        for subreddit in subreddits_to_scrape:
            print(f"Starting to scrape subreddit: {subreddit}")
            posts_data, ngo_content_data, ngos_found, comments_data = scrape_subreddit_posts(
                reddit, 
                subreddit, 
                ngos_list, 
                fuzzy_threshold=90, 
                target_date=args.target_date,
                date_range=args.date_range
            )
            all_posts_data.extend(posts_data)
            all_ngo_content_data.extend(ngo_content_data)
            all_ngos_found.update(ngos_found)
            all_comments_data.extend(comments_data)
        
        save_content_to_csv(all_posts_data)
        save_ngo_content_to_csv(all_ngo_content_data)
        save_comments_to_csv(all_comments_data)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
