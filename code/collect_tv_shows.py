# collect_tv_shows.py

import sys
import os
import requests
import time
import csv
import logging

# Add the project root directory to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import config  # Now this should work

# Ensure the logs directory exists
logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=os.path.join(logs_dir, 'data_collection.log'),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


# TMDb API key from config.py
TMDB_API_KEY = config.TMDB_API_KEY

# OMDb API key from config.py
OMDB_API_KEY = config.OMDB_API_KEY

# Provider ID for Paramount Plus
PROVIDER_ID = '531'

# Set the region and language as needed
WATCH_REGION = 'US'
LANGUAGE = 'en-US'

def get_tv_show_details(tv_id):
    url = f'https://api.themoviedb.org/3/tv/{tv_id}'
    params = {
        'api_key': TMDB_API_KEY,
        'language': LANGUAGE,
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching details for TV show ID {tv_id}: {response.status_code}")
        return None

def get_tv_show_external_ids(tv_id):
    url = f'https://api.themoviedb.org/3/tv/{tv_id}/external_ids'
    params = {
        'api_key': TMDB_API_KEY,
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching external IDs for TV show ID {tv_id}: {response.status_code}")
        return None

def get_data_from_omdb(imdb_id):
    url = 'http://www.omdbapi.com/'
    params = {
        'apikey': OMDB_API_KEY,
        'i': imdb_id,
        'plot': 'full',
        'r': 'json'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get('Response') == 'True':
            return data
        else:
            print(f"OMDb Error: {data.get('Error')} for IMDb ID {imdb_id}")
            return None
    else:
        print(f"Error fetching data from OMDb for IMDb ID {imdb_id}: {response.status_code}")
        return None

def fetch_tv_shows():
    url = 'https://api.themoviedb.org/3/discover/tv'
    params = {
        'api_key': TMDB_API_KEY,
        'language': LANGUAGE,
        'sort_by': 'popularity.desc',
        'with_watch_providers': PROVIDER_ID,
        'watch_region': WATCH_REGION,
        'page': 1
    }
    all_results = []
    total_pages = 1  # Initialize total_pages

    tmdb_request_count = 0
    omdb_request_count = 0
    start_time = time.time()

    while params['page'] <= total_pages:
        # First API call in this loop
        response = requests.get(url, params=params)
        tmdb_request_count += 1

        # Rate limiting: TMDb allows 40 requests every 10 seconds
        if tmdb_request_count >= 40:
            elapsed_time = time.time() - start_time
            if elapsed_time < 10:
                wait_time = 10 - elapsed_time
                print(f"TMDb rate limit reached. Waiting for {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            # Reset the counters
            tmdb_request_count = 0
            start_time = time.time()

        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break

        data = response.json()
        if 'results' in data:
            total_pages = data.get('total_pages', 1)
            print(f"Fetched page {params['page']} of {total_pages}")
            for tv_show in data['results']:
                tv_id = tv_show['id']
                # Fetch detailed info
                details = get_tv_show_details(tv_id)
                tmdb_request_count += 1
                if tmdb_request_count >= 40:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 10:
                        wait_time = 10 - elapsed_time
                        print(f"TMDb rate limit reached. Waiting for {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                    # Reset the counters
                    tmdb_request_count = 0
                    start_time = time.time()

                if details:
                    # Fetch external IDs
                    external_ids = get_tv_show_external_ids(tv_id)
                    tmdb_request_count += 1
                    if tmdb_request_count >= 40:
                        elapsed_time = time.time() - start_time
                        if elapsed_time < 10:
                            wait_time = 10 - elapsed_time
                            print(f"TMDb rate limit reached. Waiting for {wait_time:.2f} seconds...")
                            time.sleep(wait_time)
                        # Reset the counters
                        tmdb_request_count = 0
                        start_time = time.time()

                    if external_ids:
                        details['external_ids'] = external_ids
                        imdb_id = external_ids.get('imdb_id')
                        if imdb_id:
                            # Fetch OMDb data
                            omdb_data = get_data_from_omdb(imdb_id)
                            omdb_request_count += 1
                            if omdb_request_count >= 990:
                                # OMDb free tier limit is 1,000 requests per day
                                print("OMDb request limit approaching. Stopping OMDb data fetching.")
                                # You can choose to wait until the next day or upgrade your plan
                                break
                            if omdb_data:
                                details['omdb_data'] = omdb_data
                    all_results.append(details)
            params['page'] += 1
        else:
            print("No results found.")
            break

    return all_results

def save_to_csv(data, filename):
    if data:
        fields = [
            # TMDb fields
            'id', 'name', 'original_name', 'overview', 'first_air_date',
            'last_air_date', 'number_of_episodes', 'number_of_seasons',
            'genres', 'origin_country', 'original_language', 'popularity',
            'vote_average', 'vote_count', 'status', 'type', 'homepage',
            'in_production', 'languages', 'episode_run_time', 'tagline',
            'created_by', 'networks',
            # OMDb fields
            'imdb_rating', 'imdb_votes', 'rotten_tomatoes_rating',
            'metacritic_rating', 'plot', 'awards', 'actors', 'writer', 'language',
            'country', 'box_office', 'production'
        ]
        with open(filename, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=fields)
            dict_writer.writeheader()
            for item in data:
                row = {}
                # Extract TMDb fields
                for field in fields:
                    if field in [
                        'id', 'name', 'original_name', 'overview', 'first_air_date',
                        'last_air_date', 'number_of_episodes', 'number_of_seasons',
                        'original_language', 'popularity', 'vote_average', 'vote_count',
                        'status', 'type', 'homepage', 'in_production', 'tagline'
                    ]:
                        value = item.get(field, '')
                        if isinstance(value, bool):
                            value = str(value)
                        elif value is None:
                            value = ''
                        row[field] = value
                    elif field == 'genres':
                        value = ', '.join([genre['name'] for genre in item.get('genres', [])])
                        row[field] = value
                    elif field == 'origin_country':
                        value = ', '.join(item.get('origin_country', []))
                        row[field] = value
                    elif field == 'languages':
                        value = ', '.join(item.get('languages', []))
                        row[field] = value
                    elif field == 'episode_run_time':
                        value = ', '.join(map(str, item.get('episode_run_time', [])))
                        row[field] = value
                    elif field == 'created_by':
                        value = ', '.join([creator['name'] for creator in item.get('created_by', [])])
                        row[field] = value
                    elif field == 'networks':
                        value = ', '.join([network['name'] for network in item.get('networks', [])])
                        row[field] = value
                    # Extract OMDb fields
                    elif field in ['imdb_rating', 'imdb_votes', 'rotten_tomatoes_rating',
                                   'metacritic_rating', 'plot', 'awards', 'actors',
                                   'writer', 'language', 'country', 'box_office', 'production']:
                        omdb_data = item.get('omdb_data', {})
                        if field == 'imdb_rating':
                            row[field] = omdb_data.get('imdbRating', '')
                        elif field == 'imdb_votes':
                            row[field] = omdb_data.get('imdbVotes', '')
                        elif field == 'rotten_tomatoes_rating':
                            ratings = omdb_data.get('Ratings', [])
                            rt_rating = next((r['Value'] for r in ratings if r['Source'] == 'Rotten Tomatoes'), '')
                            row[field] = rt_rating
                        elif field == 'metacritic_rating':
                            row[field] = omdb_data.get('Metascore', '')
                        elif field == 'plot':
                            row[field] = omdb_data.get('Plot', '')
                        elif field == 'awards':
                            row[field] = omdb_data.get('Awards', '')
                        elif field == 'actors':
                            row[field] = omdb_data.get('Actors', '')
                        elif field == 'writer':
                            row[field] = omdb_data.get('Writer', '')
                        elif field == 'language':
                            row[field] = omdb_data.get('Language', '')
                        elif field == 'country':
                            row[field] = omdb_data.get('Country', '')
                        elif field == 'box_office':
                            row[field] = omdb_data.get('BoxOffice', '')
                        elif field == 'production':
                            row[field] = omdb_data.get('Production', '')
                dict_writer.writerow(row)
        print(f"Data saved to {filename}")
    else:
        print(f"No data to save for {filename}")

def main():
    print("Starting to fetch TV shows available on Paramount Plus...")
    tv_shows = fetch_tv_shows()
    save_to_csv(tv_shows, 'paramount_plus_tv_shows.csv')

if __name__ == '__main__':
    main()
