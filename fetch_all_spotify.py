import time
from main import MusicBrainzDB, init_spotify
from tqdm import tqdm

sp = init_spotify()
db = MusicBrainzDB(
    user="postgres",
    password="password",
    host="localhost",
    port="55432",
    database="musicbrainz",
)

db.connect()

# Create new table spotify with a generated primary key.
query = """
    CREATE SCHEMA IF NOT EXISTS ds_spotify;

    CREATE TABLE IF NOT EXISTS ds_spotify.spotify (
        id SERIAL PRIMARY KEY,
        musicbrainz_id INT REFERENCES musicbrainz.artist (id),
        spotify_id VARCHAR(255) NOT NULL UNIQUE,
        date_fetched DATE NOT NULL,
        followers INT,
        popularity INT,
        image TEXT,
        genres TEXT[]
    );
"""

print("Creating table spotify...")
db.cursor.execute(query)

# Fetch all artists and their spotify ids
query = """
    SELECT t.musicbrainz_id, t.url FROM (
        SELECT DISTINCT
            L_URL.ENTITY0 as musicbrainz_id,
            URL.URL as url,
            CASE WHEN S.musicbrainz_id IS NULL THEN 0 ELSE 1 END as sort_order
        FROM
            MUSICBRAINZ.L_ARTIST_URL L_URL
            JOIN MUSICBRAINZ.URL URL ON URL.ID = L_URL.ENTITY1
            LEFT JOIN ds_spotify.spotify S ON S.musicbrainz_id = L_URL.ENTITY0
        WHERE
            URL.URL ILIKE '%%spotify.com/artist%%'
    ) t
    ORDER BY t.sort_order
    LIMIT 4999*50;
"""

print("Fetching all artists with spotify urls...")
db.cursor.execute(query)
rows = db.cursor.fetchall()
print(f"Found {len(rows)} artists with spotify urls")

# Group rows by 50
grouped_rows = [rows[i : i + 50] for i in range(0, len(rows), 50)]

for group in tqdm(grouped_rows):
    artists = []
    artist_ids = []
    for row in group:
        url = row[1]
        spotify_id = url.split("/")[-1].split("?")[0]
        # Verify that the spotify id is valid
        if not spotify_id.isalnum() or len(spotify_id) != 22:
            print(f"Invalid spotify id: {spotify_id} for artist {row[0]}")
            continue
        artist_id = row[0]
        artist_ids.append(artist_id)
        artists.append(spotify_id)

    ret = []

    while True:
        try:
            ret = sp.artists(artists)["artists"]
            break
        except Exception as e:
            print(f"Error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

    for artist in ret:
        musicbrainz_id = artist_ids.pop(0)
        if not artist:
            print(f"Artist {musicbrainz_id} (spotify id: {spotify_id}) not found")
            continue
        spotify_id = artist["id"]
        followers = artist["followers"]["total"]
        popularity = artist["popularity"]
        images = artist["images"]
        if images:
            best_image = max(images, key=lambda img: img["width"] * img["height"])
            image_url = best_image["url"]
        else:
            image_url = None
        genres = artist["genres"]

        query = """
            INSERT INTO ds_spotify.spotify (musicbrainz_id, spotify_id, date_fetched, followers, popularity, image, genres)
            VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s)
            ON CONFLICT (spotify_id) DO UPDATE SET
            musicbrainz_id = EXCLUDED.musicbrainz_id,
            date_fetched = CURRENT_DATE,
            followers = EXCLUDED.followers,
            popularity = EXCLUDED.popularity,
            image = EXCLUDED.image,
            genres = EXCLUDED.genres
        """

        db.cursor.execute(
            query,
            (musicbrainz_id, spotify_id, followers, popularity, image_url, genres),
        )

print("Committing changes...")
db.connection.commit()
print("Done!")
db.close()
