import random
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from constraints import (
    HasCollaboratedWithArtistConstraint,
    StartedInDecadeConstraint,
    ReleasedMusicInDecade,
    ReleasedMusicInYear,
)
from tqdm import tqdm


def init_spotify():
    auth_manager = SpotifyClientCredentials(
        client_id="1f622bbdc7374223884912cf4b5d2ab1",
        client_secret="2e34e4990170420d9afd022f7c06a984",
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)
    return sp


class NConstraints:
    def __init__(self, constraints):
        self.constraints = constraints

    def generate_query(self):
        return " INTERSECT ".join(
            [constraint.generate_query() for constraint in self.constraints]
        )

    def display_name(self, debug=False):
        return " AND ".join(
            [constraint.display_name(debug) for constraint in self.constraints]
        )


def get_constraint_spotify_stats(constraint):
    sql = constraint.generate_query()
    spotify_sql = f"""
    SELECT sub.*
    FROM (
        {sql}
    ) AS sub(name, artist_id)
    WHERE EXISTS (
        SELECT 1
        FROM MUSICBRAINZ.L_ARTIST_URL L_URL
        JOIN MUSICBRAINZ.URL URL ON URL.ID = L_URL.ENTITY1
        WHERE L_URL.ENTITY0 = sub.artist_id
          AND URL.URL ILIKE '%%spotify.com/artist%%'
    )
    """

    db.cursor.execute(sql)
    artists = db.cursor.fetchall()
    artists_len = len(artists)

    db.cursor.execute(spotify_sql)
    spotify_artists = db.cursor.fetchall()
    spotify_artists_len = len(spotify_artists)

    print(
        f"Found {artists_len} artists, {spotify_artists_len} with spotify urls ({spotify_artists_len / artists_len * 100:.2f}%) for {constraint.display_name()}"
    )


class MusicBrainzDB:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
            print("PostgreSQL connection is open")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def close(self):
        if self.connection:
            self.connection.close()
        if self.cursor:
            self.cursor.close()
        print("PostgreSQL connection is closed")

    def search_artists(self, query):
        query = """
            SELECT * 
            FROM MUSICBRAINZ.ARTIST
            WHERE levenshtein(LEFT(name, 255), %s) <= 3
            ORDER BY levenshtein(LEFT(name, 255), %s)
        """

        self.cursor.execute(query, (query, query))
        return self.cursor.fetchall()

    def get_spotify_id_from_artist_id(self, artist_id):
        query = """
            SELECT
                URL.URL
            FROM
                MUSICBRAINZ.L_ARTIST_URL L_URL
                JOIN MUSICBRAINZ.URL URL ON URL.ID = L_URL.ENTITY1
            WHERE
                L_URL.ENTITY0 = %s
                AND URL.URL ILIKE '%%spotify.com/artist%%'
        """

        self.cursor.execute(query, (artist_id,))
        url = self.cursor.fetchone()
        if url:
            return url[0].split("/")[-1].split("?")[0]
        return None

    def get_artists_by_name(self, name):
        # Example query
        # SELECT * FROM musicbrainz.artist
        # WHERE artist.name ILIKE '21 Savage';
        query = "SELECT NAME, ID FROM musicbrainz.artist WHERE artist.name ILIKE %s ORDER BY id ASC"
        self.cursor.execute(query, (name,))
        return self.cursor.fetchall()

    def get_artist_by_gid(self, gid):
        query = "SELECT NAME, ID FROM musicbrainz.artist WHERE artist.gid = %s"
        self.cursor.execute(query, (gid,))
        return self.cursor.fetchone()

    def get_all_recordings_by_artist_id(self, artist_id):
        query = """
            SELECT DISTINCT
                *
            FROM
                MUSICBRAINZ.RECORDING
                JOIN MUSICBRAINZ.ARTIST_CREDIT_NAME ACN ON ACN.ARTIST_CREDIT = RECORDING.ARTIST_CREDIT
            WHERE
                ACN.ARTIST = %s
        """

        self.cursor.execute(query, (artist_id,))
        return self.cursor.fetchall()

    def get_artist_image_by_id(self, artist_id):
        query = """
            SELECT
                COALESCE(S.IMAGE, 'https://placehold.co/512x512?text=' || REPLACE(ARTIST.NAME, ' ', '+'))
            FROM
                MUSICBRAINZ.ARTIST ARTIST
                LEFT JOIN DS_SPOTIFY.SPOTIFY S ON S.MUSICBRAINZ_ID = ARTIST.ID
            WHERE
                ARTIST.ID = %s
        """

        self.cursor.execute(query, (artist_id,))
        return self.cursor.fetchone()

    def execute_constraint(self, constraint):
        self.cursor.execute(constraint.generate_query())
        return self.cursor.fetchall()


# Usage example

if __name__ == "__main__":
    db = MusicBrainzDB(
        user="postgres",
        password="password",
        host="localhost",
        port="55432",
        database="musicbrainz",
    )

    # http://127.0.0.1:9090
    db.connect()
    try:
        row_constraints = [
            HasCollaboratedWithArtistConstraint(
                *db.get_artists_by_name("Lil Wayne")[0]
            ),
            HasCollaboratedWithArtistConstraint(*db.get_artists_by_name("Drake")[0]),
            HasCollaboratedWithArtistConstraint(
                *db.get_artists_by_name("Kendrick Lamar")[0]
            ),
        ]

        # row_constraints = [
        #     HasCollaboratedWithArtistConstraint(
        #         *db.get_artist_by_gid("596ffa74-3d08-44ef-b113-765d43d12738")
        #     ),
        #     HasCollaboratedWithArtistConstraint(
        #         *db.get_artist_by_gid("afb680f2-b6eb-4cd7-a70b-a63b25c763d5")
        #     ),
        #     HasCollaboratedWithArtistConstraint(
        #         *db.get_artist_by_gid("73e5e69d-3554-40d8-8516-00cb38737a1c")
        #     ),
        # ]

        print([constraint.display_name(debug=True) for constraint in row_constraints])

        # column_constraints = [
        #     StartedInDecadeConstraint(1970),
        #     StartedInDecadeConstraint(1980),
        #     StartedInDecadeConstraint(1990),
        # ]

        column_constraints = [
            ReleasedMusicInDecade(1990),
            ReleasedMusicInDecade(2000),
            ReleasedMusicInYear(2025),
        ]

        print(
            [constraint.display_name(debug=True) for constraint in column_constraints]
        )

        # Create grid
        constraints = []
        for row_constraint in row_constraints:
            to_append = []
            for column_constraint in column_constraints:
                to_append.append(NConstraints([row_constraint, column_constraint]))
            constraints.append(to_append)

        # Validate grid
        returns = []
        for x, row in enumerate(constraints):
            to_append = []
            for y, constraint in enumerate(row):
                db.cursor.execute(constraint.generate_query())
                ret = db.cursor.fetchall()
                to_append.append(ret)
                if not ret:
                    print(
                        f"No artists found for {row_constraints[x].display_name()} and {column_constraints[y].display_name()}"
                    )
                else:
                    print(
                        f"Found {len(ret)} artists for {row_constraints[x].display_name()} and {column_constraints[y].display_name()}"
                    )
                # get_constraint_spotify_stats(constraint)
            returns.append(to_append)

        # Generate puzzle JSON
        puzzle_inner = {
            "artists": {},
            "rows_names": [],
            "columns_names": [],
            "rows": [],
        }

        all_artists_set = set()
        for row in returns:
            for cell in row:
                for artist in cell:
                    all_artists_set.add(artist)
        all_artists = list(all_artists_set)

        print(f"Found {len(all_artists)} artists in total")
        for i, artist in tqdm(enumerate(all_artists)):
            puzzle_inner["artists"][i] = {
                "name": artist[0],
                # TODO: Add image
                "artist_image": db.get_artist_image_by_id(artist[1]),
                "musicbrainz_id": artist[1],
            }

        for constraint in row_constraints:
            puzzle_inner["rows_names"].append(constraint.render_name())

        for constraint in column_constraints:
            puzzle_inner["columns_names"].append(constraint.render_name())

        for row in returns:
            row_add = []
            for cell in row:
                cell_artists = []
                for artist in cell:
                    all_artists_id = all_artists.index(artist)
                    cell_artists.append(all_artists_id)
                row_add.append(cell_artists)
            puzzle_inner["rows"].append({"columns": row_add})

        # Write to out.json
        with open("out.json", "w") as f:
            json.dump({"format": 1, "data": puzzle_inner}, f, indent=4)

        dump_artists = []
        query = """
            SELECT DISTINCT
                L_URL.ENTITY0,
                ARTIST.NAME,
                COALESCE(S.POPULARITY, 0) AS POPULARITY,
                COALESCE(
                    S.IMAGE,
                    'https://placehold.co/512x512?text=' || REPLACE(ARTIST.NAME, ' ', '+')
                ) AS IMAGE
            FROM
                MUSICBRAINZ.L_ARTIST_URL L_URL
                JOIN MUSICBRAINZ.URL URL ON URL.ID = L_URL.ENTITY1
                JOIN MUSICBRAINZ.ARTIST ARTIST ON ARTIST.ID = L_URL.ENTITY0
                LEFT JOIN DS_SPOTIFY.SPOTIFY S ON S.MUSICBRAINZ_ID = L_URL.ENTITY0
            WHERE
                URL.URL ILIKE '%%spotify.com/artist%%'
        """

        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        print(f"Found {len(rows)} artists with spotify urls")

        # For now, limit to 1000 artists and the ones in the puzzle
        # reduced_rows = rows[:1000]
        # for artist in all_artists:
        #     reduced_rows.append((artist[1], artist[0], random.randint(0, 100)))

        for row in rows:
            dump_artists.append(" ".join([str(x) for x in row[:3]]))

        with open("artists.txt", "w") as f:
            f.write("\n".join(dump_artists))
    finally:
        db.close()
