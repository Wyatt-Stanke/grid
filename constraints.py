class HasCollaboratedWithArtistConstraint:
    def __init__(self, artist_name, artist_id):
        self.artist_name = artist_name
        self.artist_id = artist_id

    def generate_query(self):
        return f"""
            SELECT DISTINCT
                AC2.NAME, AC2.ARTIST
            FROM
                MUSICBRAINZ.RECORDING R
                JOIN MUSICBRAINZ.ARTIST_CREDIT_NAME AC1 ON AC1.ARTIST_CREDIT = R.ARTIST_CREDIT
                JOIN MUSICBRAINZ.ARTIST_CREDIT_NAME AC2 ON AC2.ARTIST_CREDIT = R.ARTIST_CREDIT
                JOIN MUSICBRAINZ.ARTIST A2 ON A2.ID = AC2.ARTIST
            WHERE
                AC1.ARTIST = {self.artist_id}
                AND AC2.ARTIST <> {self.artist_id}
        """

    def display_name(self, debug=False):
        if debug:
            return f"Has collaborated with {self.artist_name} ({self.artist_id})"
        return f"Has collaborated with {self.artist_name}"

    def render_name(self):
        return [self.artist_name, "COLLABORATED WITH"]


class StartedInDecadeConstraint:
    def __init__(self, decade):
        self.decade = decade

    def generate_query(self):
        start_year = self.decade
        # 10 years to make it easier for player
        end_year = self.decade + 10

        return f"""
            SELECT DISTINCT
                A.NAME,
                A.ID
            FROM
                MUSICBRAINZ.ARTIST A
            WHERE
                A.BEGIN_DATE_YEAR BETWEEN {start_year} AND {end_year}
        """

    def display_name(self, debug=False):
        if debug:
            return f"Person born in/group formed in {self.decade}s ({self.decade}-{self.decade + 10})"
        return f"Born in {self.decade}s"

    def render_name(self):
        return [f"{self.decade}s", "BIRTHDAY"]


class ReleasedMusicInDecade:
    def __init__(self, decade):
        self.decade = decade

    def generate_query(self):
        start_year = self.decade
        # 10 years to make it easier for player
        end_year = self.decade + 10

        return f"""
            SELECT DISTINCT
                ACN.NAME, ACN.ARTIST
            FROM
                MUSICBRAINZ.RECORDING
                JOIN MUSICBRAINZ.ARTIST_CREDIT_NAME ACN ON ACN.ARTIST_CREDIT = RECORDING.ARTIST_CREDIT
                JOIN MUSICBRAINZ.recording_first_release_date RD ON RD.RECORDING = RECORDING.ID
            WHERE
                RD.YEAR BETWEEN {start_year} AND {end_year}
        """

    def display_name(self, debug=False):
        if debug:
            return f"Person/group released music in {self.decade}s ({self.decade}-{self.decade + 10})"
        return f"Released music in {self.decade}s"

    def render_name(self):
        return [f"{self.decade}s", "RELEASED MUSIC"]


class ReleasedMusicInYear:
    def __init__(self, year):
        self.year = year

    def generate_query(self):
        return f"""
            SELECT DISTINCT
                ACN.NAME, ACN.ARTIST
            FROM
                MUSICBRAINZ.RECORDING
                JOIN MUSICBRAINZ.ARTIST_CREDIT_NAME ACN ON ACN.ARTIST_CREDIT = RECORDING.ARTIST_CREDIT
                JOIN MUSICBRAINZ.recording_first_release_date RD ON RD.RECORDING = RECORDING.ID
            WHERE
                RD.YEAR = {self.year}
        """

    def display_name(self, debug=False):
        if debug:
            return f"Person/group released music in {self.year} ({self.year})"
        return f"Released music in {self.year}"

    def render_name(self):
        return [f"{self.year}", "RELEASED MUSIC"]
