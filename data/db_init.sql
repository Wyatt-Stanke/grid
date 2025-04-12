CREATE EXTENSION fuzzystrmatch;

BEGIN;

CREATE OR REPLACE FUNCTION musicbrainz.get_release_first_release_date_rows(condition TEXT)
  RETURNS SETOF musicbrainz.release_first_release_date
  LANGUAGE plpgsql
  STRICT
AS $$
BEGIN
    RETURN QUERY EXECUTE '
        SELECT DISTINCT ON (release) release,
               date_year  AS year,
               date_month AS month,
               date_day   AS day
          FROM (
               SELECT release, date_year, date_month, date_day
                 FROM musicbrainz.release_country
                WHERE (date_year IS NOT NULL
                       OR date_month IS NOT NULL
                       OR date_day IS NOT NULL)
               UNION ALL
               SELECT release, date_year, date_month, date_day
                 FROM musicbrainz.release_unknown_country
          ) all_dates
         WHERE ' || condition || '
         ORDER BY release,
                  year   NULLS LAST,
                  month  NULLS LAST,
                  day    NULLS LAST
    ';
END;
$$;

CREATE TABLE IF NOT EXISTS musicbrainz.release_first_release_date (
    release INTEGER PRIMARY KEY,
    year    INTEGER,
    month   INTEGER,
    day     INTEGER
);

SET SESSION_REPLICATION_ROLE = REPLICA;

TRUNCATE musicbrainz.release_first_release_date;

INSERT INTO musicbrainz.release_first_release_date
SELECT *
  FROM musicbrainz.get_release_first_release_date_rows('TRUE');

CLUSTER musicbrainz.release_first_release_date
        USING release_first_release_date_pkey;

SET SESSION_REPLICATION_ROLE = DEFAULT;

CREATE OR REPLACE FUNCTION musicbrainz.get_recording_first_release_date_rows(condition TEXT)
  RETURNS SETOF musicbrainz.recording_first_release_date
  LANGUAGE plpgsql
  STRICT
AS $$
BEGIN
    RETURN QUERY EXECUTE '
        SELECT DISTINCT ON (t.recording)
               t.recording,
               rd.year,
               rd.month,
               rd.day
          FROM musicbrainz.track t
          JOIN musicbrainz.medium m
            ON m.id = t.medium
          JOIN musicbrainz.release_first_release_date rd
            ON rd.release = m.release
         WHERE ' || condition || '
         ORDER BY t.recording,
                  rd.year  NULLS LAST,
                  rd.month NULLS LAST,
                  rd.day   NULLS LAST
    ';
END;
$$;

CREATE TABLE IF NOT EXISTS musicbrainz.recording_first_release_date (
    recording INTEGER PRIMARY KEY,
    year      INTEGER,
    month     INTEGER,
    day       INTEGER
);

TRUNCATE musicbrainz.recording_first_release_date;

INSERT INTO musicbrainz.recording_first_release_date
SELECT *
  FROM musicbrainz.get_recording_first_release_date_rows('TRUE');

CLUSTER musicbrainz.recording_first_release_date
        USING recording_first_release_date_pkey;

COMMIT;

-- Generate indicies
CREATE INDEX idx_recording_artist_credit ON MUSICBRAINZ.RECORDING (ARTIST_CREDIT);
CREATE INDEX idx_artist_credit_name ON MUSICBRAINZ.ARTIST_CREDIT_NAME (ARTIST_CREDIT, ARTIST);
CREATE INDEX idx_artist_begin_date_year ON MUSICBRAINZ.ARTIST (BEGIN_DATE_YEAR);
CREATE INDEX idx_release_date_year ON MUSICBRAINZ.recording_first_release_date (YEAR);
CREATE INDEX idx_release_date_recording ON MUSICBRAINZ.recording_first_release_date (RECORDING);
CREATE INDEX idx_release_date_year_recording ON MUSICBRAINZ.recording_first_release_date (YEAR, RECORDING);
CREATE INDEX idx_l_artist_url_entity0 ON MUSICBRAINZ.L_ARTIST_URL (ENTITY0);
CREATE INDEX idx_l_artist_url_entity1 ON MUSICBRAINZ.L_ARTIST_URL (ENTITY1);
CREATE INDEX idx_url_id ON MUSICBRAINZ.URL (ID);