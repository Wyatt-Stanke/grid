
-- Enable the FDW extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Remove any previous server definition to avoid conflicts
DROP SERVER IF EXISTS musicbrainz_server CASCADE;

-- Create a foreign server pointing to the "musicbrainz" database.
CREATE SERVER musicbrainz_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'musicbrainz');

-- Create a user mapping for the current user.
CREATE USER MAPPING FOR CURRENT_USER
    SERVER musicbrainz_server;
    
-- Import the selected tables from the "musicbrainz" schema into a local foreign schema.
-- The imported tables will be created in the "mb_foreign" schema.
CREATE SCHEMA IF NOT EXISTS mb_foreign;

IMPORT FOREIGN SCHEMA musicbrainz
        LIMIT TO (recording, artist_credit_name, artist, l_artist_url, url)
        FROM SERVER musicbrainz_server
        INTO mb_foreign;
        
-- Copy the imported foreign tables into local tables in the "data" database.
-- This creates local copies of the data.
DROP TABLE IF EXISTS recording;
CREATE TABLE recording AS
    SELECT * FROM mb_foreign.recording;

DROP TABLE IF EXISTS artist_credit_name;
CREATE TABLE artist_credit_name AS
    SELECT * FROM mb_foreign.artist_credit_name;

DROP TABLE IF EXISTS artist;
CREATE TABLE artist AS
    SELECT * FROM mb_foreign.artist;

DROP TABLE IF EXISTS l_artist_url;
CREATE TABLE l_artist_url AS
    SELECT * FROM mb_foreign.l_artist_url;

DROP TABLE IF EXISTS url;
CREATE TABLE url AS
    SELECT * FROM mb_foreign.url;