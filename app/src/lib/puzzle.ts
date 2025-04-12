export interface Artist {
	name: string;
	artist_image: string;
	musicbrainz_id: number;
}

interface Artists {
	[key: string]: Artist;
}

interface Row {
	columns: number[][];
}

interface Data {
	artists: Artists;
	rows_names: string[][];
	columns_names: string[][];
	rows: Row[];
}

interface PuzzleFormat1 {
	format: 1;
	data: Data;
}

export type Puzzle = PuzzleFormat1;
