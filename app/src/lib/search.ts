// Web Worker

const fetchArtists = async () => {
	const res = await fetch("/artists.txt");
	const text = await res.text();
	const artists: [number, string, number][] = text
		.split("\n")
		.filter((line) => line)
		.map((line) => {
			const split = line.split(" ");
			const name = split.slice(1, -1).join(" ");
			return [
				Number.parseInt(split[0]),
				name,
				Number.parseInt(split[split.length - 1]),
			];
		});

	const groupedArtists: { [key: number]: [number, string][] } = {};
	for (const artist of artists) {
		const groupKey = artist[2];
		if (!groupedArtists[groupKey]) {
			groupedArtists[groupKey] = [];
		}
		groupedArtists[groupKey].push([artist[0], artist[1]]);
	}

	return groupedArtists;
};

let artists: { [popularity: number]: [number, string][] } = {};

self.onmessage = async (e) => {
	if (e.data.type === "start") {
		artists = await fetchArtists();
		self.postMessage({ type: "done" });
	}

	if (e.data.type === "search") {
		for (let i = 100; i >= 0; i--) {
			// self.postMessage({
			// 	type: "message",
			// 	message: `Searching popularity ${i}`,
			// });
			if (artists[i]) {
				const matches = artists[i].filter((artist) =>
					artist[1].toLowerCase().includes(e.data.query),
				);
				if (matches.length) {
					self.postMessage({ type: "result", artists: matches });
				}
			}
		}
	}
};
