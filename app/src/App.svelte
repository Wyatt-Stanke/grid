<script lang="ts">
  import { onMount } from "svelte";
  import type { Puzzle, Artist } from "./lib/puzzle";
  import { writable } from "svelte/store";
  import SearchWorker from "./lib/search.ts?worker";

  const searchWorker = new SearchWorker();

  const darkIndices = new Set<number>([5, 6, 7, 9, 10, 11, 13, 14, 15]);
  const colIndices = new Set<number>([1, 2, 3]);
  const rowIndices = new Set<number>([4, 8, 12]);

  let puzzle: Puzzle | undefined;
  let loadingText: string | undefined;

  const cellResponses = writable<Record<number, Artist>>({});

  let showPopover = false;
  let searchText = "";
  let selectedCellIndex: number | null = null;

  const fetchPuzzle = async () => {
    loadingText = "Loading puzzle...";
    const res = await fetch("/out.json");
    puzzle = await res.json();
    loadingText = undefined;
  };

  onMount(async () => {
    await fetchPuzzle();
    loadingText = "Loading artists...";
    searchWorker.postMessage({ type: "start" });
  });

  searchWorker.onmessage = (event) => {
    if (event.data.type === "done") {
      loadingText = undefined;
    }
    if (event.data.type === "result") {
      console.log(event.data.artists);
      filteredArtists = filteredArtists.concat(event.data.artists);
    }
    if (event.data.type === "message") {
      console.log(event.data.message);
    }
  };

  const fillPuzzleSquare = (i: number) => {
    if (!darkIndices.has(i)) throw new Error("Invalid index");
    showPopover = true;
    searchText = "";
    selectedCellIndex = i;
  };

  const setCellResponse = (response: Artist) => {
    console.log(response);
    if (response === undefined) {
      alert("Invalid response");
    }
    if (selectedCellIndex !== null && puzzle !== undefined) {
      // Check if response is valid
      const musicbrainzId = response.musicbrainz_id;
      const selectedRow = Math.floor(selectedCellIndex / 4) - 1;
      const selectedCol = (selectedCellIndex % 4) - 1;
      const artistPuzzleId = Object.keys(puzzle.data.artists).find(
        (key) => puzzle?.data.artists[key].musicbrainz_id === musicbrainzId
      );

      const valid = puzzle.data.rows[selectedRow].columns[selectedCol].includes(
        Number.parseInt(artistPuzzleId as string)
      );

      if (valid) {
        cellResponses.update((responses) => ({
          ...responses,
          [selectedCellIndex as number]: response,
        }));
        showPopover = false;
        filteredArtists = [];
      } else {
        alert("Invalid response");
      }
    }
  };

  const filterArtists = (query: string) => {
    filteredArtists = [];
    searchWorker.postMessage({ type: "search", query });
  };

  let filteredArtists: [number, string][] = [];
  let debounceTimeout: ReturnType<typeof setTimeout>;

  $: {
    clearTimeout(debounceTimeout);
    debounceTimeout = setTimeout(() => {
      if (searchText.length > 0) {
        filterArtists(searchText);
      }
    }, 300);
  }
</script>

<main class="h-screen flex items-center justify-center relative font-sans">
  {#if loadingText}
    <div class="fixed top-4 left-4">
      <div class="loader"></div>
      <span class="ml-2 text-white">{loadingText}</span>
    </div>
  {/if}

  <div
    class="grid grid-cols-4 gap-2
           [width:min(75vh,100vw)]
           [height:min(75vh,100vw)]"
  >
    {#each Array(16) as _, i}
      <div
        class="aspect-square flex items-center justify-center rounded-lg {darkIndices.has(
          i
        )
          ? 'bg-gray-700 hover:bg-gray-500 transition-colors duration-200'
          : ''} text-white"
      >
        {#if i === 0}
          <p class="text-4xl">Grid</p>
        {:else if darkIndices.has(i) && puzzle}
          <button
            class="w-full h-full bg-transparent border-none cursor-pointer p-0"
            on:click={() => fillPuzzleSquare(i)}
          >
            {#if $cellResponses[i]}
              <div class="relative w-full h-full">
                <div
                  style="background-image: url({$cellResponses[i]
                    .artist_image}); background-size: cover;"
                  class="absolute inset-0 rounded-lg"
                ></div>
                <div
                  class="absolute bottom-0 left-0 p-1 bg-black bg-opacity-50"
                >
                  <p class="text-xs text-white m-0">
                    {$cellResponses[i].name}
                  </p>
                </div>
              </div>
            {/if}
          </button>
        {:else if rowIndices.has(i) && puzzle}
          <p class="text-2xl font-bold text-center line-height-7">
            {puzzle.data.rows_names[i / 4 - 1][0]}
            {#if puzzle.data.rows_names[i / 4 - 1][1]}
              <br />
              <span class="text-sm text-gray-500">
                {puzzle.data.rows_names[i / 4 - 1][1]}
              </span>
            {/if}
          </p>
        {:else if colIndices.has(i) && puzzle}
          <p class="text-2xl font-bold text-center line-height-7">
            {puzzle.data.columns_names[i - 1][0]}
            {#if puzzle.data.columns_names[i - 1][1]}
              <br />
              <span class="text-sm text-gray-500">
                {puzzle.data.columns_names[i - 1][1]}
              </span>
            {/if}
          </p>
        {/if}
      </div>
    {/each}
  </div>

  {#if showPopover}
    <!-- Popover overlay -->
    <div
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center"
    >
      <div class="bg-white p-4 rounded-lg w-96 h-96 overflow-auto">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-lg font-semibold">Search Artists</h2>
          <button
            class="text-xl"
            on:click={() => {
              showPopover = false;
              filteredArtists = [];
            }}>&times;</button
          >
        </div>
        <input
          type="text"
          class="w-full p-2 border rounded mb-4"
          placeholder="Search..."
          bind:value={searchText}
        />

        <ul>
          {#if searchText.length > 0}
            {#each filteredArtists as [id, name]}
              <li class="mb-2">
                <span class="font-bold">{name}</span>
                <button
                  class="ml-2 p-1 bg-blue-500 text-white rounded"
                  on:click={() =>
                    setCellResponse(
                      puzzle!.data.artists[
                        Object.keys(puzzle!.data.artists).find(
                          (key) =>
                            puzzle!.data.artists[key].musicbrainz_id === id
                        )!
                      ]
                    )}
                >
                  Select
                </button>
              </li>
            {/each}
          {:else}
            <p>Start typing to search for artists.</p>
          {/if}

          {#if filteredArtists.length === 0}
            <li>No artists found.</li>
          {/if}
        </ul>
      </div>
    </div>
  {/if}
</main>

<style>
  .loader {
    width: 20px;
    height: 20px;
    border: 5px solid #fff;
    border-bottom-color: transparent;
    border-radius: 50%;
    display: inline-block;
    box-sizing: border-box;
    animation: rotation 1s linear infinite;
  }

  @keyframes rotation {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }
</style>
