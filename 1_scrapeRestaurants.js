import fs from "fs";
import _ from 'lodash';
import { fileURLToPath } from 'url';
import ndjson from 'ndjson';
import readline from 'readline';
import cuisines from "./openriceCuisines.js";
import districts from "./openriceDistricts.js";

const __filename = fileURLToPath(import.meta.url);

console.time(__filename);

const restaurantListApiUrls = [];

for (let district of Object.keys(districts)) {
    for (let cuisine of Object.keys(cuisines)) {
        restaurantListApiUrls.push(`https://www.openrice.com/api/v1/pois?uiLang=en&uiCity=hongkong&seoCategory=&callName=&sortBy=ORScoreDesc&districtId=${district}&cuisineId=${cuisine}&tabIndex=0&tabType=&page=$pageNum`);
    }
}

let totalUrlsLeft = restaurantListApiUrls.length;

console.log(`Processing ${totalUrlsLeft} restaurant lists`);

const restaurantsAlreadyDone = [];
try {
    const restaurantsReadStream = fs.createReadStream('restaurants.ndjson', { encoding: 'utf8' })
    const rl = readline.createInterface({
        input: restaurantsReadStream,
        crlfDelay: Infinity
    });
    for await (const line of rl) {
        if (!line.trim()) {
            continue;
        }
        const data = JSON.parse(line);
        restaurantsAlreadyDone.push(data.poiId);
    }
} catch (e) {
    console.error(e);
}

const restaurantsFileWriteStream = fs.createWriteStream('restaurants.ndjson', {
    flags: 'a'
});
const restaurantsNdjsonWriteStream = ndjson.stringify()
restaurantsNdjsonWriteStream.on('data', function (line) {
    restaurantsFileWriteStream.write(line);
})

async function getRestaurants(restaurantListApiUrl, pageNum = 1) {
    const response = await fetch(restaurantListApiUrl.replace("$pageNum", pageNum));
    const data = await response.json();
    const restaurants = data.searchResult.paginationResult.results;
    if (restaurants.length < 1) {
        return restaurants;
    }
    const nextPage = await getRestaurants(restaurantListApiUrl, pageNum + 1);
    return restaurants.concat(nextPage);
}

let failedCount = 0;
let count = 0;

const chunkSize = 15;
const chunks = _.chunk(restaurantListApiUrls, chunkSize);

for (let chunk of chunks) {
    const fetches = [];
    try {
        for (let url of chunk) {
            fetches.push(getRestaurants(url, 1));
        }
        const results = await Promise.all(fetches);
        for (let restaurants of results) {
            totalUrlsLeft--;
            if (totalUrlsLeft % 100 === 0) {
                console.log(`${totalUrlsLeft}`)
            }
            for (let restaurant of restaurants) {
                if (restaurantsAlreadyDone.includes(restaurant.poiId)) {
                    process.stdout.write('.');
                    continue
                }
                count++;
                process.stdout.write('+');

                restaurantsNdjsonWriteStream.write(restaurant);
            }
        }
    } catch (error) {
        failedCount++;
        console.error(error);
    }
}

restaurantsNdjsonWriteStream.end();

restaurantsFileWriteStream.end("", "utf8", () => {
    console.log(`${count} restaurants Added!`);
    console.log("restaurants.ndjson writable stream is ended!");
});

if (failedCount > 0) {
    console.log(`${failedCount} failed! Please restart the script to clear them!`);
}

console.log();
console.timeEnd(__filename);
