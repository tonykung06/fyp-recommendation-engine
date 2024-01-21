import fs from "fs";
import ndjson from 'ndjson';
import _ from 'lodash';
import fetchData from './fetchData.js';
import { fileURLToPath } from 'url';
import readline from 'readline';
import events from 'events';
import { getRestaurantId } from './fetchData.js';

const __filename = fileURLToPath(import.meta.url);

console.time(__filename);

const restaurantUrls = [];
for await (const line of readline.createInterface({
    input: fs.createReadStream('restaurants.ndjson', { encoding: 'utf8' }),
    crlfDelay: Infinity
})) {
    if (!line.trim()) {
        continue;
    }
    const data = JSON.parse(line);
    if (!data.urlUI || !data.reviewUrlUI || !data.poiId) {
        continue;
    }

    if (restaurantUrls.findIndex(v => v[0] === data.poiId) > -1) {
        continue;
    }

    restaurantUrls.push([data.poiId, data.urlUI, data.reviewUrlUI]);
}

const rl = readline.createInterface({
    input: fs.createReadStream('restaurantReviews.ndjson', { encoding: 'utf8' }),
    crlfDelay: Infinity
});

const restaurantAlreadyDone = [];

rl.on('line', (line) => {
    if (!line.trim()) {
        return;
    }
    const data = JSON.parse(line);
    const restaurantId = getRestaurantId(data.restaurant);
    if (!restaurantId) {
        return;
    }
    if (restaurantAlreadyDone.includes(restaurantId)) {
        return;
    }
    restaurantAlreadyDone.push(restaurantId);
});

await events.once(rl, 'close');

const restaurantReviewsFileWriteStream = fs.createWriteStream('restaurantReviews.ndjson', {
    flags: 'a'
});
const restaurantReviewsNdjsonWriteStream = ndjson.stringify()
restaurantReviewsNdjsonWriteStream.on('data', function (line) {
    restaurantReviewsFileWriteStream.write(line);
})

console.log(`Processing ${restaurantUrls.length} restaurants`);
console.log(`Skipping ${restaurantAlreadyDone.length} restaurants`);

const chunkSize = 15;
const chunks = _.chunk(restaurantUrls, chunkSize);

let count = 0;
let addedCount = 0;
let failedCount = 0;
for (let chunk of chunks) {
    const fetches = [];

    try {
        for (let [restaurantId, restaurantWebpageUrl, restaurantReviewWebpageUrl] of chunk) {
            restaurantWebpageUrl = `https://www.openrice.com${restaurantWebpageUrl}`;
            restaurantReviewWebpageUrl = `https://www.openrice.com${restaurantReviewWebpageUrl}`;
            count++;
            if (count % 1000 === 0) {
                console.log(`\n${restaurantUrls.length - count}`);
            }
            if (restaurantAlreadyDone.includes(restaurantId)) {
                process.stdout.write('.');
                continue;
            }
            fetches.push(fetchData(restaurantWebpageUrl, restaurantReviewWebpageUrl));
        }
        if (fetches.length < 1) {
            continue;
        }
        const results = await Promise.all(fetches);
        for (let result of results) {
            restaurantReviewsNdjsonWriteStream.write(result);
            process.stdout.write('+');
            addedCount++;
        }
    } catch (e) {
        console.log(e);
        failedCount++;
        continue;
    }
}

console.log();

restaurantReviewsNdjsonWriteStream.end();
restaurantReviewsFileWriteStream.end("", "utf8", () => {
    console.log(`${addedCount} restaurants Added!`);
    console.log("restaurantReviews.ndjson writable stream is ended!");
});

if (failedCount > 0) {
    console.log(`${failedCount} restaurants failed! Please restart the script to clear them!`);
}

console.timeEnd(__filename);
