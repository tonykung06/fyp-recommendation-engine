import fs from "fs";
import { stringify } from 'csv-stringify';
import querystring from 'querystring';
import readline from 'readline';
import events from 'events';
import { fileURLToPath } from 'url';
import { getRestaurantId } from './fetchData.js';

const __filename = fileURLToPath(import.meta.url);

console.time(__filename);

function getUserId(review) {
    if (!review.userId) {
        return "";
    }

    const urlParts = review.userId.split('?');
    const query = urlParts[urlParts.length - 1];

    const userId = querystring.parse(query)['userid'];

    if (!userId) {
        return "";
    }

    return userId;
}

function getRating(review) {
    let totalCount = 0;
    let sum = 0;
    ['taste', 'decor', 'service', 'hygiene', 'value'].forEach((key) => {
        const rating = Number(review[key]);
        if (rating < 0) {
            return;
        }
        totalCount++;
        sum += rating;
    });
    if (totalCount < 1) {
        return -1;
    }
    // exclude extreme zero sum
    // probably bad data scrape or extreme rating
    // if (sum < 1) {
    //     return -1;
    // }
    return Math.round(sum / totalCount * 10) / 10;
}


const userItemMatrixFileWriteStream = fs.createWriteStream('userItemMatrix.csv');

const userItemMatrixCsvStringifierWriteStream = stringify({
    header: true
});

userItemMatrixCsvStringifierWriteStream.on('readable', function () {
    let chunk;
    while ((chunk = userItemMatrixCsvStringifierWriteStream.read()) !== null) {
        userItemMatrixFileWriteStream.write(chunk);
    }
});
userItemMatrixCsvStringifierWriteStream.on('error', function (err) {
    console.error('stringifier failed with err', err);
});
userItemMatrixCsvStringifierWriteStream.on('finish', function () {
    console.log('userItemMatrixCsvStringifierWriteStream is ended!');
});

const restaurantsReadStream = fs.createReadStream('restaurants.ndjson', { encoding: 'utf8' })
const rl = readline.createInterface({
    input: restaurantsReadStream,
    crlfDelay: Infinity
});

const restaurants = {};
rl.on('line', (line) => {
    if (!line.trim()) {
        return;
    }

    // data.poiId
    // data.name
    // data.urlUI
    // data.shortenUrl
    const data = JSON.parse(line);
    const restaurantId = Number(data.poiId);
    if (!restaurantId || restaurants[restaurantId]) {
        return;
    }
    restaurants[restaurantId] = {
        restaurantId: data.poiId,
        restaurantName: data.name,
        restaurantShortedUrl: data.shortenUrl,
    }
});

await events.once(rl, 'close');

let count = 0;
let addedCount = 0;
const missingRestaurants = [];
const restaurantsAlreadyDone = [];

try {
    const restaurantReviewsReadStream = fs.createReadStream('restaurantReviews.ndjson', { encoding: 'utf8' })
    const rl = readline.createInterface({
        input: restaurantReviewsReadStream,
        crlfDelay: Infinity
    });

    rl.on('line', (line) => {
        if (!line.trim()) {
            return;
        }
        const currentValue = JSON.parse(line);

        const restaurantId = getRestaurantId(currentValue.restaurant);
        if (!restaurantId) {
            return;
        }
        restaurantsAlreadyDone.push(restaurantId);
        count++;
        if (count % 100 === 0) {
            console.log(count);
        }
        process.stdout.write('.');

        if (currentValue.restaurantReviews.length < 1) {
            return;
        }
        if (!restaurants[restaurantId]) {
            missingRestaurants.push(restaurantId);
            return;
        }
        if (restaurantsAlreadyDone.includes(restaurantId)) {
            return;
        }

        currentValue.restaurantReviews.forEach((review) => {
            const rating = getRating(review);
            const userId = getUserId(review);
            if (rating < 0 || !userId) {
                return;
            }

            const reviewTitle = review.reviewTitle;
            addedCount++;
            userItemMatrixCsvStringifierWriteStream.write({
                ...restaurants[restaurantId],
                userId,
                rating
            });
        });
    });

    await events.once(rl, 'close');
} catch (err) {
    console.error(err);
}

userItemMatrixCsvStringifierWriteStream.end();
userItemMatrixFileWriteStream.end("", "utf8", () => {
    console.log(`${addedCount} records Added!`);
    console.log("userItemMatrix.csv writable stream is ended!");
});

if (missingRestaurants.length > 0) {
    console.log();
    console.log(`${missingRestaurants.length} missing restaurants! ${missingRestaurants} ! They might have been shut down!`);
}

console.timeEnd(__filename);
