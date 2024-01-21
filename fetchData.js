import * as cheerio from 'cheerio';

function getTextsOfElements($matchedElements) {
    return $matchedElements.map(function (i, el) {
        const $ = cheerio.load(el);
        return $(el).text().trim();
    }).toArray()
}

function getOpeningHours($matchedElements) {
    return $matchedElements.map(function (i, el) {
        const $ = cheerio.load(el);
        const days = $(el).find('.opening-hours-date').text().trim();
        const hours = $(el).find('.opening-hours-time div').map(function (i2, el2) {
            return $(el2).text().trim();
        }).toArray()
        return {
            [days]: hours
        }
    }).toArray()
}

async function scrapeRestaurant(restaurantWebpageUrl) {
    const response = await fetch(restaurantWebpageUrl);
    const body = await response.text();
    const $ = cheerio.load(body);

    const restaurantName = $('div.poi-name span.name').text().trim();
    const restaurantName2 = $('.poi-name-container .smaller-font-name').text().trim();
    const restaurantDistrict = $('.header-poi-district a').text().trim();
    const restaurantAddress = $('section.address-section > div.address-info-map-section > div.address-info-section > div.content > a').text().trim();
    const restaurantTransportation = $('section.transport-section > div').contents().eq(0).text().trim();
    const restaurantSeatCount = $('.more-info-section div.content').text().trim();
    const restaurantOverallStar = $('div.header-score').text().trim();
    const restaurantGoodOkBadCount = getTextsOfElements($('.header-smile-section .score-div'));
    const restaurantPriceRange = $("[itemprop='priceRange'] > a").text().trim();
    const restaurantCategories = getTextsOfElements($("div.header-poi-categories a"));
    const restaurantTelephones = getTextsOfElements($("section.telephone-section div.content"));
    const restaurantAdditionalInfo = $("section.poi-additional-info-section .text").text().trim();
    const restaurantPaymentMethods = getTextsOfElements($("div.comma-tags > span"));
    const restaurantOtherMethods = getTextsOfElements($(".conditions-section span.condition-name"));
    const restaurantBookmarkCount = $(".header-bookmark-count").text().trim();
    const restaurantOpeningHours = getOpeningHours($('.opening-hours-day'));

    return {
        restaurantWebpageUrl,
        restaurantName,
        restaurantName2,
        restaurantDistrict,
        restaurantAddress,
        restaurantTransportation,
        restaurantSeatCount,
        restaurantOverallStar,
        restaurantGoodOkBadCount,
        restaurantPriceRange,
        restaurantCategories,
        restaurantTelephones,
        restaurantAdditionalInfo,
        restaurantPaymentMethods,
        restaurantOtherMethods,
        restaurantBookmarkCount,
        restaurantOpeningHours
    }
}

function getImmediateChildTexts($matchedElements) {
    return $matchedElements.contents().not('img').not('br').not('a').text().trim();
}

async function scrapeRestaurantReviews(restaurantReviewWebpageUrl, pageNum = 1) {
    const url = `${restaurantReviewWebpageUrl}?page=${pageNum}`
    const response = await fetch(url);
    const body = await response.text();
    const $ = cheerio.load(body);

    const restaurantReviews = $("div[itemprop='review']").map(function (i, el) {
        const $ = cheerio.load(el);
        // some reviews are from non-member without userId, might need to exclude in later pipeline
        const userId = $("[itemprop='author'] a").attr('href');
        const userName = $("[itemprop='author'] span").text().trim();
        const userLevel = $(".grade-name").text().trim();
        const reviewCount = $(".no-reviews").text().trim();
        const reviewTitle = $(".review-title .title").text().trim();
        const reviewComment = getImmediateChildTexts($(".main-review .review-container"));
        const dateOfVisit = $(".info div.title:contains('Date of Visit') + div.text").text().trim();
        const typeOfMeal = $(".info div.title:contains('Type of Meal') + div.text").text().trim();
        const diningMethod = $(".info div.title:contains('Dining Method') + div.text").text().trim();
        const spendingPerHead = $(".info div.title:contains('Spending Per Head') + div.text").text().trim();
        const reviewDate = $("[itemprop='datepublished']").text().trim();
        const reviewViewCount = $(".view-count").text().trim();
        const recommendedDishes = getTextsOfElements($(".recommend-dish-name-list .dish-name"));
        const taste = $(".name:contains('Taste')").length < 0 ? -1 : $(".name:contains('Taste') + div.stars .common_yellowstar_desktop").length;
        const decor = $(".name:contains('Decor')").length < 0 ? -1 : $(".name:contains('Decor') + div.stars .common_yellowstar_desktop").length;
        const service = $(".name:contains('Service')").length < 0 ? -1 : $(".name:contains('Service') + div.stars .common_yellowstar_desktop").length;
        const hygiene = $(".name:contains('Hygiene')").length < 0 ? -1 : $(".name:contains('Hygiene') + div.stars .common_yellowstar_desktop").length;
        const value = $(".name:contains('Value')").length < 0 ? -1 : $(".name:contains('Value') + div.stars .common_yellowstar_desktop").length;

        return {
            userId,
            userName,
            userLevel,
            reviewCount,
            reviewTitle,
            reviewComment,
            dateOfVisit,
            typeOfMeal,
            diningMethod,
            reviewDate,
            reviewViewCount,
            recommendedDishes,
            taste,
            decor,
            service,
            hygiene,
            value,
            spendingPerHead,
            url,
        }
    }).toArray();

    const nextButton = $('div.common_pagination_more_r_desktop');
    if (nextButton.length < 1) {
        return restaurantReviews;
    }

    const nextPageRestaurantReviews = await scrapeRestaurantReviews(restaurantReviewWebpageUrl, pageNum + 1);
    return restaurantReviews.concat(nextPageRestaurantReviews);
}

export function getRestaurantId(restaurant) {
    if (restaurant.restaurantId) {
        return Bumber(restaurant.restaurantId);
    }
    if (restaurant.restaurantWebpageUrl) {
        const urlParts = restaurant.restaurantWebpageUrl.split('/');
        const slug = urlParts[urlParts.length - 1];
        const slugSplits = slug.split('-');
        const restaurantId = slugSplits[slugSplits.length - 1].replace('r', '');
        return Number(restaurantId);
    }
    return "";
}

export default async function fetchData(restaurantWebpageUrl, restaurantReviewWebpageUrl) {
    const restaurant = await scrapeRestaurant(restaurantWebpageUrl);
    // console.log(restaurant);
    const restaurantReviews = await scrapeRestaurantReviews(restaurantReviewWebpageUrl);
    // console.log(restaurantReviews);
    // const used = process.memoryUsage().heapUsed / 1024 / 1024;
    // console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);
    return {
        restaurant,
        restaurantReviews
    };
}