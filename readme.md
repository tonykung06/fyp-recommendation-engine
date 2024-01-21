## Openrice Scraper
### How to run end-to-end?
```
npm install
npm start
```
Note:
1. This is a long-running process that could take hours due to limited API parallelism to avoid API throttling.
2. Please reserve at least 100MB of memory.
3. Please consider data streaming pipeline on Apache Flink if the data gets bigger beyond limit of a single machine and high parallelism is needed to scale for faster scraping and processing.
4. Please use proxy service with IP rotation from a IP pool to get around API throttling if (3) is used."# fyp-recommendation-engine" 
