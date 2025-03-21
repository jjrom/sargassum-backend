'use strict';
const express = require('express');
const duckdb = require('duckdb');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const app = express();
const port = 3001;

// Handle result
const MINIO_HTTPS_PATH = "https://minio.dive.edito.eu/project-sargasse/GeoParquet/";
//const MINIO_HTTPS_PATH = "/data/";

// Caching mechanism
const useCache = true;
const CACHE_DIR = '/cache';
if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR);
}

// Define the forecast period
const firstForecast = new Date("2024-01-01");
const latestForecast = new Date("2025-03-01");

// EEZ parquet file
const eezParquetFile = MINIO_HTTPS_PATH + "eez.parquet";

// Connect to DuckDB
const db = new duckdb.Database("/data/duckdb.db");

db.exec("INSTALL spatial; LOAD spatial;");

// to support JSON-encoded bodies
app.use(express.json());

// to support URL-encoded bodies
app.use(express.urlencoded({ extended: true }));

app.use(function (req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    next();
});

app.get("/", async (req, res) => {
    res.status(200).json({"message":"hello"});
});

/**
 * Get forecast at a specific timestamp (YYYY-MM-DD)
 */
app.get("/forecast/:timestamp", async (req, res) => {

    const limit = req.query.limit || null;
    const value = req.query.value || null;
    const timestamp = req.params.timestamp;

    var sargaseParquetFile;
    try {
        sargaseParquetFile = getSargasseParquetFile(timestamp);
        if (!sargaseParquetFile) {
            throw new Error("Forecast not available for " + timestamp);
        }
    }
    catch (error) {
        return res.status(404).json({ error: error.message });
    }

    var query = `
        SELECT time, ST_AsGeoJSON(geometry) as geometry, value FROM '${sargaseParquetFile}'
        WHERE time = '${timestamp}T12:00:00'
    `;

    if (value) {
        query += ` AND value > ${value}`;
    }

    if (limit) {
        query += ` LIMIT ${limit}`;
    }

    try {

        db.all(query, (err, rows) => {

            if (err) {
                throw err;
            }

            // Construct GeoJSON FeatureCollection
            const geojson = {
                type: 'FeatureCollection',
                links: [
                    {
                        href: sargaseParquetFile,
                        rel: 'data',
                        title: 'Forecast data used',
                        type: 'application/vnd.apache.parquet'
                    }
                ],
                features: rows.map(row => ({
                    type: 'Feature',
                    properties: {
                        time: row.time,
                        value: row.value
                    },
                    geometry: JSON.parse(row.geometry), // Parsing GeoJSON geometry
                }))
            };

            // Return the GeoJSON response
            res.json(geojson);
        });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

/**
 * Get daily sums per eez
 */
app.get("/forecast/:timestamp/volume/:eez", async (req, res) => {

    const timestamp = req.params.timestamp;

    var sargaseParquetFile;
    try {
        //sargaseParquetFile = getSargasseParquetFile(latestForecast.toISOString().split('T')[0]);
        sargaseParquetFile = getSargasseParquetFile(timestamp);
    }
    catch (error) {
        res.status(404).json({ error: error.message });
    }

    /*var query = `
        SELECT p.time, r.AREA_KM2 as eez_area, LIST(p.value) AS values_array FROM '${sargaseParquetFile}' p
        JOIN '${eezParquetFile}' r
        ON ST_Intersects(p.geometry, r.geometry)
        WHERE r.GEONAME = '${req.params.eez}'
        GROUP BY p.time,r.AREA_KM2
        ORDER BY p.time ASC;
    `;*/

    var query = `
        SELECT p.time, p.value, ST_AsGeoJSON(p.geometry) as geometry, CAST(r.AREA_KM2 AS INTEGER) as eez_area FROM '${sargaseParquetFile}' p
        JOIN '${eezParquetFile}' r
        ON ST_Intersects(p.geometry, r.geometry)
        WHERE r.GEONAME = '${req.params.eez}'
        ORDER BY p.time ASC;
    `;

    try {

        const queryHash = hashQuery(query);
        const cachedResult = getCachedResult(queryHash);
        
        if (useCache && cachedResult) {
            console.log('Returning cached result');
            return res.json(cachedResult);
        }
        
        db.all(query, (err, rows) => {

            if (err) {
                throw err;
            }
            
            var values = [];
            if (rows.length > 0) {
                var lastTime = rows[0].time;
                var currentValue = 0;
                var eez_area = rows[0].eez_area;
                for (var i = 0, ii = rows.length; i < ii; i++) {
                    if (rows[i].time.getTime() !== lastTime.getTime()) {
                        values.push({
                            date:lastTime,
                            m2PerKm2:currentValue / eez_area
                        });
                        lastTime = rows[i].time;
                        currentValue = 0;
                    }
                    //currentValue += sargcToSquareMeters(rows[i].value, JSON.parse(rows[i].geometry));
                    currentValue = currentValue + (rows[i].value * 1000000);

                }
            }

            // Construct GeoJSON FeatureCollection
            const json = {
                eez: req.params.eez,
                eez_area:eez_area,
                links: [
                    {
                        href: sargaseParquetFile,
                        rel: 'data',
                        title: 'Forecast data used',
                        type: 'application/vnd.apache.parquet'
                    }
                ],
                values:values
            };

            if (useCache) {
                saveToCache(queryHash, json);
            }

            // Return the JSON response
            res.json(json);
        });

    } catch (error) {
        console.log(error);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Get daily sums per eez
 */
app.get("/volume/:eez/:year", async (req, res) => {

    var parquetFile = MINIO_HTTPS_PATH + "sargassum_year_" + req.params.year + ".parquet";
    var query = `
        SELECT p.time, SUM(p.value) AS total_value FROM '${parquetFile}' p
        JOIN '${eezParquetFile}' r
        ON ST_Intersects(p.geometry, r.geometry)
        WHERE r.GEONAME = '${req.params.eez}'
        GROUP BY p.time
        ORDER BY p.time ASC;
    `;

    try {

        const queryHash = hashQuery(query);
        const cachedResult = getCachedResult(queryHash);
        
        if (cachedResult) {
            console.log('Returning cached result');
            return res.json(cachedResult);
        }
        
        db.all(query, (err, rows) => {

            if (err) {
                throw err;
            }

            // Construct GeoJSON FeatureCollection
            const json = {
                eez: req.params.eez,
                values: rows.map(row => ({
                    date: row.time,
                    value: row.total_value
                }))
            };

            if (useCache) {
                saveToCache(queryHash, json);
            }
            
            // Return the JSON response
            res.json(json);
        });

    } catch (error) {
        console.log(error);
        res.status(500).json({ error: error.message });
    }
});


/**
 * Get data within a bounding box
 * Request format: POST /data/bbox { "min_lon": -80, "max_lon": -60, "min_lat": 10, "max_lat": 30 }
 */
app.post("/data/bbox", async (req, res) => {
    const { min_lon, max_lon, min_lat, max_lat } = req.body;

    // Create a bounding box polygon
    const boundingBoxWKT = `POLYGON((${min_lon} ${min_lat}, ${min_lon} ${max_lat}, ${max_lon} ${max_lat}, ${max_lon} ${min_lat}, ${min_lon} ${min_lat}))`;

    const query = `
        SELECT time, ST_AsGeoJSON(geometry) as geometry, value FROM '${PARQUET_FILE}'
        WHERE ST_Intersects(geometry, ST_GeomFromText('${boundingBoxWKT}'))
    `;

    try {
        const result = await new Promise((resolve, reject) => {
            db.all(query, (err, rows) => (err ? reject(err) : resolve(rows)));
        });
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});


/** =================================================================================== */

/**
 * Hash a query to be used as a cache key
 * 
 * @param {string} query 
 * @returns 
 */
function hashQuery(query) {
    return crypto.createHash('sha256').update(query).digest('hex');
}

/**
 * Get cached result
 * 
 * @param {string} hash 
 * @returns 
 */
function getCachedResult(hash) {
    const cacheFile = path.join(CACHE_DIR, `${hash}.json`);
    if (fs.existsSync(cacheFile)) {
        return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    }
    return null;
};

/**
 * Save data to cache
 * 
 * @param {string} hash 
 * @param {string} data 
 */
function saveToCache(hash, data) {
    const cacheFile = path.join(CACHE_DIR, `${hash}.json`);
    fs.writeFileSync(cacheFile, JSON.stringify(data), 'utf8');
};

/**
 * Return the path to the Parquet file for a specific timestamp
 * 
 * The forecast is 7 month length and processed on the first of every month
 * Given the timestamp, get the better forecast if exists
 * 
 * @param {string} timestamp as YYYY-MM-DD
 */
function getSargasseParquetFile(timestamp) {

    var endOfLatestForecast = new Date(latestForecast.getTime());
    endOfLatestForecast.setMonth(endOfLatestForecast.getMonth() + 7);

    var d = new Date(timestamp);
    if (parseInt(timestamp.split('-')[2]) < 16) {
        d.setMonth(d.getMonth() - 1);
    }
    if (d < firstForecast) {
        throw new Error("Forecast not available before " + firstForecast.toISOString().split('T')[0]);
    }
    if (d > endOfLatestForecast) {
        throw new Error("Forecast not available after " + endOfLatestForecast.toISOString().split('T')[0]);
    }

    // Best forecast is the closest first of the month
    d.setDate(1);
    if (d > latestForecast) {
        d = latestForecast;
    }
    var goodForecast = d.toISOString().split('T')[0].split('-').slice(0, 2).join('') + "01";

    return MINIO_HTTPS_PATH + goodForecast + "_sarg_mean.parquet";

}

/*
function sargcToSquareMeters(sargc, geometry) {

    const DEG_TO_RAD = Math.PI / 180;
    
    // Cell size is 1/4 degrees around the geometry centroid
    const lon1 = geometry.coordinates[0] - 0.125;
    const lon2 = geometry.coordinates[0] + 0.125;
    const lat1 = geometry.coordinates[1] - 0.125;
    const lat2 = geometry.coordinates[1] + 0.125;
    
    // Convert latitudes to radians
    let latAvg = ((lat1 + lat2) / 2) * DEG_TO_RAD;
    
    // Each degree of latitude is approximately 111.32 km
    let latDiffKm = Math.abs(lat1 - lat2) * 111.32;
    
    // Each degree of longitude varies with latitude
    let lonDiffKm = Math.abs(lon1 - lon2) * (111.32 * Math.cos(latAvg));
    
    // Calculate the surface area (width * height)
    let areaKm2 = latDiffKm * lonDiffKm;
    
    return sargc * 1000000;

}*/