const pool = require('./db.js');

const insertExchange = async (req, res) => {
    if (!req.body.exchangeName || !req.body.currency) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        let exchangeInsert = await pool.query(
            `
            INSERT INTO exchanges(exchange_name, currency)
            VALUES($1, $2)
            `, [req.body.exchangeName, req.body.currency]
        );

        res.status(200).json({
            result: `
                ${exchangeInsert.command} ${exchangeInsert.rowCount} rows
            `, success: true
        });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getExchange = async (req, res) => {
    if (!req.params.name) {
        res.status(500).json({ success: false, error: 'Incorrect params' });
        return;
    }

    try {
        let exchangeQuery = await pool.query(
            `
            SELECT id, exchange_name, currency
            FROM exchanges
            WHERE UPPER(exchange_name) = $1
            `, [req.params.name.toUpperCase()]
        );

        res.status(200).json({ data: exchangeQuery.rows, success: true });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const insertInstrument = async (req, res) => {
    if (!req.params.id || !req.body.symbol) {
        res.status(500).json({ success: false, error: 'Incorrect params/body' });
        return;
    }

    try {
        let instrumentInsert = await pool.query(
            `
            INSERT INTO instruments(exchange_id, symbol)
            VALUES($1, $2)
            `, [req.params.id, req.body.symbol]
        );

        res.status(200).json({
            result: `
                ${instrumentInsert.command} ${instrumentInsert.rowCount} rows
            `, success: true
        });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getInstrument = async (req, res) => {
    if (!req.params.symbol) {
        res.status(500).json({ success: false, error: 'Incorrect params' });
        return;
    }

    try {
        let instrumentQuery = await pool.query(
            `
            SELECT id, exchange_id, symbol
            FROM instruments
            WHERE UPPER(symbol) = $1
            `, [req.params.symbol.toUpperCase()]
        );

        res.status(200).json({ data: instrumentQuery.rows, success: true });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const insertPriceData = async (req, res) => {
    if (!req.params.id || !req.body.data) {
        res.status(500).json({ success: false, error: 'Incorrect params/body' });
        return;
    }

    try {
        let existingDates = [];
        let priceDataInserts = 0;

        const priceDataInsertPromise = await JSON.parse(req.body.data).map(async priceData => {
            const incorrectDataPoint = !priceData.open || !priceData.high ||
                !priceData.low || !priceData.close ||
                !priceData.hasOwnProperty('volume') || !priceData.date;
            if (incorrectDataPoint) {
                res.status(500).json({
                    success: false, error: 'Incorrect format of data point'
                });
                return;
            }

            /* let priceDataInsert = await pool.query(
                `
                WITH
                    check_date AS (
                        SELECT instrument_id, date_time
                        FROM price_data
                        WHERE instrument_id = $1
                        AND date_time = $7
                    )
                    INSERT INTO price_data(
                        instrument_id, open_price, high_price, 
                        low_price, close_price, volume, date_time
                    )
                    SELECT $1, $2, $3, $4, $5, $6, $7
                    WHERE NOT EXISTS(
                        SELECT date_time
                        FROM check_date
                        WHERE date_time = $7
                    )
                `,
                [
                    req.params.id, priceData.open, priceData.high,
                    priceData.low, priceData.close, priceData.volume,
                    priceData.date
                ]
            ); */

            let checkDateQuery = await pool.query(
                `
                SELECT instrument_id, date_time
                FROM price_data
                WHERE instrument_id = $1
                AND date_time = $2
                `, [req.params.id, priceData.date]
            );
            //if (!priceDataInsert.rowCount > 0) {
            if (checkDateQuery.rowCount > 0) {
                existingDates.push(priceData.date);
            }
            else {
                let priceDataInsert = await pool.query(
                    `
                    INSERT INTO price_data(
                        instrument_id, open_price, high_price, 
                        low_price, close_price, volume, date_time
                    )
                    VALUES(
                        $1, $2, $3, $4, $5, $6, $7
                    )
                    `,
                    [
                        req.params.id, priceData.open, priceData.high,
                        priceData.low, priceData.close, priceData.volume,
                        priceData.date
                    ]
                );
                priceDataInserts += priceDataInsert.rowCount;
            }
        });
        await Promise.all(priceDataInsertPromise);

        res.status(200).json({
            result: `
                INSERT ${priceDataInserts} rows
            `, prevExistingDates: existingDates, success: true
        });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getPriceData = async (req, res) => {
    if (!req.params.symbol || !req.body.startDateTime || !req.body.endDateTime) {
        res.status(500).json({ success: false, error: 'Incorrect params/body' });
        return;
    }

    try {
        let priceDataQuery = await pool.query(
            `
            SELECT instruments.symbol,
                price_data.open_price, price_data.high_price, 
                price_data.low_price, price_data.close_price,
                price_data.volume, price_data.date_time AT TIME ZONE 'UTC' AS date_time
            FROM instruments, price_data
            WHERE instruments.id = price_data.instrument_id
            AND UPPER(instruments.symbol) = $1
            AND price_data.date_time >= $2
            AND price_data.date_time <= $3
            ORDER BY price_data.date_time
            `,
            [
                req.params.symbol.toUpperCase(), req.body.startDateTime,
                req.body.endDateTime
            ]
        );

        res.status(200).json({ data: priceDataQuery.rows, success: true });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = {
    insertExchange,
    getExchange,
    insertInstrument,
    getInstrument,
    insertPriceData,
    getPriceData
}