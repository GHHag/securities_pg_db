require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const router = require('./routes');

const port = process.env.HTTP_PORT;
const api_url = process.env.API_URL;

const server = express();

server.use(bodyParser.urlencoded({ extended: true, limit: '50mb' }));
server.use(bodyParser.json());
server.use(api_url, router);

server.listen(port, () => {
    console.log(`Server live at ${port}`);
});
