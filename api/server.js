require('dotenv').config();
const express = require('express');
//const cors = require('cors');
const bodyParser = require('body-parser');
const appRouter = require('./routes');

const app = express();

//app.use(cors());
app.use(bodyParser.json());
app.use('/api', appRouter);

const port = process.env.HTTP_PORT;

app.listen(port, () => {
    console.log(`Server live at ${port}`);
});
