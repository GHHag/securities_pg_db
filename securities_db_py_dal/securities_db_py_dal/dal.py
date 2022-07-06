import json
import datetime as dt
import pytz
import requests
import logging

from yahooquery import Ticker

from instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from securities_db_py_dal.market_data import get_stock_indices_symbols_list, \
    get_futures_symbols_list
import securities_db_py_dal.env as env


def get_yahooquery_data(
    instrument, *args, 
    start_date=dt.datetime.now(), end_date=dt.datetime.now(), 
    omxs_stock=False
):
    try:
        if omxs_stock:
            if '^' in instrument:
                data = Ticker(
                    instrument.upper()).history(start=start_date, end=end_date
                )
            else:
                data = Ticker(
                    instrument.upper().replace('_', '-') + '.ST'
                ).history(start=start_date, end=end_date)
        else:
            data = Ticker(instrument.upper()).history(start=start_date, end=end_date)
        data.reset_index(inplace=True)
        return data
    except (KeyError, AttributeError, TypeError):
        logging.error(
            f'\n\tERROR while trying to fetch data with yhq. Instrument: {instrument}\n'
        )


def exchange_post_req(exchange_data):
    exchange_post_res = requests.post(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/exchange',
        data={
            "exchangeName": exchange_data['name'], 
            "currency": exchange_data['currency']
        }
    )

    logging.info(f'\n\tEXCHANGE POST REQUEST:\n\t{exchange_post_res.content}\n')
    return exchange_post_res.content


def exchange_get_req(exchange_name):
    return requests.get(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/exchange/{exchange_name}',
    ).json()


def instrument_post_req(exchange_id, symbol):
    instrument_post_res = requests.post(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/instrument/{exchange_id}',
        data={"symbol": symbol}
    )

    logging.info(f'\n\tINSTRUMENT POST REQUEST:\n\t{instrument_post_res.content}\n')
    return instrument_post_res.content


def instrument_get_req(symbol):
    return requests.get(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/instrument/{symbol}'
    ).json()


def price_data_post_req(instrument_id, df_json):
    price_data_post_res = requests.post(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/price-data/{instrument_id}',
        data={"data": json.dumps(df_json['data'])}
    )

    logging.info(f'\n\tPRICE DATA POST REQUEST:\n\t{price_data_post_res.content}\n')
    return price_data_post_res.content  


def price_data_get_req(symbol, start_date_time, end_date_time):
    return requests.get(
        f'http://{env.DATABASE_HOST}:{env.HTTP_PORT}{env.API_URL}/price-data/{symbol}',
        data={
            'startDateTime': start_date_time,
            'endDateTime': end_date_time
        }
    ).json()


def post_daily_data(
    symbols_list, exchange_name, start_date, end_date, omxs_stock=False
):
    for symbol in symbols_list:
        df = get_yahooquery_data(
            symbol, start_date=start_date, end_date=end_date, omxs_stock=omxs_stock
        )

        exception_none_df_symbols = ''
        if df is None or len(df) == 0:
            exception_none_df_symbols += f'{symbol}, '
        else:
            df_json = json.loads(df.to_json(orient='table'))

            try:
                exchange_get_res = exchange_get_req(exchange_name)
                exchange_id = exchange_get_res['data'][0]['id']

                instrument_post_req(exchange_id, symbol)

                instrument_get_res = instrument_get_req(symbol)
                instrument_id = instrument_get_res['data'][0]['id']

                price_data_post_req(instrument_id, df_json)

            except Exception:
                logging.error(
                    f'\n\t'
                    f'EXCEPTION raised while attempting to POST data for {symbol}'
                )
                exception_none_df_symbols += f'{symbol}, '

    logging.warning(
        f"\n\t"
        f"Symbols where conditional: 'df is None or len(df) == 0:' resulted in True\n\t"
        f"Symbols: {exception_none_df_symbols}\n"
    )


if __name__ == '__main__':
    logging.basicConfig(filename=f'{env.LOG_FILE_PATH}\stonkinator_log.log', level=logging.INFO)

    INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, 'instruments_db')

    #omxs_stock_symbols_list = json.loads(INSTRUMENTS_DB.get_omxs30_instruments())
    omxs_stock_symbols_list = json.loads(INSTRUMENTS_DB.get_omxs_large_cap_instruments()) + \
        json.loads(INSTRUMENTS_DB.get_omxs_mid_cap_instruments()) #+
    #    json.loads(INSTRUMENTS_DB.get_omxs_small_cap_instruments()) +
    #    json.loads(INSTRUMENTS_DB.get_first_north25_instruments())
    stock_indices_symbols_list = get_stock_indices_symbols_list()
    futures_symbols_list = get_futures_symbols_list()

    exchanges_dict = {
        'omxs': {
            'name': 'OMXS',
            'currency': 'SEK',
            'symbols': omxs_stock_symbols_list
        },
        'stock indices': {
            'name': 'Stock indices',
            'currency': 'USD',
            'symbols': stock_indices_symbols_list
        },
        'futures': {
            'name': 'Futures',
            'currency': 'USD',
            'symbols': futures_symbols_list
        }
    }

    start_date = dt.datetime(2022, 6, 25, tzinfo=pytz.timezone('Europe/Berlin'))
    end_date = dt.datetime.now(tz=pytz.timezone('Europe/Berlin'))
    dt_now = dt.datetime.now(tz=pytz.timezone('Europe/Berlin'))

    logging.info(
        f'\n\t'
        f'Insert data\n\t'
        f'Current datetime: {dt_now}\n\t'
        f'Start date: {start_date.strftime("%d-%m-%Y")}\n\t'
        f'End date: {end_date.strftime("%d-%m-%Y")}\n'
    )

    yes_no_input = 'y' # input('Enter: ')
    if yes_no_input.lower() == 'y':
        for exchange, exchange_data in exchanges_dict.items():
            exchange_post_req(exchange_data)

            end_date_today_check = dt_now.year == end_date.year and \
                dt_now.month == end_date.month and \
                dt_now.day == end_date.day
            omxs_stock = False
            if exchange == 'omxs':
                omxs_stock = True
                if end_date_today_check and dt_now.hour < 18:
                    end_date = end_date - dt.timedelta(days=1)
                    logging.info(
                        f'\n\t'
                        f'Date check (omxs): {end_date_today_check}, subtracting one day\n\t'
                        f'New end date: {end_date}\n'
                    )
            else:
                if end_date_today_check:
                    end_date = end_date - dt.timedelta(days=1)
                    logging.info(
                        f'\n\t'
                        f'Date check: {end_date_today_check}, subtracting one day\n\t'
                        f'New end date: {end_date}\n'
                    )

            post_daily_data(
                exchange_data['symbols'], exchange, 
                start_date=start_date, end_date=end_date, 
                omxs_stock=omxs_stock
            )
