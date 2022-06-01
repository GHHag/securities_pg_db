import json
import datetime as dt

from yahooquery import Ticker

from instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb


def get_yahooquery_data(
    instrument, *args, 
    start_date=dt.datetime.now(), end_date=dt.datetime.now(), 
    omx_stock=False
):
    try:
        if omx_stock:
            if '^' in instrument:
                data = Ticker(instrument.upper()).history(start=start_date, end=end_date)
            else:
                data = Ticker(
                    instrument.upper().replace('_', '-') + '.ST'
                ).history(start=start_date, end=end_date)
        else:
            data = Ticker(instrument.upper()).history(start=start_date, end=end_date)
        data.reset_index(inplace=True)
        return data.rename(
            columns={
                'volume': 'Volume', 'date': 'Date',
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'
            }
        )
    except (KeyError, AttributeError, TypeError):
        print('Error trying to fetch data', instrument)


def post_daily_data(
    symbol_list, exchange_name, start_date, end_date, omx_stock=False
):
    none_tickers = []
    for ticker in symbol_list:
        df = get_yahooquery_data(
            ticker, start_date=start_date, end_date=end_date, omx_stock=omx_stock
        )
        if df is None:
            none_tickers.append(ticker)
            continue

        try:
            #insert_dataframe(connection, cursor, exchange_name, df, ticker_col_name='symbol')
            pass
        except Exception:
            print('EXCEPTION', ticker)
            none_tickers.append(ticker)

    print(none_tickers)


if __name__ == '__main__':
    import env

    INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, 'instruments_db')

    symbol_list = json.loads(INSTRUMENTS_DB.get_omxs_large_cap_instruments()) + \
        json.loads(INSTRUMENTS_DB.get_omxs_mid_cap_instruments())

    start_date = dt.datetime(2022, 5, 26)
    end_date = dt.datetime(2022, 6, 1)

    print(
        f'\nInsert data\n' # for {exchange_name}\n'
        f'Start date: {start_date.strftime("%d-%m-%Y")}\n'
        f'End date: {end_date.strftime("%d-%m-%Y")}\n'
        f'proceed? y/n'
    )
    yes_no_input = input('Enter: ')
    if yes_no_input.lower() == 'y':
        post_daily_data(
            symbol_list, start_date=start_date, end_date=end_date #, omx_stock=omxs_stock
        )
