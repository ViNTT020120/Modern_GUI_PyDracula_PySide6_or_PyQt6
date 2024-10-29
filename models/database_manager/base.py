import duckdb
import os
import pandas as pd
import asyncio
from pathlib import Path
import json
from models.util import get_futures
import atexit


class BaseDatabaseManager:
    def __init__(self):
        self._create_today_folder()
        self._conn = duckdb.connect(os.path.join(self._save_path, 'data.ddb'))
        # Register the cleanup function to be called at exit
        atexit.register(self.close_db)

    def close_db(self):
        """Function to close the DuckDB connection."""
        if self._conn:
            print('Closing database')
            self._conn.close()

    def _create_today_folder(self):
        """
        Create a folder for today's date inside the 'save' directory and check for 'login.pkl'.

        This function constructs the path to the 'save' directory, which is
        expected to be a sibling of the directory containing the executing
        script. It then checks if a folder named with today's date
        (formatted as 'YYYY-MM-DD') already exists within the 'save'
        directory. If the folder does not exist, it creates the folder.

        Additionally, it checks for a 'login.pkl' file inside today's folder.
        If the file does not exist, it creates a 'login.pkl' file with an
        empty dictionary inside.

        Usage:
            Call this function from a script located in the 'src' folder
            to automatically create a date-specific folder in the
            corresponding 'save' folder.
        """
        from datetime import datetime

        # Get the directory of the currently executing script
        script_dir = Path(__file__).parents[2]

        # Construct the path to the 'save' folder
        # save_path = os.path.normpath(os.path.join(script_dir, '..', 'save'))
        save_path = os.path.join(script_dir, 'save')
        self._config_path = os.path.join(script_dir, 'config')

        # Get today's date in 'YYYY-MM-DD' format
        today = datetime.now().strftime('%Y-%m-%d')

        # Create the full path for today's folder

        self._save_path = os.path.join(save_path, today)

        # Check if the folder already exists, create it if not
        if not os.path.exists(self._save_path):
            os.makedirs(self._save_path)

    @staticmethod
    async def _extract_etf_basket_info(directory, file):
        etf_code = file.split('_')[0]
        file_path = os.path.join(directory, file)
        df = pd.read_excel(file_path, usecols=[0, 2, 3, 5, 6])
        df.columns = ['ETF code', 'Ticker', 'Weight', 'Price', 'FOL']
        df = df[df['ETF code'] == etf_code][['Ticker', 'Weight', 'Price', 'FOL']]
        if df['Weight'].sum() > 1:
            df['Weight'] /= 100

        # calculate FOL ratio
        # Create a mask to filter rows where the column contains 'KIS' or 'AP'
        mask = df['FOL'].str.contains('KIS|AP', na=False)

        # Calculate the sum of the 'Tỷ lệ' column for the filtered rows
        fol_ratio = df.loc[mask, 'Weight'].sum()
        nfol_ratio = df.loc[~mask, 'Weight'].sum()

        baskets = df.set_index('Ticker')[['Weight', 'Price']].to_dict()
        return etf_code, baskets, fol_ratio, nfol_ratio

    async def _fetch_etf_basket_async(self):
        directory = os.path.join(self._config_path, 'etf_basket')
        files = os.listdir(directory)
        tasks = [self._extract_etf_basket_info(directory, file) for file in files]
        _etf_baskets = await asyncio.gather(*tasks)

        _ticker_to_etf_basket = {}
        for etf_code, baskets, _, _ in _etf_baskets:
            for ticker, price in baskets['Price'].items():
                if ticker in _ticker_to_etf_basket:
                    _ticker_to_etf_basket[ticker]['etf_code'].append(etf_code)
                else:
                    _ticker_to_etf_basket[ticker] = {'price': price, 'etf_code': [etf_code]}

        return _etf_baskets, _ticker_to_etf_basket

    async def _create_order_book(self, ticker_to_etf_basket, etf_baskets):
        self._conn.execute('DROP TABLE IF EXISTS order_book')
        self._conn.execute('''
            CREATE TABLE order_book (
                ticker VARCHAR,
                exchange VARCHAR DEFAULT 'HM',
                type VARCHAR DEFAULT 'S',
                bid FLOAT DEFAULT 0,
                trade FLOAT DEFAULT 0,
                ask FLOAT DEFAULT 0,
                bid_premium FLOAT DEFAULT 0,
                ask_discount FLOAT DEFAULT 0,
                etf_basket VARCHAR[] DEFAULT []
            )
        ''')

        for ticker, data in ticker_to_etf_basket.items():
            etf_codes = data['etf_code']
            price = data['price']
            self._conn.execute(f'''
                INSERT INTO order_book (ticker, bid, trade, ask, etf_basket)
                VALUES ('{ticker}', {price}, {price}, {price}, {etf_codes})
            ''')

        for etf_code, _, _, _ in etf_baskets:
            self._conn.execute(f'''
                INSERT INTO order_book (ticker, type)
                VALUES ('{etf_code}', 'E')
            ''')

        self._conn.commit()

    async def _create_futures(self):
        self._conn.execute('DROP TABLE IF EXISTS futures')
        self._conn.execute('''
            CREATE TABLE futures (
                ticker VARCHAR,
                time_to_maturity INTEGER
            )
        ''')

        futures_tickers = await get_futures()
        for item in futures_tickers.items():
            type = 'C1' if item[0] == 'vnc1' else 'C2'

            self._conn.execute(f'''
                INSERT INTO futures (ticker, time_to_maturity)
                VALUES ('{item[1][0]}', {item[1][1]})
            ''')
            self._conn.execute(f'''
                    INSERT INTO order_book (ticker, exchange, type, bid, ask)
                    VALUES ('{item[1][0]}', 'HN', '{type}', {item[1][2]}, {item[1][3]}),
                ''')
        self._conn.commit()

    async def _create_vn30_last_price(self, etf_baskets):
        self._conn.execute('DROP TABLE IF EXISTS vn30_last_price')
        self._conn.execute('''
            CREATE TABLE vn30_last_price (
                ticker VARCHAR,
                last_price INT
            )
        ''')
        for ticker, price in etf_baskets[0][1]['Price'].items():
            self._conn.execute(f'''
                INSERT INTO vn30_last_price (ticker, last_price)
                VALUES ('{ticker}', {price})
            ''')
        self._conn.commit()

    async def _create_etf_inav(self, etf_baskets):
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS etf_inav (
                etf_code VARCHAR,
                iNav FLOAT DEFAULT 0,
                basket_bid_premium FLOAT DEFAULT 0,
                basket_ask_discount FLOAT DEFAULT 0,
                fol FLOAT,
                nfol FLOAT
            )
        ''')

        for etf_code, _, fol, nfol in etf_baskets:
            self._conn.execute(f'''
                INSERT INTO etf_inav (etf_code, fol, nfol)
                VALUES ('{etf_code}', {fol}, {nfol})
            ''')
        self._conn.commit()

    async def _create_etf_components(self, etf_baskets):
        self._conn.execute('DROP TABLE IF EXISTS etf_components')
        self._conn.execute('''
            CREATE TABLE etf_components (
                etf_code VARCHAR,
                weights JSON
            )
        ''')

        for etf_code, baskets, _, _ in etf_baskets:
            weights = json.dumps(baskets['Weight'])
            self._conn.execute(f'''
                INSERT INTO etf_components (etf_code, weights)
                VALUES ('{etf_code}', {weights})
            ''')

        self._conn.commit()

    async def refresh_database(self):
        etf_baskets, ticker_to_etf_basket = await self._fetch_etf_basket_async()
        tasks = [
            self._create_order_book(ticker_to_etf_basket, etf_baskets),
            self._create_etf_inav(etf_baskets),
            self._create_etf_components(etf_baskets),
            self._create_futures(),
            self._create_vn30_last_price(etf_baskets)
        ]
        await asyncio.gather(*tasks)

    async def get_registered_tickers(self):
        # get tuples of (tickers, exchange) from order_book
        query = 'SELECT ticker, exchange FROM order_book'
        return self._conn.execute(query).fetchall()

    async def get_registered_etf_tickers(self):
        # get etf codes from etf_inav
        query = 'SELECT etf_code FROM etf_inav'
        return self._conn.execute(query).fetchall()

    async def get_registered_futures_tickers(self):
        # get futures tickers from futures
        query = 'SELECT ticker FROM futures'
        result = self._conn.execute(query).fetchall()
        return [row[0] for row in result]

    async def get_registered_equity_tickers(self):
        # get equities tickers from order_book where type = 'S' or 'E'
        query = "SELECT ticker FROM order_book WHERE type IN ('S', 'E')"
        result = self._conn.execute(query).fetchall()
        return [row[0] for row in result]

    async def update_order_book(self, ticker, update_dict: dict):
        """Update order info in orders_table"""
        # Construct the SET clause dynamically from the update_dict
        set_clause = ', '.join(
            [f"{col} = {repr(value)}" if isinstance(value, str) else f"{col} = {value}" for col, value in
             update_dict.items()])

        # Construct the SQL query
        query = f"UPDATE order_book SET {set_clause} WHERE ticker = '{ticker}'"

        # Execute the query (assuming self._conn is your database connection)
        self._conn.execute(query)
        self._conn.commit()

    async def update_etf_inav(self, etf_code, update_dict: dict):
        """Update order info in orders_table"""
        # Construct the SET clause dynamically from the update_dict
        set_clause = ', '.join(
            [f"{col} = {repr(value)}" if isinstance(value, str) else f"{col} = {value}" for col, value in
             update_dict.items()])

        # Construct the SQL query
        query = f"UPDATE etf_inav SET {set_clause} WHERE etf_code = '{etf_code}'"

        # Execute the query (assuming self._conn is your database connection)
        self._conn.execute(query)
        self._conn.commit()

    async def get_ticker_info_from_order_book(self, ticker, columns: list):
        query = f"SELECT {', '.join(columns)} FROM order_book WHERE ticker = '{ticker}'"
        return self._conn.execute(query).fetchone()

    async def get_ticker_info_from_etf_inav(self, etf_code, columns: list):
        query = f"SELECT {', '.join(columns)} FROM etf_inav WHERE etf_code = '{etf_code}'"
        return self._conn.execute(query).fetchone()

    async def get_ticker_info_from_futures(self, ticker, columns: list):
        query = f"SELECT {', '.join(columns)} FROM futures WHERE ticker = '{ticker}'"
        return self._conn.execute(query).fetchone()

    def print_order_book(self, ticker):
        self._conn.sql(f"SELECT * FROM vn30_last_price").show()
        self._conn.sql(f"SELECT bid FROM order_book where ticker = '{'VN30F2411'}'").show()
        self._conn.sql(f"SELECT * FROM etf_components").show()
        # self._conn.sql(
        #     f"SELECT iNav, nfol, fol FROM etf_inav WHERE etf_code = '{ticker}'").show()

    async def update_etf_basket_premium(self, etf_code, ticker, ticker_bid_premium_change, ticker_ask_discount_change):
        query = f"""
            UPDATE etf_inav
            SET basket_bid_premium = basket_bid_premium + 
            {ticker_bid_premium_change} * CAST(JSON_EXTRACT(weights, '$.{ticker}') AS FLOAT),
                basket_ask_discount = basket_ask_discount + 
                {ticker_ask_discount_change} * CAST(JSON_EXTRACT(weights, '$.{ticker}') AS FLOAT)
            FROM etf_components
            WHERE etf_inav.etf_code = etf_components.etf_code
              AND etf_inav.etf_code = '{etf_code}';
        """

        self._conn.execute(query)
        self._conn.commit()

    def calculate_rolls(self, best_bid: float, best_ask: float, seek: str, is_vnc1: bool):
        """
            Optimized roll calculation with DuckDB, accessing JSON using json_extract functions.
            """
        # SQL query to extract the first price from the bid and ask JSON arrays
        query = f"""
            SELECT 
                ask AS seek_best_ask,
                bid AS seek_best_bid
            FROM order_book
            WHERE type = '{seek}'
            LIMIT 1
            """

        # Execute the query and get the result
        result = self._conn.execute(query).fetchone()

        seek_best_bid, seek_best_ask = result

        # Safely check for valid best_bid, best_ask, and seek data before calculations
        if is_vnc1:
            if best_bid > 0:
                roll_1 = (seek_best_ask / best_bid - 1) * 100 if seek_best_ask else 999
            else:
                roll_1 = 999
            if best_ask > 0:
                roll_2 = (seek_best_bid / best_ask - 1) * 100 if seek_best_bid else 999
            else:
                roll_2 = 999
        else:
            roll_1 = (best_ask / seek_best_bid - 1) * 100 if seek_best_bid else 999
            roll_2 = (best_bid / seek_best_ask - 1) * 100 if seek_best_ask else 999

        return f"{roll_1:.2f}%", f"{roll_2:.2f}%"

    async def calculate_basis(self, vn30_value):
        query = """
                WITH bids AS (
                    SELECT 
                        ob.ticker, 
                        ob.type, 
                        ob.bid,
                        f.time_to_maturity
                    FROM 
                        order_book ob
                    JOIN 
                        futures f ON ob.ticker = f.ticker
                    WHERE 
                        ob.type IN ('C1', 'C2')
                )
                SELECT 
                    MAX(CASE WHEN type = 'C1' THEN bid END) / ? - 1 AS basis_1,
                    MAX(CASE WHEN type = 'C2' THEN bid END) / ? - 1 AS basis_2,
                    MAX(CASE WHEN type = 'C1' THEN (bid / ? - 1) * 365 / (time_to_maturity * 1.23) END) AS effective_rate_1,
                    MAX(CASE WHEN type = 'C2' THEN (bid / ? - 1) * 365 / (time_to_maturity * 1.23) END) AS effective_rate_2
                FROM 
                    bids;
            """

        # Execute the query
        result = self._conn.execute(query, (vn30_value, vn30_value, vn30_value, vn30_value)).fetchall()

        # Extract the results
        basis_1 = result[0][0] * 100
        basis_2 = result[0][1] * 100
        effective_rate_1 = result[0][2] * 100
        effective_rate_2 = result[0][3] * 100

        return f"{basis_1:.2f}%", f"{basis_2:.2f}%", f"{effective_rate_1:.2f}%", f"{effective_rate_2:.2f}%"

    def calculate_basket_premiums(self, etf_code):
        # Define the query to calculate the basket premiums
        query = f"""
         WITH etf_weights AS (
             SELECT
                 etf_code,
                 weights
             FROM etf_components
             WHERE etf_code = '{etf_code}'
         ),
         combined AS (
             SELECT
                 ob.ticker,
                 ob.bid_premium,
                 ob.ask_discount,
                 json_extract(ew.weights, '$.' || ob.ticker) AS weight
             FROM order_book ob
             JOIN etf_weights ew ON TRUE
         )
         SELECT
             SUM(bid_premium * CAST(weight AS FLOAT)) * 100 AS basket_bid_premium,
             SUM(ask_discount * CAST(weight AS FLOAT)) * 100 AS basket_ask_premium
         FROM combined
         """

        # Execute the query and fetch the result
        result = self._conn.execute(query).fetchall()[0]

        return result[0], result[1]

    def calculate_etf_premiums(self, etf_code, best_bid, best_ask):
        query = f"""
            SELECT iNav
            FROM etf_inav
            WHERE etf_code = '{etf_code}'
        """
        inav = self._conn.execute(query).fetchone()[0]
        if inav == 0:
            return None, None
        else:
            etf_bid_premium = (best_bid / inav - 1) * 100
            etf_ask_discount = (best_ask / inav - 1) * 100
            return f"{etf_bid_premium:.2f}%", f"{etf_ask_discount:.2f}%"

    async def calculate_vn30_price_change(self):
        query = """
        SELECT 
            v.ticker,
            (o.bid / v.last_price - 1) * CAST(JSON_EXTRACT(e.weights, '$.' || v.ticker) AS FLOAT) AS price_change
        FROM 
            vn30_last_price v
        JOIN 
            order_book o ON v.ticker = o.ticker
        JOIN 
            etf_components e ON e.etf_code = 'E1VFVN30'
        """

        result = self._conn.execute(query).fetchall()

        price_change_dict = {row[0]: row[1] for row in result}
        await asyncio.gather(*[self._update_last_price(ticker) for ticker in price_change_dict.keys()])
        return price_change_dict

    async def _update_last_price(self, ticker):
        self._conn.execute(f"""
        UPDATE vn30_last_price 
        SET last_price = (SELECT bid FROM order_book WHERE ticker = '{ticker}')
        WHERE ticker = '{ticker}'
        """)

        self._conn.commit()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    database_manager = BaseDatabaseManager()
    loop.run_until_complete(database_manager.refresh_database())
    # # database_manager.calculate_basket_premiums('E1VFVN30')
    a = loop.run_until_complete(database_manager.calculate_basis(1300))
    print(a)
    # database_manager.print_order_book('E1VFVN30')
    # loop.run_until_complete(database_manager.calculate_vn30_price_change())
    # # loop.run_until_complete(database_manager._create_vn30_last_price())
    # # loop.run_until_complete(database_manager.calculate_etf_pre
    # # miums('FUEVFVND', 1, 2))
    # # loop.run_until_complete(database_manager.calculate_basis(1300))
    #
    # # loop.run_until_complete(database_manager.update_order_book('ACB', {'bid_premium': 0.1, 'ask_discount': 0.1}))
    # loop.run_until_complete(database_manager.print_order_book('E1VFVN30'))
