import asyncio
import aiohttp


class KISSocketManager:
    def __init__(self, data_manager, dashboard_etf_signal, dashboard_futures_signal):
        self._dashboard_etf_signal = dashboard_etf_signal
        self._dashboard_futures_signal = dashboard_futures_signal
        self._ws = None
        self._vn30 = 1000
        self._data_manager = data_manager
        self._session = None  # Added to manage the session

    async def connect(self):
        # get registered tickers
        tickers = await asyncio.gather(self._data_manager.get_registered_tickers(),
                                       self._data_manager.get_registered_etf_tickers())

        aggregate_order_book_message = {
            "subscribe": "true",
            "msgTypes": ["AGGREGATE_ORDER_BOOK", "TRADE"],
            "symbols": [
                {"exchangeID": ticker[1], "key": ticker[0]} for ticker in tickers[0]
            ]
        }

        etf_statistics_message = {
            "subscribe": "true",
            "msgTypes": ["ETF_STATISTICS"],
            "symbols": [
                {"exchangeID": "HM", "key": etf_ticker[0]} for etf_ticker in tickers[1]
            ]
        }

        async with aiohttp.ClientSession() as session:
            self._session = session  # Store the session for later cleanup
            async with session.ws_connect('ws://172.25.11.17:9996/pubsub') as websocket:
                self._ws = websocket
                print(self._ws)
                # connect to server
                tasks = [
                    self._ws.send_json(aggregate_order_book_message),
                    self._ws.send_json(etf_statistics_message),
                    self._ws.send_json({
                        "subscribe": "true",
                        "msgTypes": ["INDEX_DATA"],
                        "symbols": [{"exchangeID": "HM", "key": "VN30"}]
                    })
                ]
                await asyncio.gather(*tasks)

                # receive messages from the server
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:

                        data = msg.json()
                        msgType = data['msgType']
                        if msgType in ['AGGREGATE_ORDER_BOOK,BEST_BID_ASK', 'AGGREGATE_ORDER_BOOK']:
                            asyncio.create_task(self._on_best_bid_ask_event(data))
                        elif msgType == 'TRADE':
                            asyncio.create_task(self._on_trade_event(data))
                        elif msgType == 'INDEX_DATA':
                            asyncio.create_task(self._on_index_data_event(data, tickers[1]))
                        elif msgType == 'ETF_STATISTICS':
                            asyncio.create_task(self._on_etf_statistics_event(data))
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f'Error: {msg.data}')
                        break

    async def close(self):
        if self._session is not None:
            # apparently this is supposed to return a future?
            await self._session.close()
            self._data_manager.close_db()

    async def _on_best_bid_ask_event(self, data):
        ticker = data['code']
        bid = data['bids']
        ask = data['asks']

        best_bid = float(bid[0]['price']) if bid else 0
        best_ask = float(ask[0]['price']) if ask else 0

        security_type, last_trade, etf_codes = \
            await self._data_manager.get_ticker_info_from_order_book(ticker, ['type', 'trade', 'etf_basket'])

        if security_type == 'S':
            new_bid_premium = best_bid / last_trade - 1
            new_ask_discount = best_ask / last_trade - 1

            # self._dashboard_etf_signal.emit(f'ladder event: {ticker}, {best_bid}, {best_ask}, {last_trade}')

            await self._data_manager.update_order_book(ticker,
                                                       {'bid': best_bid, 'ask': best_ask,
                                                        'bid_premium': new_bid_premium,
                                                        'ask_discount': new_ask_discount})

        elif security_type == 'C1':
            # print(f'ladder event: {ticker}, {best_bid}, {best_ask}')
            await self._data_manager.update_order_book(ticker, {'bid': best_bid, 'ask': best_ask})

            time_to_maturity = await self._data_manager.get_ticker_info_from_futures(ticker, ['time_to_maturity'])
            # calculate roll_1 and roll_2
            roll_1, roll_2 = self._data_manager.calculate_rolls(best_bid, best_ask, seek='C2', is_vnc1=True)
            basis = (best_bid / self._vn30 - 1) * 100
            effective_rate = (basis * 365) / (time_to_maturity[0] * 1.23)
            self._dashboard_futures_signal.emit({'ticker': 'VNC1', 'roll_1': roll_1, 'roll_2': roll_2,
                                                 'basis': f"{basis:.2f}%",
                                                 'effective_rate': f"{effective_rate:.2f}%",
                                                 'best_bid': str(best_bid), 'best_ask': str(best_ask),
                                                 'bid_volume': str(bid[0]['qty']), 'ask_volume': str(ask[0]['qty'])})

        elif security_type == 'C2':
            # print(f'ladder event: {ticker}, {best_bid}, {best_ask}')
            await self._data_manager.update_order_book(ticker, {'bid': best_bid, 'ask': best_ask})

            time_to_maturity = await self._data_manager.get_ticker_info_from_futures(ticker, ['time_to_maturity'])
            # calculate roll_1 and roll_2
            roll_1, roll_2 = self._data_manager.calculate_rolls(best_bid, best_ask, seek='C1', is_vnc1=False)
            basis = (best_bid / self._vn30 - 1) * 100
            effective_rate = (basis * 365) / (time_to_maturity[0] * 1.23)
            self._dashboard_futures_signal.emit({'ticker': 'VNC2', 'roll_1': roll_1, 'roll_2': roll_2,
                                                 'basis': f"{basis:.2f}%",
                                                 'effective_rate': f"{effective_rate:.2f}%",
                                                 'best_bid': str(best_bid), 'best_ask': str(best_ask),
                                                 'bid_volume': str(bid[0]['qty']), 'ask_volume': str(ask[0]['qty'])})
        # ETF
        else:
            await self._data_manager.update_order_book(ticker, {'bid': best_bid, 'ask': best_ask})

            # calculate ETF_premiums
            etf_bid_premium, etf_ask_discount = self._data_manager.calculate_etf_premiums(ticker, best_bid, best_ask)
            if etf_bid_premium and etf_ask_discount:
                self._dashboard_etf_signal.emit({'ticker': ticker, 'etf_bid_premium': etf_bid_premium,
                                                 'etf_ask_premium': etf_ask_discount,
                                                 'etf_bid_volume': str(bid[0]['qty']),
                                                 'etf_ask_volume': str(ask[0]['qty']), 'best_bid': str(best_bid),
                                                 'best_ask': str(best_ask)})

    async def _update_etf_on_tick_event(self, etf_code):
        basket_bid_premium, basket_ask_discount = self._data_manager.calculate_basket_premiums(etf_code)

        self._dashboard_etf_signal.emit({'ticker': etf_code, 'basket_bid_premium': f"{basket_bid_premium:.2f}%",
                                         'basket_ask_premium': f"{basket_ask_discount:.2f}%"})

    async def _on_trade_event(self, data):
        ticker = data['securityCode']
        price = float(data['price'])
        security_type, last_bid, last_ask, etf_codes = await self._data_manager.get_ticker_info_from_order_book(
            ticker, ['type', 'bid', 'ask', 'etf_basket'])
        if security_type == 'S':
            new_bid_premium = (last_bid / price - 1) if price > 0 else 0
            new_ask_discount = (last_ask / price - 1) if price > 0 else 0
            await self._data_manager.update_order_book(ticker, {'trade': price,
                                                                'bid_premium': new_bid_premium,
                                                                'ask_discount': new_ask_discount})
            await asyncio.gather(*[
                self._update_etf_on_tick_event(etf_code)
                for etf_code in etf_codes
            ])

    async def _on_index_data_event(self, data, etf_tickers):
        self._vn30 = float(data['value'])
        tasks = [self._update_etf_hedge_ratio_on_index_event(etf_code[0]) for etf_code in etf_tickers]
        tasks.append(self._data_manager.calculate_basis(self._vn30))
        basis = await asyncio.gather(*tasks)
        self._dashboard_futures_signal.emit({'ticker': 'VNC1', 'basis': basis[-1][0], 'effective_rate': basis[-1][2]})
        self._dashboard_futures_signal.emit({'ticker': 'VNC2', 'basis': basis[-1][1], 'effective_rate': basis[-1][3]})

    async def _update_etf_hedge_ratio_on_index_event(self, etf_code):
        inav, fol, nfol = await self._data_manager.get_ticker_info_from_etf_inav(etf_code, ['iNav', 'fol', 'nfol'])
        hedge_ratio = inav / self._vn30
        fol_hedge_ratio = hedge_ratio * fol
        nfol_hedge_ratio = hedge_ratio * nfol
        self._dashboard_etf_signal.emit({'ticker': etf_code, 'realtime_hedge_ratio': f"{hedge_ratio: .2f}",
                                         'fol_hedge_ratio': f"{fol_hedge_ratio: .2f}",
                                         'nfol_hedge_ratio': f"{nfol_hedge_ratio: .2f}"})

    async def _on_etf_statistics_event(self, data):
        ticker = data['securityCode']
        inav = float(data['iNAV'])
        asyncio.create_task(self._data_manager.update_etf_inav(ticker, {'iNav': inav}))
        quote, fol_ratio = await asyncio.gather(
            self._data_manager.get_ticker_info_from_order_book(ticker, ['bid', 'ask']),
            self._data_manager.get_ticker_info_from_etf_inav(ticker, ['fol', 'nfol']))
        last_bid, last_ask = quote
        fol, nfol = fol_ratio

        etf_bid_premium = (last_bid / inav - 1) * 100
        etf_ask_discount = (last_ask / inav - 1) * 100
        hedge_ratio = inav / self._vn30
        fol_hedge_ratio = hedge_ratio * fol
        nfol_hedge_ratio = hedge_ratio * nfol

        self._dashboard_etf_signal.emit({'ticker': ticker, 'inav_basket': data['iNAV'],
                                         'etf_bid_premium': f"{etf_bid_premium:.2f}%",
                                         'etf_ask_premium': f"{etf_ask_discount:.2f}%",
                                         'realtime_hedge_ratio': f"{hedge_ratio: .2f}",
                                         'fol_hedge_ratio': f"{fol_hedge_ratio: .2f}",
                                         'nfol_hedge_ratio': f"{nfol_hedge_ratio: .2f}"})

    async def send_new_message(self, message: dict):
        if self._ws and not self._ws.closed:
            await self._ws.send_json(message)
