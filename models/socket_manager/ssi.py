import asyncio
import aiohttp


class SSISocketManager:
    def __init__(self, data_manager, dashboard_etf_signal, dashboard_futures_signal):
        self._dashboard_etf_signal = dashboard_etf_signal
        self._dashboard_futures_signal = dashboard_futures_signal
        self._ws = None
        self._vn30 = 1000
        self._data_manager = data_manager
        self._session = None  # Added to manage the session

    async def connect(self):
        tickers = await asyncio.gather(self._data_manager.get_registered_equity_tickers(),
                                       self._data_manager.get_registered_futures_tickers(),
                                       self._data_manager.get_registered_etf_tickers())
        tickers[2] = [row[0] for row in tickers[2]]
        equity_prices_message = {
            "type": "sub",
            "topic": "stockRealtimeByListV2",
            "variables": tickers[0],
            "component": "priceTableEquities"
        }
        futures_prices_message = {
            "type": "sub",
            "topic": "stockRealtimeByListV2",
            "variables": tickers[1],
            "component": "priceTableDerivatives"
        }
        index_massage = {
            "type": "sub",
            "topic": "notifyIndexRealtimeByListV2",
            "variables": ["VN30"]
        }

        async with aiohttp.ClientSession() as session:
            self._session = session
            async with session.ws_connect('wss://iboard-pushstream.ssi.com.vn/realtime') as websocket:
                self._ws = websocket
                tasks = [
                    self._ws.send_json(equity_prices_message),
                    self._ws.send_json(futures_prices_message),
                    self._ws.send_json(index_massage)
                ]
                await asyncio.gather(*tasks)

                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        parts = msg.data.split('|')
                        ticker = parts[0][2:]
                        if 'S#' in parts[0]:
                            # futures
                            if ticker in tickers[1]:
                                security_type = await self._data_manager.get_ticker_info_from_order_book(ticker,
                                                                                                         ['type'])
                                if 'vnf' not in parts:
                                    # Prepare bid and ask pairs
                                    bids = [(parts[i], parts[i + 1]) for i in range(1, 21, 2) if parts[i]]
                                    asks = [(parts[i], parts[i + 1]) for i in range(21, 41, 2) if parts[i]]

                                    best_bid = float(bids[0][0]) if bids else 0
                                    best_ask = float(asks[0][0]) if asks else 0
                                    bid_volume = bids[0][1] if bids else 'N/A'
                                    ask_volume = asks[0][1] if asks else 'N/A'

                                    if security_type[0] == 'C1':
                                        # print(f'ladder event: {ticker}, {best_bid}, {best_ask}')
                                        await self._data_manager.update_order_book(ticker,
                                                                                   {'bid': best_bid,
                                                                                    'ask': best_ask})

                                        time_to_maturity = await self._data_manager.get_ticker_info_from_futures(
                                            ticker, ['time_to_maturity'])
                                        # calculate roll_1 and roll_2
                                        roll_1, roll_2 = self._data_manager.calculate_rolls(best_bid, best_ask,
                                                                                            seek='C2', is_vnc1=True)
                                        basis = (best_bid / self._vn30 - 1) * 100
                                        effective_rate = (basis * 365) / (time_to_maturity[0] * 1.23)
                                        self._dashboard_futures_signal.emit(
                                            {'ticker': 'VNC1', 'roll_1': roll_1, 'roll_2': roll_2,
                                             'basis': f"{basis:.2f}%",
                                             'effective_rate': f"{effective_rate:.2f}%",
                                             'bid_price': str(best_bid), 'ask_price': str(best_ask),
                                             'bid_volume': bid_volume, 'ask_volume': ask_volume})
                                    elif security_type[0] == 'C2':
                                        # print(f'ladder event: {ticker}, {best_bid}, {best_ask}')
                                        await self._data_manager.update_order_book(ticker,
                                                                                   {'bid': best_bid,
                                                                                    'ask': best_ask})

                                        time_to_maturity = await self._data_manager.get_ticker_info_from_futures(
                                            ticker, ['time_to_maturity'])
                                        # calculate roll_1 and roll_2
                                        roll_1, roll_2 = self._data_manager.calculate_rolls(best_bid, best_ask,
                                                                                            seek='C1',
                                                                                            is_vnc1=False)
                                        basis = (best_bid / self._vn30 - 1) * 100
                                        effective_rate = (basis * 365) / (time_to_maturity[0] * 1.23)
                                        self._dashboard_futures_signal.emit(
                                            {'ticker': 'VNC2', 'roll_1': roll_1, 'roll_2': roll_2,
                                             'basis': f"{basis:.2f}%",
                                             'effective_rate': f"{effective_rate:.2f}%",
                                             'bid_price': str(best_bid), 'ask_price': str(best_ask),
                                             'bid_volume': bid_volume, 'ask_volume': ask_volume})

                            # ETF
                            elif ticker in tickers[2]:
                                if 'e' in parts:
                                    bids = [(parts[i], parts[i + 1]) for i in range(1, 6, 2) if parts[i]]
                                    asks = [(parts[i], parts[i + 1]) for i in range(21, 26, 2) if parts[i]]
                                    best_bid = float(bids[0][0]) if bids else 0
                                    best_ask = float(asks[0][0]) if asks else 0
                                    bid_volume = bids[0][1] if bids else 0
                                    ask_volume = asks[0][1] if asks else 0
                                    await self._data_manager.update_order_book(ticker,
                                                                               {'bid': float(best_bid),
                                                                                'ask': float(best_ask)})

                                    # calculate ETF_premiums
                                    etf_bid_premium, etf_ask_discount = self._data_manager.calculate_etf_premiums(
                                        ticker, best_bid, best_ask)
                                    if etf_bid_premium and etf_ask_discount:
                                        self._dashboard_etf_signal.emit(
                                            {'ticker': ticker, 'etf_bid_premium': etf_bid_premium,
                                             'etf_ask_premium': etf_ask_discount,
                                             'etf_bid_volume': bid_volume,
                                             'etf_ask_volume': ask_volume, 'best_bid': str(best_bid),
                                             'best_ask': str(best_ask)})
                                else:
                                    asyncio.create_task(self._on_etf_statistics_event(ticker, parts))

                            # HCM stocks
                            elif 'hose' in parts:
                                last_trade, etf_codes = await self._data_manager.get_ticker_info_from_order_book(
                                    ticker, ['trade', 'etf_basket'])

                                bids = [(parts[i], parts[i + 1]) for i in range(1, 6, 2) if parts[i]]
                                asks = [(parts[i], parts[i + 1]) for i in range(21, 26, 2) if parts[i]]
                                trade = float(parts[41]) if float(parts[41]) > 0 else last_trade
                                best_bid = float(bids[0][0]) if bids else 0
                                best_ask = float(asks[0][0]) if asks else 0

                                new_bid_premium = best_bid / trade - 1
                                new_ask_discount = best_ask / trade - 1

                                # self._dashboard_etf_signal.emit(f'ladder event: {ticker}, {best_bid}, {best_ask}, {last_trade}')

                                await self._data_manager.update_order_book(ticker,
                                                                           {'bid': best_bid, 'trade': trade,
                                                                            'ask': best_ask,
                                                                            'bid_premium': new_bid_premium,
                                                                            'ask_discount': new_ask_discount})
                                await asyncio.gather(*[
                                    self._update_etf_on_tick_event(etf_code)
                                    for etf_code in etf_codes
                                ])

                            # HNX stocks
                            # else:
                            #     # trade event
                            #     if 'hnx' in parts:
                            #         price = float(parts[41])
                            #         last_bid, last_ask, etf_codes = await self._data_manager.get_ticker_info_from_order_book(
                            #             ticker, ['bid', 'ask', 'etf_basket'])
                            #         new_bid_premium = (last_bid / price - 1) if price > 0 else 0
                            #         new_ask_discount = (last_ask / price - 1) if price > 0 else 0
                            #         await self._data_manager.update_order_book(ticker, {'trade': price,
                            #                                                             'bid_premium': new_bid_premium,
                            #                                                             'ask_discount': new_ask_discount})
                            #         await asyncio.gather(*[
                            #             self._update_etf_on_tick_event(etf_code)
                            #             for etf_code in etf_codes
                            #         ])
                            #
                            #     # quote event
                            #     else:
                            #         bids = [(parts[i], parts[i + 1]) for i in range(1, 21, 2) if
                            #                 parts[i]]  # 10 levels of bids
                            #         asks = [(parts[i], parts[i + 1]) for i in range(21, 41, 2) if
                            #                 parts[i]]  # 10 levels of asks
                            #         best_bid = float(bids[0][0]) if bids else 0
                            #         best_ask = float(asks[0][0]) if asks else 0
                            #         last_trade, etf_codes = \
                            #             await self._data_manager.get_ticker_info_from_order_book(
                            #                 ticker, ['trade', 'etf_basket'])
                            #         new_bid_premium = best_bid / last_trade - 1
                            #         new_ask_discount = best_ask / last_trade - 1
                            #         await self._data_manager.update_order_book(ticker,
                            #                                                    {'bid': best_bid, 'ask': best_ask,
                            #                                                     'bid_premium': new_bid_premium,
                            #                                                     'ask_discount': new_ask_discount})
                            #         await asyncio.gather(*[
                            #             self._update_etf_on_tick_event(etf_code)
                            #             for etf_code in etf_codes
                            #         ])



                        # Index
                        elif 'I#' in parts[0]:
                            asyncio.create_task(self._on_index_data_event(parts, tickers[2]))

    async def _update_etf_on_tick_event(self, etf_code):
        basket_bid_premium, basket_ask_discount = self._data_manager.calculate_basket_premiums(etf_code)

        self._dashboard_etf_signal.emit({'ticker': etf_code, 'basket_bid_premium': f"{basket_bid_premium:.2f}%",
                                         'basket_ask_premium': f"{basket_ask_discount:.2f}%"})

    async def _on_etf_statistics_event(self, ticker, data):
        inav = data[77]
        if inav:
            _inav = float(inav)
            asyncio.create_task(self._data_manager.update_etf_inav(ticker, {'iNav': _inav}))
            quote, fol_ratio = await asyncio.gather(
                self._data_manager.get_ticker_info_from_order_book(ticker, ['bid', 'ask']),
                self._data_manager.get_ticker_info_from_etf_inav(ticker, ['fol', 'nfol']))
            last_bid, last_ask = quote
            fol, nfol = fol_ratio

            etf_bid_premium = (last_bid / _inav - 1) * 100
            etf_ask_discount = (last_ask / _inav - 1) * 100
            hedge_ratio = _inav / self._vn30
            fol_hedge_ratio = hedge_ratio * fol
            nfol_hedge_ratio = hedge_ratio * nfol

            self._dashboard_etf_signal.emit({'ticker': ticker, 'inav_basket': inav,
                                             'etf_bid_premium': f"{etf_bid_premium:.2f}%",
                                             'etf_ask_premium': f"{etf_ask_discount:.2f}%",
                                             'realtime_hedge_ratio': f"{hedge_ratio: .2f}",
                                             'fol_hedge_ratio': f"{fol_hedge_ratio: .2f}",
                                             'nfol_hedge_ratio': f"{nfol_hedge_ratio: .2f}"})

    async def _on_index_data_event(self, data, etf_tickers):
        if data[1]:
            self._vn30 = float(data[1])
            tasks = [self._update_etf_hedge_ratio_on_index_event(etf_code) for etf_code in etf_tickers]
            tasks.append(self._data_manager.calculate_basis(self._vn30))
            basis = await asyncio.gather(*tasks)
            self._dashboard_futures_signal.emit(
                {'ticker': 'VNC1', 'basis': basis[-1][0], 'effective_rate': basis[-1][2]})
            self._dashboard_futures_signal.emit(
                {'ticker': 'VNC2', 'basis': basis[-1][1], 'effective_rate': basis[-1][3]})

    async def _update_etf_hedge_ratio_on_index_event(self, etf_code):
        inav, fol, nfol = await self._data_manager.get_ticker_info_from_etf_inav(etf_code, ['iNav', 'fol', 'nfol'])
        hedge_ratio = inav / self._vn30
        fol_hedge_ratio = hedge_ratio * fol
        nfol_hedge_ratio = hedge_ratio * nfol
        self._dashboard_etf_signal.emit({'ticker': etf_code, 'realtime_hedge_ratio': f"{hedge_ratio: .2f}",
                                         'fol_hedge_ratio': f"{fol_hedge_ratio: .2f}",
                                         'nfol_hedge_ratio': f"{nfol_hedge_ratio: .2f}"})


if __name__ == "__main__":
    from models.database_manager.base import BaseDatabaseManager

    loop = asyncio.get_event_loop()
    database_manager = BaseDatabaseManager()
    socket_manager = SSISocketManager(database_manager)
    loop.run_until_complete(socket_manager.connect())
    loop.close()
