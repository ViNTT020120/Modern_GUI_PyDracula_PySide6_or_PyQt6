from enum import Enum


class ColTable(Enum):
    # ETF
    etf = 0
    inav_basket = 1
    best_bid = 2
    etf_bid_premium = 3
    etf_bid_volume = 4
    best_ask = 5
    etf_ask_premium = 6
    etf_ask_volume = 7
    basket_bid_premium = 8
    basket_ask_premium = 9
    realtime_hedge_ratio = 10
    nfol_hedge_ratio = 11
    fol_hedge_ratio = 12
    per_1_f_contract = 13

    # Future
    future = 0
    time_to_m = 1
    bid_price = 2
    bid_volume = 3
    ask_price = 4
    ask_volume = 5
    basis = 6
    effective_rate = 7
    roll = 8

    # Index
    vn30_index = 0
    vnfl_index = 1


class RowTable(Enum):
    # ETF
    E1VFVN30 = 0
    FUEVFVND = 1
    FUESSVFL = 2
    FUESSV30 = 3
    FUESSV50 = 4
    FUEVN100 = 5
    FUEKIV30 = 6
    FUEDCMID = 7
    FUEKIVFS = 8
    FUEMAVND = 9
    FUEKIVND = 10
    FUEMAV30 = 11

    # Future
    VNC1 = 0
    VNC2 = 1
