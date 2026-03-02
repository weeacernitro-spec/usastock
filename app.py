"""
TCSM US STOCK PRO v5.0 — EARLY TREND DETECTION ENGINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Derived from: KINGZ UltraFusion Pro v3 + TCSMPro v1.0
Adapted for: US Markets (NYSE / NASDAQ)
Key Strategy: Catch trends at inception via:
  1. CHoCH (Change of Character) — earliest structural reversal
  2. BOS (Break of Structure) — trend continuation
  3. Order Blocks — institutional entry zones
  4. Fair Value Gaps — price imbalances
  5. Premium/Discount — optimal entry zones
  6. Triple Confluence Engine — 3 oscillator agreement
  7. SmartMomentum Composite — 7 weighted indicators
  8. Divergence Detection — leading reversal signals
  9. Volume Profile — institutional participation
  10. Multi-Timeframe — HTF alignment

DEPLOY on Render:
  Build: pip install -r requirements.txt
  Start: gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1 -b 0.0.0.0:$PORT app:app

requirements.txt:
  flask, flask-socketio, gunicorn, gevent, gevent-websocket,
  pandas, numpy, yfinance, requests
"""
from gevent import monkey
monkey.patch_all()

import gevent
from flask import Flask, render_template_string, request, redirect, url_for, session, flash, jsonify
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from functools import wraps
import pandas as pd
import numpy as np
import hashlib, secrets, time, json, os, traceback, logging, math, threading
import requests as req_lib

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('TCSM-US-v5')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent',
    ping_timeout=120, ping_interval=25, logger=False, engineio_logger=False, always_connect=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
ET = timezone(timedelta(hours=-5))  # US Eastern (adjust to -4 for EDT if needed)
def now(): return datetime.now(ET)
def ts(f="%H:%M:%S"): return now().strftime(f)
def dts(): return now().strftime("%Y-%m-%d %H:%M:%S")

def sanitize(obj):
    if obj is None: return None
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return 0.0 if (math.isnan(v) or math.isinf(v)) else round(v, 6)
    if isinstance(obj, (np.bool_,)): return bool(obj)
    if isinstance(obj, np.ndarray): return [sanitize(x) for x in obj.tolist()]
    if isinstance(obj, pd.Timestamp): return str(obj)
    if isinstance(obj, dict): return {str(k): sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [sanitize(i) for i in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return 0.0
    return obj

def safe_emit(event, data, **kwargs):
    try: socketio.emit(event, sanitize(data), **kwargs)
    except: pass

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG — US STOCKS
# ═══════════════════════════════════════════════════════════════════════════════
class C:
    SCAN_INTERVAL = 90
    BATCH_SIZE = 5
    DELIST_THRESHOLD = 3

    # Triple Confluence Engine A
    STOCH_RSI_K=3; STOCH_RSI_D=3; RSI_PD=14; STOCH_PD=14
    ENH_K=21; ENH_D=3; ENH_SLOW=5
    MACD_F=12; MACD_S=26; MACD_SIG=9; MACD_NLB=500
    OB_LVL=80.0; OS_LVL=20.0; NEU_HI=60.0; NEU_LO=40.0

    # SmartMomentum Engine B
    SIG_TH=38; CHG_TH=14; MOM_SM=3
    RSI_W=18; MACD_W=18; STOCH_W=14; CCI_W=14; WPR_W=12; ADX_W=12; MOM_W=12
    CCI_PD=20; WPR_PD=14; ADX_PD=14; MOM_PD=14

    # Market Structure
    MS_PIVOT_LEN = 3
    MS_BODY_BREAK = True
    SWING_STR = 5

    # Order Block
    OB_LOOKBACK = 20
    OB_DISP_MULT = 1.3

    # FVG
    FVG_MIN_ATR = 0.15

    # Supply/Demand
    SD_LEN = 10
    SD_WIDTH_ATR = 2.5

    # Risk Management
    ATR_PD=14; ATR_SL=1.5; RR=2.0
    DIV_BARS=20; VOL_PD=20

    # Early Trend Detection Thresholds
    EARLY_TREND_MIN_STR = 45.0
    STRONG_STR = 65.0
    TREND_MA = 50

    # ═══════════════════════════════════════════════
    #  US STOCK UNIVERSE (~120 tickers)
    # ═══════════════════════════════════════════════
    STOCKS = [
        # Mega Cap Tech
        "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","AVGO","ORCL","CRM",
        "ADBE","AMD","INTC","QCOM","TXN","NFLX","CSCO","IBM","NOW","UBER",
        "SHOP","SQ","SNOW","PLTR","MRVL","MU","ANET","PANW","CRWD","DDOG",
        # Financials
        "JPM","BAC","WFC","GS","MS","C","BLK","SCHW","AXP","USB",
        "PNC","TFC","COF","FIS","MCO",
        # Healthcare
        "UNH","JNJ","LLY","PFE","ABBV","MRK","TMO","ABT","DHR","BMY",
        "AMGN","GILD","ISRG","VRTX","MDT",
        # Energy
        "XOM","CVX","COP","SLB","EOG","PXD","MPC","VLO","PSX","OXY",
        "HAL","DVN","FANG","HES","KMI",
        # Consumer
        "WMT","COST","PG","KO","PEP","MCD","NKE","SBUX","TGT","LOW",
        "HD","TJX","EL","CL","MNST",
        # Industrials
        "CAT","DE","UNP","BA","HON","GE","RTX","LMT","MMM","UPS",
        "FDX","EMR","ETN","ITW","NSC",
        # Communication
        "DIS","CMCSA","T","VZ","TMUS","CHTR","EA","TTWO","WBD","PARA",
        # REITs & Utilities
        "AMT","PLD","CCI","EQIX","PSA","NEE","DUK","SO","D","AEP",
        # Materials & Others
        "LIN","APD","FCX","NEM","DOW","PPG","ECL","BHP","RIO","VALE",
        # Popular / Meme / Growth
        "COIN","HOOD","RIVN","LCID","SOFI","RBLX","U","PATH","ABNB","DASH",
    ]
    STOCKS = list(dict.fromkeys(STOCKS))

    SECTOR_MAP = {}
    _s = {
        "Tech": ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","AVGO","ORCL","CRM",
                 "ADBE","AMD","INTC","QCOM","TXN","NFLX","CSCO","IBM","NOW","UBER",
                 "SHOP","SQ","SNOW","PLTR","MRVL","MU","ANET","PANW","CRWD","DDOG"],
        "Finance": ["JPM","BAC","WFC","GS","MS","C","BLK","SCHW","AXP","USB",
                     "PNC","TFC","COF","FIS","MCO","COIN","HOOD","SOFI"],
        "Health": ["UNH","JNJ","LLY","PFE","ABBV","MRK","TMO","ABT","DHR","BMY",
                    "AMGN","GILD","ISRG","VRTX","MDT"],
        "Energy": ["XOM","CVX","COP","SLB","EOG","PXD","MPC","VLO","PSX","OXY",
                    "HAL","DVN","FANG","HES","KMI"],
        "Consumer": ["WMT","COST","PG","KO","PEP","MCD","NKE","SBUX","TGT","LOW",
                      "HD","TJX","EL","CL","MNST"],
        "Industrial": ["CAT","DE","UNP","BA","HON","GE","RTX","LMT","MMM","UPS",
                        "FDX","EMR","ETN","ITW","NSC"],
        "Comms": ["DIS","CMCSA","T","VZ","TMUS","CHTR","EA","TTWO","WBD","PARA"],
        "REIT/Util": ["AMT","PLD","CCI","EQIX","PSA","NEE","DUK","SO","D","AEP"],
        "Materials": ["LIN","APD","FCX","NEM","DOW","PPG","ECL","BHP","RIO","VALE"],
        "Growth": ["RIVN","LCID","RBLX","U","PATH","ABNB","DASH"],
    }
    for sect, syms in _s.items():
        for s in syms: SECTOR_MAP[s] = sect

    @staticmethod
    def get_sector(sym): return C.SECTOR_MAP.get(sym, "Other")
    @staticmethod
    def yf_sym(sym): return sym  # US stocks: no suffix needed


# ═══════════════════════════════════════════════════════════════════════════════
#  YAHOO FINANCE CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
class YFClient:
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def __init__(self):
        self._cache = {}; self._cache_time = {}; self._lock = threading.Lock()
        self._cookie = None; self._crumb = None; self._crumb_time = 0
        self._session = None; self._fail_count = {}; self._dead_tickers = set()

    def _get_session(self):
        if not self._session:
            self._session = req_lib.Session()
            self._session.headers.update(self.HEADERS)
        return self._session

    def _get_cookie_crumb(self):
        if self._crumb and time.time() - self._crumb_time < 300:
            return self._cookie, self._crumb
        try:
            s = self._get_session()
            s.get('https://fc.yahoo.com', timeout=10, allow_redirects=True)
            r = s.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=10)
            if r.status_code == 200 and r.text and 'Too Many' not in r.text:
                self._crumb = r.text.strip(); self._cookie = s.cookies; self._crumb_time = time.time()
                return self._cookie, self._crumb
        except: pass
        return None, None

    def is_dead(self, symbol): return symbol in self._dead_tickers

    def _mark_dead(self, symbol, reason=""):
        fc = self._fail_count.get(symbol, 0) + 1
        self._fail_count[symbol] = fc
        if fc >= C.DELIST_THRESHOLD:
            self._dead_tickers.add(symbol)
            logger.warning(f"☠ {symbol} dead ({reason})")

    def _fetch_direct(self, symbol, period="3mo", interval="1h"):
        yf_sym = C.yf_sym(symbol)
        try:
            s = self._get_session(); cookie, crumb = self._get_cookie_crumb()
            params = {'range': period, 'interval': interval, 'includePrePost': 'false'}
            if crumb: params['crumb'] = crumb
            r = s.get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_sym}",
                      params=params, timeout=15, cookies=cookie)
            if r.status_code == 200:
                chart = r.json().get('chart', {}).get('result', [])
                if not chart: return None
                res = chart[0]; ts_list = res.get('timestamp', []); q = res.get('indicators', {}).get('quote', [{}])[0]
                if ts_list and q:
                    df = pd.DataFrame({'open': q.get('open'), 'high': q.get('high'), 'low': q.get('low'),
                                       'close': q.get('close'), 'volume': q.get('volume')},
                                      index=pd.to_datetime(ts_list, unit='s'))
                    df = df.dropna(subset=['close']); df = df[df['close'] > 0]
                    if len(df) >= 30: return df
            elif r.status_code == 404: self._mark_dead(symbol, "404")
            elif r.status_code == 429: gevent.sleep(8)
        except: pass
        return None

    def _fetch_yfinance(self, symbol, period="3mo", interval="1h"):
        try:
            import yfinance as yf
            df = yf.Ticker(C.yf_sym(symbol)).history(period=period, interval=interval)
            if df is not None and len(df) >= 30:
                df.columns = [c.lower() for c in df.columns]
                for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
                df = df.dropna(subset=['close']); df = df[df['close'] > 0]
                if len(df) >= 30: return df
        except Exception as e:
            if 'delisted' in str(e).lower(): self._mark_dead(symbol, str(e)[:60])
        return None

    def get_history(self, symbol, period="3mo", interval="1h"):
        if symbol in self._dead_tickers: return None
        ck = f"{symbol}_{period}_{interval}"
        with self._lock:
            if ck in self._cache and time.time() - self._cache_time.get(ck, 0) < 55:
                return self._cache[ck]
        for name, fn in [("direct", self._fetch_direct), ("yfinance", self._fetch_yfinance)]:
            if symbol in self._dead_tickers: return None
            try:
                df = fn(symbol, period, interval)
                if df is not None and len(df) >= 30:
                    with self._lock: self._cache[ck] = df; self._cache_time[ck] = time.time()
                    self._fail_count[symbol] = 0
                    return df
            except: pass
            gevent.sleep(0.5)
        self._mark_dead(symbol, "all methods failed")
        return None

    def get_daily(self, symbol):
        if symbol in self._dead_tickers: return None
        return self.get_history(symbol, period="6mo", interval="1d")

yfc = YFClient()


# ═══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS (CORE)
# ═══════════════════════════════════════════════════════════════════════════════
def sma(s, n): return s.rolling(n, min_periods=1).mean()
def ema(s, n): return s.ewm(span=n, adjust=False, min_periods=1).mean()
def rma(s, n): return s.ewm(alpha=1.0/n, adjust=False, min_periods=1).mean()

def calc_rsi(close, period=14):
    d = close.diff()
    g = d.where(d > 0, 0.0).ewm(alpha=1.0/period, adjust=False, min_periods=1).mean()
    l = (-d.where(d < 0, 0.0)).ewm(alpha=1.0/period, adjust=False, min_periods=1).mean()
    return 100.0 - 100.0 / (1.0 + g / (l + 1e-10))

def calc_stoch(close, high, low, period):
    ll = low.rolling(period, min_periods=1).min()
    hh = high.rolling(period, min_periods=1).max()
    return ((close - ll) / (hh - ll + 1e-10)) * 100.0

def calc_cci(close, high, low, period):
    tp = (high + low + close) / 3.0; ma = tp.rolling(period, min_periods=1).mean()
    md = tp.rolling(period, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    return (tp - ma) / (0.015 * md + 1e-10)

def calc_wpr(close, high, low, period):
    hh = high.rolling(period, min_periods=1).max(); ll = low.rolling(period, min_periods=1).min()
    return ((hh - close) / (hh - ll + 1e-10)) * -100.0

def calc_atr(high, low, close, period):
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def calc_adx(high, low, close, period):
    up = high.diff(); dn = -low.diff()
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=high.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=high.index)
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    atr_s = rma(tr, period)
    pdi = 100.0 * rma(pdm, period) / (atr_s + 1e-10)
    mdi = 100.0 * rma(mdm, period) / (atr_s + 1e-10)
    dx = 100.0 * (pdi - mdi).abs() / (pdi + mdi + 1e-10)
    return rma(dx, period), pdi, mdi

def calc_obv(close, volume):
    return (volume * np.sign(close.diff())).cumsum()


# ═══════════════════════════════════════════════════════════════════════════════
#  MARKET STRUCTURE ENGINE (CHoCH / BOS)
# ═══════════════════════════════════════════════════════════════════════════════
def detect_market_structure(df, pivot_len=3, use_body=True):
    close = df['close'].values; high = df['high'].values
    low = df['low'].values; opn = df['open'].values
    n = len(close)
    if n < pivot_len * 3: return None

    swing_highs = []; swing_lows = []
    for i in range(pivot_len, n - pivot_len):
        is_sh = all(high[i] > high[i - j] and high[i] > high[i + j] for j in range(1, pivot_len + 1))
        is_sl = all(low[i] < low[i - j] and low[i] < low[i + j] for j in range(1, pivot_len + 1))
        if is_sh: swing_highs.append((i, high[i]))
        if is_sl: swing_lows.append((i, low[i]))

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {'trend': 0, 'events': [], 'last_sh': None, 'last_sl': None,
                'is_hh': False, 'is_hl': False, 'is_lh': False, 'is_ll': False,
                'choch_bull': False, 'choch_bear': False, 'bos_bull': False, 'bos_bear': False}

    is_hh = swing_highs[-1][1] > swing_highs[-2][1]
    is_lh = swing_highs[-1][1] < swing_highs[-2][1]
    is_hl = swing_lows[-1][1] > swing_lows[-2][1]
    is_ll = swing_lows[-1][1] < swing_lows[-2][1]
    trend = 1 if (is_hh and is_hl) else (-1 if (is_lh and is_ll) else 0)

    events = []
    last_sh = swing_highs[-1] if swing_highs else None
    last_sl = swing_lows[-1] if swing_lows else None
    choch_bull = False; choch_bear = False
    bos_bull = False; bos_bear = False

    if last_sh and last_sl:
        recent_close = close[-1]
        bu = recent_close if use_body else high[-1]
        bd = recent_close if use_body else low[-1]
        sh_val = last_sh[1]; sl_val = last_sl[1]
        if bu > sh_val:
            if trend == -1 or is_lh:
                choch_bull = True
                events.append({'type': 'CHoCH', 'dir': 'BULL', 'level': sh_val, 'bar': last_sh[0]})
            else:
                bos_bull = True
                events.append({'type': 'BOS', 'dir': 'BULL', 'level': sh_val, 'bar': last_sh[0]})
        if bd < sl_val:
            if trend == 1 or is_hl:
                choch_bear = True
                events.append({'type': 'CHoCH', 'dir': 'BEAR', 'level': sl_val, 'bar': last_sl[0]})
            else:
                bos_bear = True
                events.append({'type': 'BOS', 'dir': 'BEAR', 'level': sl_val, 'bar': last_sl[0]})

    recent_choch_bull = False; recent_choch_bear = False
    recent_bos_bull = False; recent_bos_bear = False
    for i in range(max(0, n - 5), n):
        bu_i = close[i] if use_body else high[i]
        bd_i = close[i] if use_body else low[i]
        if last_sh and bu_i > last_sh[1]:
            if trend <= 0: recent_choch_bull = True
            else: recent_bos_bull = True
        if last_sl and bd_i < last_sl[1]:
            if trend >= 0: recent_choch_bear = True
            else: recent_bos_bear = True

    return {
        'trend': trend, 'events': events[-5:],
        'last_sh': last_sh, 'last_sl': last_sl,
        'is_hh': bool(is_hh), 'is_hl': bool(is_hl),
        'is_lh': bool(is_lh), 'is_ll': bool(is_ll),
        'choch_bull': bool(recent_choch_bull or choch_bull),
        'choch_bear': bool(recent_choch_bear or choch_bear),
        'bos_bull': bool(recent_bos_bull or bos_bull),
        'bos_bear': bool(recent_bos_bear or bos_bear),
        'swing_highs': swing_highs[-5:], 'swing_lows': swing_lows[-5:],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ORDER BLOCK DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
def detect_order_blocks(df, atr_series, lookback=20, disp_mult=1.3):
    close = df['close'].values; opn = df['open'].values
    high = df['high'].values; low = df['low'].values
    n = len(close)
    avg_range = pd.Series(high - low).rolling(20, min_periods=1).mean().values
    bull_obs = []; bear_obs = []

    for i in range(2, min(n, lookback + 2)):
        idx = n - 1 - i
        if idx < 1: break
        c_range = abs(close[idx + 1] - opn[idx + 1])
        safe_ar = max(avg_range[idx], 0.0001)
        if close[idx + 1] > opn[idx + 1] and c_range > safe_ar * disp_mult:
            for j in range(1, min(8, idx + 1)):
                if close[idx + 1 - j] < opn[idx + 1 - j]:
                    ob_top = max(opn[idx + 1 - j], close[idx + 1 - j])
                    ob_bot = low[idx + 1 - j]
                    bull_obs.append({'top': ob_top, 'bot': ob_bot, 'bar': idx + 1 - j, 'type': 'BULL'})
                    break
        if close[idx + 1] < opn[idx + 1] and c_range > safe_ar * disp_mult:
            for j in range(1, min(8, idx + 1)):
                if close[idx + 1 - j] > opn[idx + 1 - j]:
                    ob_top = high[idx + 1 - j]
                    ob_bot = min(opn[idx + 1 - j], close[idx + 1 - j])
                    bear_obs.append({'top': ob_top, 'bot': ob_bot, 'bar': idx + 1 - j, 'type': 'BEAR'})
                    break

    curr_price = close[-1]
    at_bull_ob = any(ob['bot'] <= curr_price <= ob['top'] * 1.002 for ob in bull_obs[:3])
    at_bear_ob = any(ob['bot'] * 0.998 <= curr_price <= ob['top'] for ob in bear_obs[:3])
    near_bull_ob = any(abs(curr_price - ob['top']) < atr_series.iloc[-1] * 0.5 for ob in bull_obs[:3]) if len(bull_obs) > 0 else False
    near_bear_ob = any(abs(curr_price - ob['bot']) < atr_series.iloc[-1] * 0.5 for ob in bear_obs[:3]) if len(bear_obs) > 0 else False

    return {
        'bull_obs': bull_obs[:5], 'bear_obs': bear_obs[:5],
        'at_bull_ob': bool(at_bull_ob), 'at_bear_ob': bool(at_bear_ob),
        'near_bull_ob': bool(near_bull_ob), 'near_bear_ob': bool(near_bear_ob),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  FVG DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
def detect_fvg(df, atr_series, min_atr_ratio=0.15):
    high = df['high'].values; low = df['low'].values
    close = df['close'].values; opn = df['open'].values
    n = len(close); fvgs = []
    for i in range(2, min(n, 30)):
        atr_val = max(float(atr_series.iloc[-i] if i < len(atr_series) else atr_series.iloc[-1]), 0.0001)
        if low[-i] > high[-i - 2] and close[-i - 1] > opn[-i - 1]:
            gap = low[-i] - high[-i - 2]
            if gap > atr_val * min_atr_ratio:
                fvgs.append({'type': 'BULL', 'hi': float(low[-i]), 'lo': float(high[-i - 2]),
                             'size': float(gap), 'mitigated': bool(low[-1] <= high[-i - 2])})
        if high[-i] < low[-i - 2] and close[-i - 1] < opn[-i - 1]:
            gap = low[-i - 2] - high[-i]
            if gap > atr_val * min_atr_ratio:
                fvgs.append({'type': 'BEAR', 'hi': float(low[-i - 2]), 'lo': float(high[-i]),
                             'size': float(gap), 'mitigated': bool(high[-1] >= low[-i - 2])})
    curr = close[-1]
    in_bull_fvg = any(f['type'] == 'BULL' and not f['mitigated'] and f['lo'] <= curr <= f['hi'] for f in fvgs)
    in_bear_fvg = any(f['type'] == 'BEAR' and not f['mitigated'] and f['lo'] <= curr <= f['hi'] for f in fvgs)
    return {'fvgs': fvgs[:10], 'in_bull_fvg': bool(in_bull_fvg), 'in_bear_fvg': bool(in_bear_fvg)}


# ═══════════════════════════════════════════════════════════════════════════════
#  PREMIUM / DISCOUNT ZONE
# ═══════════════════════════════════════════════════════════════════════════════
def calc_premium_discount(df, lookback=50):
    high_val = df['high'].iloc[-lookback:].max()
    low_val = df['low'].iloc[-lookback:].min()
    curr = float(df['close'].iloc[-1])
    rng = high_val - low_val
    if rng <= 0: return {'zone': 'EQUILIBRIUM', 'pct': 50.0, 'eq': curr}
    eq = low_val + rng * 0.5
    pct = ((curr - low_val) / rng) * 100
    zone = "PREMIUM" if curr > eq else ("DISCOUNT" if curr < eq else "EQUILIBRIUM")
    return {'zone': zone, 'pct': round(float(pct), 1), 'eq': round(float(eq), 2),
            'high': round(float(high_val), 2), 'low': round(float(low_val), 2)}


# ═══════════════════════════════════════════════════════════════════════════════
#  CANDLESTICK PATTERNS
# ═══════════════════════════════════════════════════════════════════════════════
def detect_candle_patterns(df):
    c = df['close'].values; o = df['open'].values
    h = df['high'].values; l = df['low'].values
    n = len(c)
    if n < 3: return {}
    body = abs(c[-1] - o[-1]); rng = h[-1] - l[-1]
    upper_wick = h[-1] - max(c[-1], o[-1]); lower_wick = min(c[-1], o[-1]) - l[-1]
    body_prev = abs(c[-2] - o[-2])
    bull_eng = (c[-1] > o[-1] and c[-2] < o[-2] and o[-1] <= c[-2] and c[-1] >= o[-2] and body > body_prev)
    bear_eng = (c[-1] < o[-1] and c[-2] > o[-2] and o[-1] >= c[-2] and c[-1] <= o[-2] and body > body_prev)
    hammer = body > 0 and lower_wick > body * 1.5 and c[-1] > o[-1]
    shoot_star = body > 0 and upper_wick > body * 1.5 and c[-1] < o[-1]
    doji = rng > 0 and body / rng < 0.1
    morning_star = False
    if n >= 3:
        morning_star = (c[-3] < o[-3] and body_prev < abs(c[-3] - o[-3]) * 0.5 and c[-1] > o[-1] and c[-1] > (o[-3] + c[-3]) / 2)
    return {
        'bull_engulf': bool(bull_eng), 'bear_engulf': bool(bear_eng),
        'hammer': bool(hammer), 'shoot_star': bool(shoot_star),
        'doji': bool(doji), 'morning_star': bool(morning_star),
        'any_bull': bool(bull_eng or hammer or morning_star),
        'any_bear': bool(bear_eng or shoot_star),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SESSION / MARKET INFO — US MARKETS (Eastern Time)
# ═══════════════════════════════════════════════════════════════════════════════
def get_market_session():
    n = now(); h = n.hour; m = n.minute; wd = n.weekday(); t = h * 60 + m
    if wd >= 5: return "WEEKEND 🔒", False
    if t < 240: return "OVERNIGHT 🌙", False           # Before 4:00 AM ET
    elif t < 570: return "PRE-MARKET ☀️", True          # 4:00 AM - 9:30 AM ET
    elif t < 600: return "OPEN AUCTION ⭐⭐", True       # 9:30 - 10:00 AM ET
    elif t < 720: return "MORNING SESSION ⭐⭐", True    # 10:00 AM - 12:00 PM ET
    elif t < 810: return "MIDDAY ⭐", True               # 12:00 PM - 1:30 PM ET
    elif t < 930: return "AFTERNOON ⭐", True            # 1:30 PM - 3:30 PM ET
    elif t < 960: return "POWER HOUR ⭐⭐⭐", True       # 3:30 PM - 4:00 PM ET
    elif t < 1200: return "AFTER-HOURS 🌆", True        # 4:00 PM - 8:00 PM ET
    else: return "CLOSED 🔒", False


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ANALYSIS ENGINE v5.0 — EARLY TREND DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
def analyze_stock(symbol):
    try:
        dec = 2; sector = C.get_sector(symbol)
        df = yfc.get_history(symbol, "3mo", "1h")
        if df is None or len(df) < 50: return None
        close = df['close']; high = df['high']; low = df['low']; vol = df['volume']
        price = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) > 1 else price
        chg = price - prev; chg_pct = (chg / prev * 100) if prev else 0
        if price < 1: dec = 4
        elif price < 10: dec = 3

        df_daily = yfc.get_daily(symbol)
        atr_s = calc_atr(high, low, close, C.ATR_PD)
        atr_v = float(atr_s.iloc[-1])
        if pd.isna(atr_v) or atr_v <= 0: atr_v = float((high - low).mean())

        # 1. MARKET STRUCTURE
        ms = detect_market_structure(df, C.MS_PIVOT_LEN, C.MS_BODY_BREAK)
        # 2. ORDER BLOCKS
        ob = detect_order_blocks(df, atr_s, C.OB_LOOKBACK, C.OB_DISP_MULT)
        # 3. FVG
        fvg = detect_fvg(df, atr_s, C.FVG_MIN_ATR)
        # 4. PREMIUM/DISCOUNT
        pd_zone = calc_premium_discount(df, 50)
        # 5. CANDLES
        candles = detect_candle_patterns(df)

        # 6. ENGINE A: TRIPLE CONFLUENCE
        rv = calc_rsi(close, C.RSI_PD)
        rh = rv.rolling(C.STOCH_PD, min_periods=1).max()
        rl = rv.rolling(C.STOCH_PD, min_periods=1).min()
        srK = sma(((rv - rl) / (rh - rl + 1e-10)) * 100.0, C.STOCH_RSI_K)
        srD = sma(srK, C.STOCH_RSI_D)
        ehh = high.rolling(C.ENH_K, min_periods=1).max()
        ell = low.rolling(C.ENH_K, min_periods=1).min()
        enK = sma(((close - ell) / (ehh - ell + 1e-10)) * 100.0, C.ENH_SLOW)
        enD = sma(enK, C.ENH_D)
        ml = ema(close, C.MACD_F) - ema(close, C.MACD_S)
        sl = ema(ml, C.MACD_SIG)
        lb = min(len(df), C.MACD_NLB)
        rH = pd.concat([ml.rolling(lb, min_periods=1).max(), sl.rolling(lb, min_periods=1).max()], axis=1).max(axis=1)
        rL = pd.concat([ml.rolling(lb, min_periods=1).min(), sl.rolling(lb, min_periods=1).min()], axis=1).min(axis=1)
        rng = rH - rL; pH = rH + rng * 0.1; pL = rL - rng * 0.1
        mnA = ((ml - pL) / (pH - pL + 1e-10) * 100.0).clip(0, 100)
        msA = ((sl - pL) / (pH - pL + 1e-10) * 100.0).clip(0, 100)

        sr_k = float(srK.iloc[-1]); sr_d = float(srD.iloc[-1])
        en_k = float(enK.iloc[-1]); en_d = float(enD.iloc[-1])
        mn_a = float(mnA.iloc[-1]); ms_a = float(msA.iloc[-1])

        def xo(a, b): return len(a) >= 2 and a.iloc[-1] > b.iloc[-1] and a.iloc[-2] <= b.iloc[-2]
        def xu(a, b): return len(a) >= 2 and a.iloc[-1] < b.iloc[-1] and a.iloc[-2] >= b.iloc[-2]
        def ir(v): return v > C.OS_LVL and v < C.OB_LVL
        def recent_xo(a, b, bars=3):
            for i in range(1, min(bars + 1, len(a))):
                if a.iloc[-i] > b.iloc[-i] and a.iloc[-i-1] <= b.iloc[-i-1]: return True
            return False
        def recent_xu(a, b, bars=3):
            for i in range(1, min(bars + 1, len(a))):
                if a.iloc[-i] < b.iloc[-i] and a.iloc[-i-1] >= b.iloc[-i-1]: return True
            return False

        bx = sum([xo(srK, srD) and ir(sr_k), xo(enK, enD) and ir(en_k), xo(mnA, msA) and ir(mn_a)])
        sx = sum([xu(srK, srD) and ir(sr_k), xu(enK, enD) and ir(en_k), xu(mnA, msA) and ir(mn_a)])
        rbx = sum([recent_xo(srK, srD), recent_xo(enK, enD), recent_xo(mnA, msA)])
        rsx = sum([recent_xu(srK, srD), recent_xu(enK, enD), recent_xu(mnA, msA)])

        bull_zn = sum([sr_k > C.NEU_HI, en_k > C.NEU_HI, mn_a > C.NEU_HI])
        bear_zn = sum([sr_k < C.NEU_LO, en_k < C.NEU_LO, mn_a < C.NEU_LO])

        ema20 = float(ema(close, 20).iloc[-1])
        ema50 = float(ema(close, 50).iloc[-1])
        ema200_v = float(ema(close, 200).iloc[-1]) if len(close) >= 200 else float(ema(close, min(len(close), 100)).iloc[-1])
        above_ma = price > ema50
        conf_buy = bool((bx >= 2 or rbx >= 2) and above_ma)
        conf_sell = bool((sx >= 2 or rsx >= 2) and not above_ma)

        # 7. ENGINE B: SMARTMOMENTUM
        rsi_v = float(calc_rsi(close, C.RSI_PD).iloc[-1])
        rsi_sc = float((rsi_v - 50) * 2.0)
        ml_v = float(ml.iloc[-1]); sl_v = float(sl.iloc[-1])
        macd_nb = float(np.clip((ml_v / max(atr_v, 1e-10)) * 50.0, -100, 100))
        macd_sc = float(np.clip(macd_nb + (15 if ml_v > sl_v else -15), -100, 100))
        stv = calc_stoch(close, high, low, C.STOCH_PD)
        st_sig = sma(stv, 3)
        stoch_sc = float(np.clip((stv.iloc[-1] - 50) * 2.0 + (10 if stv.iloc[-1] > st_sig.iloc[-1] else -10), -100, 100))
        cci_sc = float(np.clip(float(calc_cci(close, high, low, C.CCI_PD).iloc[-1]) / 2.0, -100, 100))
        wpr_sc = float((float(calc_wpr(close, high, low, C.WPR_PD).iloc[-1]) + 50) * 2.0)
        adx_v, pdi, mdi = calc_adx(high, low, close, C.ADX_PD)
        adx_val = float(adx_v.iloc[-1]); pdi_v = float(pdi.iloc[-1]); mdi_v = float(mdi.iloc[-1])
        adx_sc = float(np.clip(adx_val * 2.0, 0, 100) if pdi_v > mdi_v else np.clip(-adx_val * 2.0, -100, 0))
        mom_pd = min(C.MOM_PD, len(close) - 1)
        mom_pct = float((close.iloc[-1] - close.iloc[-mom_pd]) / close.iloc[-mom_pd] * 100) if mom_pd > 0 else 0.0
        mom_sc = float(np.clip(mom_pct * 10.0, -100, 100))

        wts = [C.RSI_W, C.MACD_W, C.STOCH_W, C.CCI_W, C.WPR_W, C.ADX_W, C.MOM_W]
        scs = [rsi_sc, macd_sc, stoch_sc, cci_sc, wpr_sc, adx_sc, mom_sc]
        tw = sum(wts)
        momentum = round(sum(s * w for s, w in zip(scs, wts)) / tw, 2) if tw > 0 else 0.0

        # 8. DIVERGENCE
        div = "NONE"
        try:
            if len(df) > C.DIV_BARS:
                rl_ = low.iloc[-C.DIV_BARS:]; rs_ = stv.iloc[-C.DIV_BARS:]
                if low.iloc[-1] < rl_.min() * 1.001 and stv.iloc[-1] > rs_.min(): div = "BULL"
                rh_ = high.iloc[-C.DIV_BARS:]
                if high.iloc[-1] > rh_.max() * 0.999 and stv.iloc[-1] < rs_.max(): div = "BEAR"
        except: pass

        # 9. VOLUME
        va = float(vol.rolling(C.VOL_PD, min_periods=1).mean().iloc[-1])
        vr = round(float(vol.iloc[-1]) / va, 2) if va > 0 else 1.0
        obv = calc_obv(close, vol); obv_sma_v = sma(obv, 20)
        obv_bullish = bool(obv.iloc[-1] > obv_sma_v.iloc[-1])
        vol_trend = float(vol.iloc[-3:].mean()) > float(vol.iloc[-10:-3].mean()) * 1.1 if len(vol) > 10 else False

        # 10. MTF
        mtf = {}
        h_score = 50 + (rsi_v - 50) * 0.8
        mtf['1h'] = {'score': round(h_score, 1), 'summary': 'Buy' if h_score > 55 else ('Sell' if h_score < 45 else 'Neutral')}
        if df_daily is not None and len(df_daily) >= 20:
            dc = df_daily['close']; dh = df_daily['high']; dl = df_daily['low']
            d_rsi = float(calc_rsi(dc, 14).iloc[-1])
            d_ema20 = float(ema(dc, 20).iloc[-1]); d_ema50 = float(ema(dc, 50).iloc[-1])
            d_score = float(50 + np.clip((d_rsi - 50) * 0.6 + (25 if float(dc.iloc[-1]) > d_ema20 else -25) + (15 if float(dc.iloc[-1]) > d_ema50 else -15), -50, 50))
            mtf['1D'] = {'score': round(d_score, 1), 'summary': 'Buy' if d_score > 55 else ('Sell' if d_score < 45 else 'Neutral')}
            if len(df_daily) >= 30:
                wc = dc.iloc[-5:]
                wt = float((wc.iloc[-1] - wc.iloc[0]) / wc.iloc[0] * 100)
                ws = float(50 + np.clip(wt * 10, -40, 40))
                mtf['1W'] = {'score': round(ws, 1), 'summary': 'Buy' if ws > 55 else ('Sell' if ws < 45 else 'Neutral')}
        for tf in ['1h', '4h', '1D', '1W']:
            if tf not in mtf: mtf[tf] = {'score': 50.0, 'summary': 'Neutral'}

        mtf_buy = sum(1 for v in mtf.values() if v['score'] > 55)
        mtf_sell = sum(1 for v in mtf.values() if v['score'] < 45)
        mtf_align = "★ ALL BUY" if mtf_buy >= 3 else ("★ ALL SELL" if mtf_sell >= 3 else ("↗ Most Buy" if mtf_buy >= 2 else ("↘ Most Sell" if mtf_sell >= 2 else "Mixed")))
        htf_score = float(mtf.get('1D', {}).get('score', 50))

        ema_golden = bool(ema20 > ema50)
        trend_label = "UPTREND" if ema_golden and price > ema20 else ("DOWNTREND" if not ema_golden and price < ema20 else "SIDEWAYS")
        session_name, in_session = get_market_session()

        # 11. SIGNAL STRENGTH SCORING
        am = abs(momentum); sig_str = 0.0
        if ms:
            if ms.get('choch_bull'): sig_str += 22
            if ms.get('choch_bear'): sig_str += 22
            if ms.get('bos_bull') and momentum > 0: sig_str += 12
            if ms.get('bos_bear') and momentum < 0: sig_str += 12
            if ms.get('trend') == 1 and momentum > 0: sig_str += 5
            if ms.get('trend') == -1 and momentum < 0: sig_str += 5
        if ob.get('at_bull_ob') and momentum > -C.CHG_TH: sig_str += 12
        if ob.get('at_bear_ob') and momentum < C.CHG_TH: sig_str += 12
        if ob.get('near_bull_ob'): sig_str += 5
        if ob.get('near_bear_ob'): sig_str += 5
        if fvg.get('in_bull_fvg') and momentum > 0: sig_str += 8
        if fvg.get('in_bear_fvg') and momentum < 0: sig_str += 8
        if pd_zone['zone'] == 'DISCOUNT' and momentum > 0: sig_str += 8
        if pd_zone['zone'] == 'PREMIUM' and momentum < 0: sig_str += 8
        if candles.get('any_bull') and momentum > -C.CHG_TH: sig_str += 8
        if candles.get('any_bear') and momentum < C.CHG_TH: sig_str += 8
        if conf_buy: sig_str += 15
        if conf_sell: sig_str += 15
        if (momentum > 0 and bull_zn >= 2) or (momentum < 0 and bear_zn >= 2): sig_str += 8
        sig_str += 12 if am >= C.SIG_TH else (7 if am >= C.CHG_TH else 2)
        if div == "BULL" and momentum > -C.CHG_TH: sig_str += 10
        if div == "BEAR" and momentum < C.CHG_TH: sig_str += 10
        if vr > 1.5: sig_str += 8
        elif vr > 1.2: sig_str += 4
        if obv_bullish and momentum > 0: sig_str += 4
        if not obv_bullish and momentum < 0: sig_str += 4
        if vol_trend: sig_str += 3
        if (momentum > 0 and htf_score > 55) or (momentum < 0 and htf_score < 45): sig_str += 8
        if (momentum > 0 and ema_golden) or (momentum < 0 and not ema_golden): sig_str += 4
        sig_str = min(sig_str, 100)

        if momentum >= C.SIG_TH: zone = "STRONG BUY"
        elif momentum >= C.CHG_TH: zone = "BULLISH"
        elif momentum <= -C.SIG_TH: zone = "STRONG SELL"
        elif momentum <= -C.CHG_TH: zone = "BEARISH"
        else: zone = "NEUTRAL"

        # 12. SIGNAL GENERATION
        signal = None; reasons = []; direction = None; sig_type = None
        st_val = float(stv.iloc[-1])
        early_buy = False; early_sell = False

        if ms and ms.get('choch_bull') and (candles.get('any_bull') or momentum > 0 or conf_buy): early_buy = True
        if ms and ms.get('choch_bear') and (candles.get('any_bear') or momentum < 0 or conf_sell): early_sell = True
        if div == "BULL" and pd_zone['zone'] == 'DISCOUNT' and (ob.get('at_bull_ob') or ob.get('near_bull_ob')): early_buy = True
        if div == "BEAR" and pd_zone['zone'] == 'PREMIUM' and (ob.get('at_bear_ob') or ob.get('near_bear_ob')): early_sell = True

        bull_rev = st_val < 35 and momentum > -C.CHG_TH
        bear_rev = st_val > 65 and momentum < C.CHG_TH
        if bull_rev and ob.get('at_bull_ob') and candles.get('any_bull'): early_buy = True
        if bear_rev and ob.get('at_bear_ob') and candles.get('any_bear'): early_sell = True

        strong_buy = ((momentum >= C.SIG_TH) or conf_buy or early_buy) and sig_str >= C.STRONG_STR
        strong_sell = ((momentum <= -C.SIG_TH) or conf_sell or early_sell) and sig_str >= C.STRONG_STR
        c_buy = conf_buy and not strong_buy
        c_sell = conf_sell and not strong_sell
        w_buy = (momentum >= C.CHG_TH or bull_rev or bull_zn >= 2) and not strong_buy and not c_buy
        w_sell = (momentum <= -C.CHG_TH or bear_rev or bear_zn >= 2) and not strong_sell and not c_sell

        if early_buy and sig_str >= C.EARLY_TREND_MIN_STR and not strong_buy:
            sig_type = "EARLY TREND"; direction = "BUY"
            reasons.append("🌅 Early Trend — Buy at trend inception")
        elif early_sell and sig_str >= C.EARLY_TREND_MIN_STR and not strong_sell:
            sig_type = "EARLY TREND"; direction = "SELL"
            reasons.append("🌅 Early Trend — Sell at trend inception")
        elif strong_buy:
            direction = "BUY"; sig_type = "STRONG"
            reasons.append("🔥 Strong Buy signal")
        elif strong_sell:
            direction = "SELL"; sig_type = "STRONG"
            reasons.append("🔥 Strong Sell signal")
        elif c_buy:
            direction = "BUY"; sig_type = "CONF"
            reasons.append(f"◆ Confluence Buy ({max(bx, rbx)}/3)")
        elif c_sell:
            direction = "SELL"; sig_type = "CONF"
            reasons.append(f"◆ Confluence Sell ({max(sx, rsx)}/3)")
        elif w_buy:
            direction = "BUY"; sig_type = "WEAK"
            reasons.append("◇ Weak Buy signal")
        elif w_sell:
            direction = "SELL"; sig_type = "WEAK"
            reasons.append("◇ Weak Sell signal")

        if direction:
            if ms and ms.get('choch_bull') and direction == "BUY": reasons.append("🔄 CHoCH Bull — Structure shift up")
            if ms and ms.get('choch_bear') and direction == "SELL": reasons.append("🔄 CHoCH Bear — Structure shift down")
            if ms and ms.get('bos_bull') and direction == "BUY": reasons.append("📈 BOS Bull — Trend continuation")
            if ms and ms.get('bos_bear') and direction == "SELL": reasons.append("📉 BOS Bear — Trend continuation")
            if ob.get('at_bull_ob') and direction == "BUY": reasons.append("🧱 At Bull Order Block")
            if ob.get('at_bear_ob') and direction == "SELL": reasons.append("🧱 At Bear Order Block")
            if fvg.get('in_bull_fvg') and direction == "BUY": reasons.append("📐 In Bull FVG")
            if fvg.get('in_bear_fvg') and direction == "SELL": reasons.append("📐 In Bear FVG")
            if pd_zone['zone'] == 'DISCOUNT' and direction == "BUY": reasons.append(f"💰 Discount Zone ({pd_zone['pct']:.0f}%)")
            if pd_zone['zone'] == 'PREMIUM' and direction == "SELL": reasons.append(f"💸 Premium Zone ({pd_zone['pct']:.0f}%)")
            if candles.get('bull_engulf') and direction == "BUY": reasons.append("🕯️ Bullish Engulfing")
            if candles.get('bear_engulf') and direction == "SELL": reasons.append("🕯️ Bearish Engulfing")
            if candles.get('hammer') and direction == "BUY": reasons.append("🔨 Hammer")
            if candles.get('shoot_star') and direction == "SELL": reasons.append("⭐ Shooting Star")
            if above_ma and direction == "BUY": reasons.append("✅ Above EMA50")
            if not above_ma and direction == "SELL": reasons.append("✅ Below EMA50")
            if ema_golden and direction == "BUY": reasons.append("✅ Golden Cross")
            if not ema_golden and direction == "SELL": reasons.append("✅ Death Cross")
            if bull_zn >= 2 and direction == "BUY": reasons.append(f"✅ Bull Zone {bull_zn}/3")
            if bear_zn >= 2 and direction == "SELL": reasons.append(f"✅ Bear Zone {bear_zn}/3")
            if div != "NONE": reasons.append(f"✅ {div} Divergence")
            if vr > 1.5: reasons.append(f"✅ Vol {vr:.1f}x")
            if obv_bullish and direction == "BUY": reasons.append("✅ OBV Bullish")
            if not obv_bullish and direction == "SELL": reasons.append("✅ OBV Bearish")
            if in_session: reasons.append(f"✅ {session_name}")

            if direction == "BUY":
                sl_ = round(price - atr_v * C.ATR_SL, dec); risk = price - sl_
                tp1_ = round(price + risk * 1.5, dec)
                tp2_ = round(price + risk * C.RR, dec)
                tp3_ = round(price + risk * 3.0, dec)
            else:
                sl_ = round(price + atr_v * C.ATR_SL, dec); risk = sl_ - price
                tp1_ = round(price - risk * 1.5, dec)
                tp2_ = round(price - risk * C.RR, dec)
                tp3_ = round(price - risk * 3.0, dec)
            risk_pct = round(risk / price * 100, 2) if price > 0 else 0.0
            rr = round(abs(price - tp2_) / max(risk, 1e-10), 2)
            ms_trend_txt = "BULL" if ms and ms['trend'] == 1 else ("BEAR" if ms and ms['trend'] == -1 else "RANGE")

            signal = {
                'id': f"{symbol}_{direction}_{now().strftime('%H%M%S')}",
                'symbol': symbol, 'sector': sector, 'direction': direction, 'type': sig_type,
                'entry': round(price, dec), 'sl': float(sl_),
                'tp1': float(tp1_), 'tp2': float(tp2_), 'tp3': float(tp3_),
                'momentum': float(momentum), 'strength': round(sig_str),
                'conf': int(max(bx, rbx) if direction == "BUY" else max(sx, rsx)),
                'srK': round(sr_k, 1), 'enK': round(en_k, 1), 'mnA': round(mn_a, 1),
                'rsi_v': round(rsi_v, 1),
                'rsi_sc': round(rsi_sc, 1), 'macd_sc': round(macd_sc, 1),
                'stoch_sc': round(stoch_sc, 1), 'cci_sc': round(cci_sc, 1),
                'wpr_sc': round(wpr_sc, 1), 'adx_sc': round(adx_sc, 1), 'mom_sc': round(mom_sc, 1),
                'trend': trend_label, 'ms_trend': ms_trend_txt,
                'choch': bool((ms and ms.get('choch_bull')) or (ms and ms.get('choch_bear'))),
                'bos': bool((ms and ms.get('bos_bull')) or (ms and ms.get('bos_bear'))),
                'at_ob': bool(ob.get('at_bull_ob') or ob.get('at_bear_ob')),
                'in_fvg': bool(fvg.get('in_bull_fvg') or fvg.get('in_bear_fvg')),
                'pd_zone': pd_zone['zone'], 'pd_pct': pd_zone['pct'],
                'candle': 'BULL' if candles.get('any_bull') else ('BEAR' if candles.get('any_bear') else 'NONE'),
                'bull_zn': int(bull_zn), 'bear_zn': int(bear_zn),
                'div': div, 'vol': float(vr), 'htf': round(htf_score, 1),
                'mtf_align': mtf_align, 'session': session_name,
                'atr': round(atr_v, dec + 2), 'risk_pct': float(risk_pct), 'rr': float(rr),
                'reasons': reasons, 'timestamp': dts(), 'ts_unix': time.time(),
            }

        analysis = {
            'symbol': symbol, 'cat': sector, 'price': round(price, dec),
            'prev': round(prev, dec), 'change': round(chg, dec + 1), 'change_pct': round(chg_pct, 3),
            'momentum': float(momentum), 'strength': round(sig_str), 'zone': zone,
            'session': session_name, 'trend': trend_label,
            'ms_trend': ms['trend'] if ms else 0,
            'choch_bull': bool(ms and ms.get('choch_bull')),
            'choch_bear': bool(ms and ms.get('choch_bear')),
            'bos_bull': bool(ms and ms.get('bos_bull')),
            'bos_bear': bool(ms and ms.get('bos_bear')),
            'at_ob': bool(ob.get('at_bull_ob') or ob.get('at_bear_ob')),
            'in_fvg': bool(fvg.get('in_bull_fvg') or fvg.get('in_bear_fvg')),
            'pd_zone': pd_zone['zone'], 'pd_pct': pd_zone['pct'],
            'srK': round(sr_k, 1), 'enK': round(en_k, 1), 'mnA': round(mn_a, 1),
            'bull_zn': int(bull_zn), 'bear_zn': int(bear_zn),
            'bx': int(max(bx, rbx)), 'sx': int(max(sx, rsx)),
            'conf_buy': bool(conf_buy), 'conf_sell': bool(conf_sell),
            'above_ma': bool(above_ma), 'ema20': round(ema20, dec), 'ema50': round(ema50, dec),
            'rsi_v': round(rsi_v, 1), 'adx_v': round(adx_val, 1),
            'div': div, 'vol': float(vr), 'atr': round(atr_v, dec + 2),
            'obv_bull': bool(obv_bullish), 'mtf': mtf, 'mtf_align': mtf_align, 'time': ts(),
        }
        return analysis, signal
    except Exception as e:
        logger.error(f"Analyze {symbol}: {e}\n{traceback.format_exc()}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  STORE & AUTH
# ═══════════════════════════════════════════════════════════════════════════════
class Store:
    def __init__(self):
        self.prices = {}; self.signals = {}; self.analysis = {}; self.history = []
        self.last_scan = "Never"; self.scan_count = 0; self.connected = 0
        self.status = "STARTING"; self.first_scan_done = False
        self.stats = {'buy': 0, 'sell': 0, 'strong': 0, 'conf': 0, 'weak': 0, 'early': 0}
        self.dead_tickers = []; self.ok_count = 0; self.fail_count = 0

    def get_state(self):
        return sanitize({
            'prices': self.prices, 'signals': self.signals,
            'analysis': self.analysis, 'history': self.history[:200],
            'scan_count': self.scan_count, 'last_scan': self.last_scan,
            'status': self.status, 'stats': self.stats,
            'dead_tickers': self.dead_tickers,
            'ok_count': self.ok_count, 'fail_count': self.fail_count,
        })

store = Store()

class Users:
    def __init__(self):
        self.u = {'admin': {'pw': hashlib.sha256(b'admin123_tcsm').hexdigest(), 'role': 'admin', 'on': True}}
    def verify(self, u, p):
        if u not in self.u: return False
        return self.u[u].get('on', True) and self.u[u]['pw'] == hashlib.sha256(f"{p}_tcsm".encode()).hexdigest()

users = Users()
def login_req(f):
    @wraps(f)
    def d(*a, **k):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*a, **k)
    return d


# ═══════════════════════════════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════════════════════════════
def scanner():
    gevent.sleep(8)
    logger.info("🚀 TCSM US v5.0 Early Trend Detection Scanner started")
    store.status = "RUNNING"
    with app.app_context():
        while True:
            try:
                store.scan_count += 1; store.last_scan = ts()
                t0 = time.time(); ok = 0; fail = 0
                stocks = [s for s in C.STOCKS if not yfc.is_dead(s)]
                store.dead_tickers = list(yfc._dead_tickers)

                for i in range(0, len(stocks), C.BATCH_SIZE):
                    batch = stocks[i:i+C.BATCH_SIZE]
                    for symbol in batch:
                        try:
                            result = analyze_stock(symbol)
                            if result:
                                analysis, signal = result; ok += 1
                                analysis = sanitize(analysis)
                                store.analysis[symbol] = analysis
                                store.prices[symbol] = {
                                    'symbol': symbol, 'cat': analysis['cat'], 'price': analysis['price'],
                                    'change': analysis['change'], 'change_pct': analysis['change_pct'], 'time': analysis['time']
                                }
                                safe_emit('price_update', {'symbol': symbol, 'data': store.prices[symbol], 'analysis': analysis})
                                if signal:
                                    signal = sanitize(signal)
                                    old = store.signals.get(symbol)
                                    is_new = not old or old.get('direction') != signal['direction'] or time.time() - old.get('ts_unix', 0) > 1800
                                    if is_new:
                                        store.signals[symbol] = signal
                                        store.history.insert(0, signal)
                                        store.history = store.history[:500]
                                        store.stats['buy' if signal['direction'] == "BUY" else 'sell'] += 1
                                        st = signal['type'].lower().replace(' ', '_')
                                        if 'early' in st: store.stats['early'] = store.stats.get('early', 0) + 1
                                        elif st in store.stats: store.stats[st] += 1
                                        safe_emit('new_signal', {'symbol': symbol, 'signal': signal})
                                        logger.info(f"🎯 {signal['type']} {signal['direction']} {symbol} @${signal['entry']} Str:{signal['strength']}%")
                            else: fail += 1
                        except Exception as e:
                            fail += 1; logger.warning(f"{symbol}: {e}")
                        gevent.sleep(0.8)
                    gevent.sleep(2)

                dur = time.time() - t0
                store.first_scan_done = True; store.ok_count = ok; store.fail_count = fail
                safe_emit('scan_update', sanitize({
                    'scan_count': store.scan_count, 'last_scan': store.last_scan,
                    'stats': store.stats, 'status': store.status,
                    'ok': ok, 'fail': fail, 'dur': round(dur, 1), 'signals': len(store.signals),
                }))
                safe_emit('full_sync', store.get_state())
                logger.info(f"✅ Scan #{store.scan_count}: {ok}/{ok+fail} ({dur:.1f}s) Signals:{len(store.signals)}")
            except Exception as e:
                logger.error(f"Scanner: {e}\n{traceback.format_exc()}")
            gevent.sleep(C.SCAN_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════════
#  HTML TEMPLATES — US STOCK EDITION
# ═══════════════════════════════════════════════════════════════════════════════
LOGIN_HTML = '''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TCSM US v5.0 Early Trend</title><script src="https://cdn.tailwindcss.com"></script>
<style>@keyframes gradient{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
body{background:linear-gradient(-45deg,#020617,#0f172a,#020617,#1e1b4b);background-size:400% 400%;animation:gradient 20s ease infinite}
.card{background:rgba(15,23,42,.7);backdrop-filter:blur(24px);border:1px solid rgba(255,255,255,.06)}</style></head>
<body class="min-h-screen flex items-center justify-center p-4">
<div class="card rounded-3xl w-full max-w-md p-10">
<div class="text-center mb-8"><div class="text-6xl mb-4">🇺🇸</div>
<h1 class="text-3xl font-black bg-clip-text text-transparent bg-gradient-to-r from-blue-300 via-cyan-400 to-emerald-400">TCSM US v5.0</h1>
<p class="text-gray-500 text-sm mt-2">US Stock Early Trend Detection Engine</p>
<div class="flex justify-center gap-2 mt-3">
<span class="text-[10px] bg-violet-500/10 text-violet-400 px-3 py-1 rounded-full border border-violet-500/20">CHoCH/BOS</span>
<span class="text-[10px] bg-cyan-500/10 text-cyan-400 px-3 py-1 rounded-full border border-cyan-500/20">Order Blocks</span>
<span class="text-[10px] bg-emerald-500/10 text-emerald-400 px-3 py-1 rounded-full border border-emerald-500/20">Dual Engine</span>
</div></div>
{% with m=get_flashed_messages(with_categories=true) %}{% if m %}{% for c,msg in m %}
<div class="mb-4 p-3 rounded-xl text-sm {% if c=='error' %}bg-red-500/10 text-red-300 border border-red-500/20{% else %}bg-green-500/10 text-green-300{% endif %}">{{msg}}</div>
{% endfor %}{% endif %}{% endwith %}
<form method="POST" class="space-y-5">
<div><label class="block text-gray-400 text-[11px] font-semibold mb-2 uppercase tracking-widest">Username</label>
<input type="text" name="u" required class="w-full px-4 py-3 bg-slate-900/60 border border-slate-700/40 rounded-xl text-white focus:border-cyan-500/40 focus:outline-none" placeholder="Username"></div>
<div><label class="block text-gray-400 text-[11px] font-semibold mb-2 uppercase tracking-widest">Password</label>
<input type="password" name="p" required class="w-full px-4 py-3 bg-slate-900/60 border border-slate-700/40 rounded-xl text-white focus:border-cyan-500/40 focus:outline-none" placeholder="Password"></div>
<button type="submit" class="w-full py-3 bg-gradient-to-r from-blue-600 to-cyan-500 rounded-xl font-bold text-white hover:opacity-90 transition">🔓 Sign In</button>
</form></div></body></html>'''

DASH_HTML = '''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🇺🇸 TCSM US v5.0 Early Trend</title><script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
<style>
*{scrollbar-width:thin;scrollbar-color:#1e293b transparent}
@keyframes pulse2{0%,100%{opacity:1}50%{opacity:.35}}.pulse2{animation:pulse2 1.5s infinite}
@keyframes glow{0%,100%{box-shadow:0 0 12px rgba(6,182,212,.15)}50%{box-shadow:0 0 30px rgba(6,182,212,.35)}}.glow{animation:glow 2.5s infinite}
@keyframes slideUp{from{transform:translateY(12px);opacity:0}to{transform:translateY(0);opacity:1}}.slideUp{animation:slideUp .3s ease-out}
.glass{background:rgba(15,23,42,.65);backdrop-filter:blur(16px);border:1px solid rgba(51,65,85,.3)}
.gc{background:rgba(15,23,42,.5);backdrop-filter:blur(8px);border:1px solid rgba(51,65,85,.25);transition:all .2s}
.gc:hover{border-color:rgba(6,182,212,.15)}
.sc{background:rgba(15,23,42,.5);backdrop-filter:blur(8px);border:1px solid rgba(51,65,85,.2)}
.sb{border-left:3px solid #22c55e;background:linear-gradient(135deg,rgba(22,101,52,.06),transparent)}
.ss{border-left:3px solid #ef4444;background:linear-gradient(135deg,rgba(127,29,29,.06),transparent)}
.se{border-left:3px solid #a855f7;background:linear-gradient(135deg,rgba(88,28,135,.08),transparent)}
.tag{display:inline-flex;align-items:center;padding:2px 8px;border-radius:6px;font-size:9px;font-weight:600}
body{background:#020617;color:#e2e8f0}
</style></head>
<body class="min-h-screen text-[13px]">
<div class="max-w-[1800px] mx-auto px-3 py-3">

<div class="flex flex-wrap justify-between items-center mb-3 gap-2">
<div><div class="flex items-center gap-3">
<h1 class="text-lg font-black bg-clip-text text-transparent bg-gradient-to-r from-blue-300 via-cyan-400 to-emerald-400">🇺🇸 TCSM US v5.0 Early Trend</h1>
<span class="tag bg-violet-500/10 text-violet-400 border border-violet-500/20">CHoCH/BOS</span>
<span class="tag bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">OB+FVG</span>
<span class="tag bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">NYSE/NASDAQ</span>
</div><p class="text-slate-600 text-[10px] mt-0.5">Market Structure • Order Blocks • FVG • Premium/Discount • Triple Confluence • SmartMomentum</p>
</div>
<div class="flex items-center gap-3">
<div id="clk" class="glass px-4 py-2 rounded-xl font-mono text-sm font-bold text-cyan-400">--:--:--</div>
<span id="st" class="text-red-400 text-sm">● Offline</span>
<button onclick="doRefresh()" class="px-3 py-2 bg-blue-600/50 hover:bg-blue-500/70 rounded-xl text-xs border border-blue-500/20">🔄</button>
<a href="/logout" class="px-3 py-2 bg-red-600/40 hover:bg-red-500/60 rounded-xl text-xs border border-red-500/20">Logout</a>
</div></div>

<div class="grid grid-cols-4 md:grid-cols-9 gap-2 mb-3">
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Scans</div><div id="scc" class="font-black text-lg mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Signals</div><div id="si" class="font-black text-lg text-cyan-400 mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">🌅 Early</div><div id="ea" class="font-black text-lg text-violet-400 mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">🔥 Strong</div><div id="st2" class="font-black text-lg text-amber-300 mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Buy</div><div id="bu" class="font-black text-lg text-emerald-400 mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Sell</div><div id="se" class="font-black text-lg text-red-400 mt-0.5">0</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Session</div><div id="ses" class="text-cyan-400 font-bold text-[10px] mt-1">—</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Last Scan</div><div id="ls" class="font-mono text-xs mt-1">--:--</div></div>
<div class="sc rounded-xl p-2.5 text-center"><div class="text-slate-500 text-[9px] uppercase font-semibold">Status</div><div id="ss2" class="text-cyan-400 font-bold text-xs mt-1">INIT</div></div>
</div>

<div id="al" class="hidden mb-3"><div class="bg-gradient-to-r from-violet-950/50 to-cyan-950/30 border border-violet-600/40 rounded-2xl p-5 glow">
<div class="flex items-center gap-4"><span class="text-4xl" id="ai">🌅</span>
<div class="flex-1"><div id="at" class="font-bold text-cyan-200 text-base"></div><div id="ax" class="text-cyan-300/60 text-sm mt-1"></div></div>
<button onclick="this.closest('#al').classList.add('hidden')" class="text-cyan-400/40 hover:text-white text-2xl">✕</button></div></div></div>

<div class="grid grid-cols-1 xl:grid-cols-12 gap-3">

<div class="xl:col-span-3 space-y-3"><div class="glass rounded-2xl p-3">
<div class="flex justify-between items-center mb-2">
<h2 class="font-bold text-sm">💹 US Stocks <span id="ps" class="text-[9px] text-slate-600">loading</span></h2>
<input id="search" type="text" placeholder="Search..." class="bg-slate-900/50 border border-slate-700/30 rounded-xl px-3 py-1.5 text-xs w-28 focus:outline-none focus:border-cyan-500/40" oninput="rP()">
</div>
<div class="flex flex-wrap gap-1 mb-2 text-[10px]">
<button onclick="fP('all')" class="px-2 py-1 rounded-lg pf bg-cyan-600/80 font-semibold" data-c="all">All</button>
<button onclick="fP('Tech')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Tech">Tech</button>
<button onclick="fP('Finance')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Finance">Finance</button>
<button onclick="fP('Health')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Health">Health</button>
<button onclick="fP('Energy')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Energy">Energy</button>
<button onclick="fP('Consumer')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Consumer">Consumer</button>
<button onclick="fP('Industrial')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Industrial">Industrial</button>
<button onclick="fP('Growth')" class="px-2 py-1 rounded-lg pf bg-slate-700/60 font-semibold" data-c="Growth">Growth</button>
</div>
<div id="pr" class="space-y-1.5 max-h-[620px] overflow-y-auto pr-1"></div>
</div></div>

<div class="xl:col-span-6 space-y-3">
<div class="flex justify-between items-center">
<h2 class="font-bold text-sm">🎯 Early Trend Signals</h2>
<div class="flex gap-1 text-[10px]">
<button onclick="fS('all')" class="px-2 py-1 rounded-lg sf bg-cyan-600/80 font-semibold" data-c="all">All</button>
<button onclick="fS('EARLY')" class="px-2 py-1 rounded-lg sf bg-slate-700/60 font-semibold" data-c="EARLY">🌅 Early</button>
<button onclick="fS('STRONG')" class="px-2 py-1 rounded-lg sf bg-slate-700/60 font-semibold" data-c="STRONG">🔥 Strong</button>
<button onclick="fS('BUY')" class="px-2 py-1 rounded-lg sf bg-slate-700/60 font-semibold" data-c="BUY">🟢 Buy</button>
<button onclick="fS('SELL')" class="px-2 py-1 rounded-lg sf bg-slate-700/60 font-semibold" data-c="SELL">🔴 Sell</button>
</div></div>
<div id="sg" class="space-y-2.5"><div class="glass rounded-2xl p-10 text-center text-slate-500">
<div class="text-5xl mb-4">🇺🇸</div><div class="text-base font-semibold">Scanning for Early Trends...</div>
<div class="text-xs mt-2 text-slate-600">CHoCH/BOS • Order Blocks • FVG • Dual Engine</div></div></div></div>

<div class="xl:col-span-3 space-y-3">
<div class="glass rounded-2xl p-3"><h3 class="font-bold text-[11px] text-violet-400 mb-2">🌅 Early Trend Picks</h3>
<div id="ep" class="space-y-1 max-h-[180px] overflow-y-auto"></div></div>
<div class="glass rounded-2xl p-3"><h3 class="font-bold text-[11px] text-cyan-400 mb-2">📊 Top Momentum</h3>
<div id="tm" class="space-y-1 max-h-[180px] overflow-y-auto"></div></div>
<div class="glass rounded-2xl p-3"><h3 class="font-bold text-[11px] text-emerald-400 mb-2">📈 Sector Heatmap</h3>
<div id="hm" class="grid grid-cols-2 gap-1.5 text-[10px]"></div></div>
<div class="glass rounded-2xl p-3"><h3 class="font-bold text-[11px] text-cyan-400 mb-2">📡 Live Log</h3>
<div id="lg" class="h-28 overflow-y-auto font-mono text-[9px] space-y-0.5"></div></div>
</div></div>

<div class="glass rounded-2xl overflow-hidden mt-3">
<div class="flex justify-between items-center p-3 border-b border-slate-800/40">
<h2 class="font-bold text-sm">📜 Signal History</h2><span class="text-[10px] text-slate-500" id="hc">0</span></div>
<div class="overflow-x-auto"><table class="w-full text-[11px]">
<thead class="bg-slate-900/50"><tr class="text-slate-500 uppercase text-[9px] tracking-wider">
<th class="px-2 py-2 text-left">Time</th><th class="px-2 py-2">Ticker</th><th class="px-2 py-2">Type</th><th class="px-2 py-2">Dir</th>
<th class="px-2 py-2 text-right">Entry</th><th class="px-2 py-2 text-right">SL</th>
<th class="px-2 py-2 text-right">TP1</th><th class="px-2 py-2 text-right">TP2</th>
<th class="px-2 py-2">R:R</th><th class="px-2 py-2">Str%</th><th class="px-2 py-2">Mom</th><th class="px-2 py-2">Structure</th><th class="px-2 py-2">Zone</th>
</tr></thead><tbody id="ht"><tr><td colspan="13" class="py-6 text-center text-slate-600">Waiting for signals...</td></tr></tbody>
</table></div></div>

<div class="mt-3 text-center text-slate-700 text-[9px] pb-4">⚠️ For educational purposes only — not investment advice | TCSM US v5.0 Early Trend Detection</div>
</div>

<script>
const P={},A={},S={},H=[];let cn=false,pf_='all',sf_='all';
const so=io({transports:['websocket','polling'],reconnection:true,reconnectionDelay:1000,reconnectionAttempts:Infinity,timeout:30000});

function uc(){const d=new Date(new Date().toLocaleString("en-US",{timeZone:"America/New_York"}));
document.getElementById('clk').textContent=d.toLocaleTimeString('en-GB',{hour12:false})+' ET';
const h=d.getHours(),m=d.getMinutes(),wd=d.getDay(),t=h*60+m;let s='CLOSED 🔒';
if(wd===0||wd===6)s='WEEKEND 🔒';else if(t<240)s='OVERNIGHT 🌙';else if(t<570)s='PRE-MARKET ☀️';
else if(t<600)s='OPEN ⭐⭐';else if(t<720)s='MORNING ⭐⭐';else if(t<810)s='MIDDAY ⭐';
else if(t<930)s='AFTERNOON ⭐';else if(t<960)s='POWER HOUR ⭐⭐⭐';else if(t<1200)s='AFTER-HOURS 🌆';
document.getElementById('ses').textContent=s;}setInterval(uc,1000);uc();

function lo(m,t='info'){const e=document.getElementById('lg');
const c={signal:'text-emerald-400',error:'text-red-400',info:'text-slate-400',early:'text-violet-400'};
const d=document.createElement('div');d.className=(c[t]||'text-slate-400');
d.textContent=`[${new Date().toLocaleTimeString('en-GB',{timeZone:'America/New_York',hour12:false})}] ${m}`;
e.insertBefore(d,e.firstChild);while(e.children.length>60)e.removeChild(e.lastChild);}

function fP(c){pf_=c;document.querySelectorAll('.pf').forEach(b=>{b.classList.toggle('bg-cyan-600/80',b.dataset.c===c);b.classList.toggle('bg-slate-700/60',b.dataset.c!==c)});rP();}
function fS(c){sf_=c;document.querySelectorAll('.sf').forEach(b=>{b.classList.toggle('bg-cyan-600/80',b.dataset.c===c);b.classList.toggle('bg-slate-700/60',b.dataset.c!==c)});rS();}
function sf(v,d=0){return typeof v==='number'&&isFinite(v)?v:d;}
function fp(p){if(p<1)return p.toFixed(4);if(p<10)return p.toFixed(3);return p.toFixed(2);}

function rP(){let h='',n=0;const q=(document.getElementById('search')?.value||'').toUpperCase();
const syms=Object.keys(A).sort((a,b)=>Math.abs(sf(A[b]?.momentum))-Math.abs(sf(A[a]?.momentum)));
for(const s of syms){const a=A[s];if(!a)continue;if(pf_!=='all'&&a.cat!==pf_)continue;if(q&&!s.includes(q))continue;n++;
const cc=sf(a.change_pct)>=0?'text-emerald-400':'text-red-400';const mc=sf(a.momentum)>=0?'text-emerald-400':'text-red-400';
const hs=S[s]?`<span class="w-2 h-2 rounded-full inline-block ${S[s].type==='EARLY TREND'?'bg-violet-400':S[s].direction==='BUY'?'bg-emerald-400':'bg-red-400'} pulse2"></span>`:'';
const tags=[];
if(a.choch_bull||a.choch_bear)tags.push('<span class="text-[8px] px-1 py-0.5 rounded bg-violet-500/20 text-violet-400">CHoCH</span>');
if(a.bos_bull||a.bos_bear)tags.push('<span class="text-[8px] px-1 py-0.5 rounded bg-cyan-500/20 text-cyan-400">BOS</span>');
if(a.at_ob)tags.push('<span class="text-[8px] px-1 py-0.5 rounded bg-amber-500/20 text-amber-400">OB</span>');
const bw=Math.min(Math.abs(sf(a.momentum)),100);
h+=`<div class="gc rounded-xl p-2.5 ${S[s]?(S[s].type==='EARLY TREND'?'border-violet-600/30':'border-cyan-600/20'):''}">
<div class="flex justify-between items-center">
<div class="flex items-center gap-1.5">${hs}<span class="font-bold text-sm">${s}</span><span class="text-[9px] px-1.5 py-0.5 rounded-md bg-slate-800/50 text-slate-500">${a.cat||''}</span></div>
<div class="text-right"><span class="font-bold">$${fp(sf(a.price))}</span> <span class="${cc} text-[10px] font-semibold">${sf(a.change_pct)>=0?'+':''}${sf(a.change_pct).toFixed(2)}%</span></div></div>
<div class="flex justify-between mt-1 text-[9px]"><span class="${mc} font-semibold">Mom:${sf(a.momentum).toFixed(1)}</span>
<span class="text-slate-400">Str:${sf(a.strength)}%</span><span class="text-slate-600">${a.pd_zone||''} ${a.trend||''}</span></div>
<div class="flex gap-1 mt-1">${tags.join('')}</div>
<div class="mt-1 bg-slate-800/40 rounded-full h-1 overflow-hidden"><div class="${sf(a.momentum)>=0?'bg-gradient-to-r from-emerald-600 to-emerald-400':'bg-gradient-to-r from-red-600 to-red-400'} h-full rounded-full transition-all" style="width:${bw}%"></div></div></div>`;}
if(!n)h='<div class="text-slate-600 text-center py-8">Scanning...</div>';
document.getElementById('pr').innerHTML=h;document.getElementById('ps').innerHTML=n>0?`<span class="text-emerald-400 pulse2">● ${n}</span>`:'loading';}

function rEP(){const earlyList=Object.values(S).filter(s=>s.type==='EARLY TREND').sort((a,b)=>sf(b.strength)-sf(a.strength));
document.getElementById('ep').innerHTML=earlyList.length?earlyList.slice(0,8).map(s=>{
const dc=s.direction==='BUY'?'text-emerald-400':'text-red-400';
return`<div class="flex justify-between items-center py-1.5 border-b border-slate-800/20">
<div><span class="font-bold text-[11px] ${dc}">${s.direction==='BUY'?'🟢':'🔴'} ${s.symbol}</span>
<span class="text-[8px] px-1.5 py-0.5 rounded bg-violet-500/15 text-violet-400 ml-1">EARLY</span></div>
<span class="font-mono font-bold text-[11px]">Str:${sf(s.strength)}%</span></div>`}).join(''):'<div class="text-slate-600 text-xs py-3 text-center">Scanning for Early Trends...</div>';}

function rT(){const arr=Object.values(A).sort((a,b)=>Math.abs(sf(b.momentum))-Math.abs(sf(a.momentum))).slice(0,8);
document.getElementById('tm').innerHTML=arr.map(a=>{const mc=sf(a.momentum)>=0?'text-emerald-400':'text-red-400';
return`<div class="flex justify-between items-center py-1 border-b border-slate-800/20"><span class="font-bold text-[11px]">${a.symbol}</span>
<span class="${mc} font-mono font-bold text-[11px]">${sf(a.momentum)>=0?'+':''}${sf(a.momentum).toFixed(1)}</span></div>`}).join('')||'<div class="text-slate-600 text-xs py-3 text-center">Loading...</div>';}

function rHM(){const sectors={};
Object.values(A).forEach(a=>{const cat=a.cat||'Other';if(!sectors[cat])sectors[cat]={sum:0,cnt:0,buy:0,sell:0};
sectors[cat].sum+=sf(a.momentum);sectors[cat].cnt++;if(S[a.symbol]){S[a.symbol].direction==='BUY'?sectors[cat].buy++:sectors[cat].sell++;}});
document.getElementById('hm').innerHTML=Object.entries(sectors).map(([k,v])=>{const avg=(v.sum/v.cnt).toFixed(1);
const c=avg>=0?'from-emerald-950/40 to-emerald-900/20 border-emerald-700/20 text-emerald-400':'from-red-950/40 to-red-900/20 border-red-700/20 text-red-400';
return`<div class="bg-gradient-to-br ${c} border rounded-xl p-2"><div class="font-bold text-[10px]">${k}</div>
<div class="text-xs font-mono font-bold">${avg>=0?'+':''}${avg}</div><div class="text-[9px] text-slate-500">${v.cnt} tickers ${v.buy}B/${v.sell}S</div></div>`}).join('');}

function rS(){let list=Object.values(S).sort((a,b)=>{
if(a.type==='EARLY TREND'&&b.type!=='EARLY TREND')return -1;if(b.type==='EARLY TREND'&&a.type!=='EARLY TREND')return 1;
return sf(b.strength)-sf(a.strength);});
if(sf_==='EARLY')list=list.filter(s=>s.type==='EARLY TREND');else if(sf_==='STRONG')list=list.filter(s=>s.type==='STRONG');
else if(sf_==='BUY')list=list.filter(s=>s.direction==='BUY');else if(sf_==='SELL')list=list.filter(s=>s.direction==='SELL');
if(!list.length){document.getElementById('sg').innerHTML=`<div class="glass rounded-2xl p-10 text-center text-slate-500"><div class="text-4xl mb-3">🇺🇸</div><div class="text-sm">${Object.keys(S).length?'No signals match filter':'Scanning...'}</div></div>`;document.getElementById('si').textContent=Object.keys(S).length;return;}
document.getElementById('si').textContent=Object.keys(S).length;
document.getElementById('sg').innerHTML=list.slice(0,15).map(s=>{const ib=s.direction==='BUY';const dc=ib?'text-emerald-400':'text-red-400';
const isEarly=s.type==='EARLY TREND';const cls=isEarly?'se':(ib?'sb':'ss');
const tc=isEarly?'bg-gradient-to-r from-violet-500 to-purple-500':s.type==='STRONG'?'bg-gradient-to-r from-amber-500 to-orange-500':s.type==='CONF'?'bg-gradient-to-r from-cyan-500 to-blue-500':'bg-slate-600';
const sw=Math.min(sf(s.strength),100);const scc=sw>=75?'bg-gradient-to-r from-emerald-500 to-emerald-400':sw>=50?'bg-gradient-to-r from-amber-500 to-yellow-400':'bg-slate-600';
const dT=ib?'BUY':'SELL';
const smcTags=[];
if(s.choch)smcTags.push('<span class="tag bg-violet-500/15 text-violet-400 border border-violet-500/20">🔄 CHoCH</span>');
if(s.bos)smcTags.push('<span class="tag bg-cyan-500/15 text-cyan-400 border border-cyan-500/20">📈 BOS</span>');
if(s.at_ob)smcTags.push('<span class="tag bg-amber-500/15 text-amber-400 border border-amber-500/20">🧱 OB</span>');
if(s.in_fvg)smcTags.push('<span class="tag bg-pink-500/15 text-pink-400 border border-pink-500/20">📐 FVG</span>');
if(s.pd_zone==='DISCOUNT'&&ib)smcTags.push('<span class="tag bg-emerald-500/15 text-emerald-400 border border-emerald-500/20">💰 Discount</span>');
if(s.pd_zone==='PREMIUM'&&!ib)smcTags.push('<span class="tag bg-red-500/15 text-red-400 border border-red-500/20">💸 Premium</span>');
return`<div class="glass ${cls} rounded-2xl p-4 slideUp"><div class="flex justify-between items-start mb-2"><div>
<div class="flex items-center gap-2 flex-wrap"><span class="text-lg font-black ${dc}">${ib?'🟢':'🔴'} ${s.symbol}</span>
<span class="px-2.5 py-0.5 rounded-lg text-[10px] font-bold text-white ${tc}">${s.type} ${dT}</span>
<span class="tag bg-slate-800/60 text-slate-400 border border-slate-700/30">${s.sector||''}</span></div>
<div class="text-[10px] text-slate-500 mt-1">Mom:${sf(s.momentum).toFixed(1)} • Conf:${sf(s.conf)}/3 • ${s.ms_trend||''} • RSI:${sf(s.rsi_v)}</div>
<div class="flex flex-wrap gap-1 mt-1">${smcTags.join('')}</div></div>
<div class="text-right"><div class="w-12 h-12 rounded-full flex items-center justify-center font-black text-[13px] ${sw>=75?'text-emerald-400 border-emerald-500/30 bg-emerald-500/10':sw>=50?'text-amber-400 border-amber-500/30 bg-amber-500/10':'text-slate-400 border-slate-500/30 bg-slate-500/10'} border">${sf(s.strength)}%</div>
<div class="text-[9px] text-slate-600 mt-1">${s.timestamp||''}</div></div></div>
<div class="grid grid-cols-5 gap-2 mb-2">
<div class="bg-slate-900/50 rounded-xl p-2 text-center border border-slate-800/30"><div class="text-[9px] text-slate-500">Entry</div><div class="font-bold text-sm mt-0.5">$${fp(sf(s.entry))}</div></div>
<div class="bg-red-950/30 rounded-xl p-2 text-center border border-red-800/20"><div class="text-[9px] text-red-400">SL</div><div class="font-bold text-sm text-red-400 mt-0.5">$${fp(sf(s.sl))}</div></div>
<div class="bg-emerald-950/20 rounded-xl p-2 text-center border border-emerald-800/20"><div class="text-[9px] text-emerald-400">TP1</div><div class="font-bold text-sm text-emerald-400 mt-0.5">$${fp(sf(s.tp1))}</div></div>
<div class="bg-emerald-950/25 rounded-xl p-2 text-center border border-emerald-700/25"><div class="text-[9px] text-emerald-300">TP2</div><div class="font-bold text-sm text-emerald-300 mt-0.5">$${fp(sf(s.tp2))}</div></div>
<div class="bg-emerald-950/30 rounded-xl p-2 text-center border border-emerald-600/30"><div class="text-[9px] text-emerald-200">TP3</div><div class="font-bold text-sm text-emerald-200 mt-0.5">$${fp(sf(s.tp3))}</div></div></div>
<div class="flex items-center gap-3 mb-2"><span class="text-[10px] text-slate-500">Strength</span>
<div class="flex-1 bg-slate-800/40 rounded-full h-2 overflow-hidden"><div class="${scc} h-full rounded-full transition-all" style="width:${sw}%"></div></div>
<span class="text-slate-400 text-[10px]">R:R <b>${sf(s.rr)}:1</b></span>
<span class="text-slate-400 text-[10px]">Risk <b>${sf(s.risk_pct)}%</b></span>
<span class="text-[10px] font-bold ${(s.mtf_align||'').includes('BUY')?'text-emerald-400':(s.mtf_align||'').includes('SELL')?'text-red-400':'text-slate-500'}">${s.mtf_align||''}</span></div>
<div class="flex flex-wrap gap-1">${(s.reasons||[]).slice(0,12).map(r=>`<span class="tag bg-slate-800/60 text-slate-300 border border-slate-700/25">${r}</span>`).join('')}</div></div>`}).join('');}

function rH(){if(!H.length){document.getElementById('ht').innerHTML='<tr><td colspan="13" class="py-6 text-center text-slate-600">Waiting for signals...</td></tr>';return;}
document.getElementById('hc').textContent=H.length+' signals';
document.getElementById('ht').innerHTML=H.slice(0,60).map(s=>{const dc=s.direction==='BUY'?'text-emerald-400 bg-emerald-500/10':'text-red-400 bg-red-500/10';
const tc=s.type==='EARLY TREND'?'text-violet-400':s.type==='STRONG'?'text-amber-400':s.type==='CONF'?'text-cyan-400':'text-slate-400';
const struct=[];if(s.choch)struct.push('CHoCH');if(s.bos)struct.push('BOS');if(s.at_ob)struct.push('OB');
return`<tr class="border-t border-slate-800/20 hover:bg-slate-800/15">
<td class="px-2 py-2 text-slate-500 text-[10px]">${s.timestamp||''}</td>
<td class="px-2 py-2 font-bold text-center">${s.symbol}</td>
<td class="px-2 py-2 text-center ${tc} font-bold">${s.type}</td>
<td class="px-2 py-2 text-center"><span class="px-2 py-0.5 rounded-md ${dc} font-bold text-[10px]">${s.direction}</span></td>
<td class="px-2 py-2 text-right font-mono">$${fp(sf(s.entry))}</td><td class="px-2 py-2 text-right font-mono text-red-400">$${fp(sf(s.sl))}</td>
<td class="px-2 py-2 text-right font-mono text-emerald-400">$${fp(sf(s.tp1))}</td><td class="px-2 py-2 text-right font-mono text-emerald-300">$${fp(sf(s.tp2))}</td>
<td class="px-2 py-2 font-bold text-center">${sf(s.rr)}:1</td>
<td class="px-2 py-2 text-center"><span class="px-1.5 py-0.5 rounded-md ${sf(s.strength)>=75?'bg-emerald-500/15 text-emerald-400':sf(s.strength)>=50?'bg-amber-500/15 text-amber-400':'bg-slate-700/50 text-slate-400'} font-bold">${sf(s.strength)}%</span></td>
<td class="px-2 py-2 text-center font-mono font-bold ${sf(s.momentum)>=0?'text-emerald-400':'text-red-400'}">${sf(s.momentum)>=0?'+':''}${sf(s.momentum).toFixed(1)}</td>
<td class="px-2 py-2 text-center text-[10px] text-violet-400 font-bold">${struct.join(' ')}</td>
<td class="px-2 py-2 text-center text-[10px]">${s.pd_zone||''}</td></tr>`}).join('');}

function sA(s){const isEarly=s.type==='EARLY TREND';
document.getElementById('ai').textContent=isEarly?'🌅':(s.direction==='BUY'?'🟢':'🔴');
document.getElementById('at').textContent=`${s.type} ${s.direction} — ${s.symbol} @ $${fp(sf(s.entry))}`;
document.getElementById('ax').textContent=`SL:$${fp(sf(s.sl))} TP2:$${fp(sf(s.tp2))} Str:${sf(s.strength)}% ${s.choch?'CHoCH':''} ${s.at_ob?'OB':''}`;
document.getElementById('al').classList.remove('hidden');
try{const c=new(window.AudioContext||window.webkitAudioContext)();const o=c.createOscillator();const g=c.createGain();o.connect(g);g.connect(c.destination);
o.frequency.value=isEarly?1047:(s.direction==='BUY'?880:660);g.gain.value=0.06;o.start();o.stop(c.currentTime+0.15);}catch(e){}
setTimeout(()=>document.getElementById('al').classList.add('hidden'),15000);}

function loadState(d){if(!d)return;if(d.prices)Object.assign(P,d.prices);
if(d.signals){for(const[k,v]of Object.entries(d.signals))S[k]=v;}
if(d.analysis){for(const[k,v]of Object.entries(d.analysis))A[k]=v;}
if(d.history){const ids=new Set(H.map(h=>h.id));const ni=(d.history||[]).filter(h=>!ids.has(h.id));H.unshift(...ni);if(H.length===0&&d.history.length>0)H.push(...d.history);}
if(d.stats){document.getElementById('bu').textContent=d.stats.buy||0;document.getElementById('se').textContent=d.stats.sell||0;
document.getElementById('st2').textContent=d.stats.strong||0;document.getElementById('ea').textContent=d.stats.early||0;}
document.getElementById('scc').textContent=d.scan_count||0;rP();rS();rH();rT();rHM();rEP();}

function doRefresh(){lo('Refreshing...','info');fetch('/api/signals').then(r=>r.json()).then(d=>{
if(d.signals){for(const[k,v]of Object.entries(d.signals))S[k]=v;}if(d.history){H.length=0;H.push(...d.history);}
if(d.stats){document.getElementById('bu').textContent=d.stats.buy||0;document.getElementById('se').textContent=d.stats.sell||0;
document.getElementById('st2').textContent=d.stats.strong||0;document.getElementById('ea').textContent=d.stats.early||0;}
rS();rH();rEP();lo(`Refreshed: ${Object.keys(S).length} signals`,'info');}).catch(e=>lo('Failed: '+e,'error'));}
setInterval(()=>{if(Object.keys(A).length===0)doRefresh();},30000);

so.on('connect',()=>{cn=true;document.getElementById('st').innerHTML='<span class="text-emerald-400 pulse2">● LIVE</span>';lo('Connected','info');});
so.on('disconnect',()=>{cn=false;document.getElementById('st').innerHTML='<span class="text-red-400">● Offline</span>';lo('Disconnected','error');});
so.on('reconnect',()=>{lo('Reconnected','info');so.emit('request_state');});
so.on('init',(d)=>{lo(`Init: ${Object.keys(d.analysis||{}).length} stocks`,'info');loadState(d);fP('all');});
so.on('full_sync',(d)=>{lo(`Sync: ${Object.keys(d.analysis||{}).length} stocks`,'info');loadState(d);});
so.on('price_update',(d)=>{if(!d?.symbol)return;P[d.symbol]=d.data;if(d.analysis)A[d.symbol]=d.analysis;rP();rT();rHM();});
so.on('new_signal',(d)=>{if(!d?.signal)return;S[d.symbol]=d.signal;if(!H.find(h=>h.id===d.signal.id))H.unshift(d.signal);rS();rH();rEP();sA(d.signal);
const isE=d.signal.type==='EARLY TREND';
lo(`${isE?'🌅':'🎯'} ${d.signal.type} ${d.signal.direction} ${d.symbol} @$${fp(sf(d.signal.entry))} Str:${sf(d.signal.strength)}%`,isE?'early':'signal');});
so.on('scan_update',(d)=>{if(!d)return;document.getElementById('scc').textContent=d.scan_count||0;document.getElementById('ls').textContent=d.last_scan||'--';document.getElementById('ss2').textContent=d.status||'OK';
if(d.stats){document.getElementById('bu').textContent=d.stats.buy||0;document.getElementById('se').textContent=d.stats.sell||0;
document.getElementById('st2').textContent=d.stats.strong||0;document.getElementById('ea').textContent=d.stats.early||0;}});
</script></body></html>'''


# ═══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════════════════════
@app.route('/')
def index(): return redirect(url_for('login') if 'user' not in session else url_for('dashboard'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user' in session: return redirect(url_for('dashboard'))
    if request.method == 'POST':
        u = request.form.get('u', '').strip().lower(); p = request.form.get('p', '')
        if users.verify(u, p):
            session.permanent = True; session['user'] = u; return redirect(url_for('dashboard'))
        flash('Invalid username or password', 'error')
    return render_template_string(LOGIN_HTML)

@app.route('/logout')
def logout(): session.clear(); return redirect(url_for('login'))

@app.route('/dashboard')
@login_req
def dashboard(): return render_template_string(DASH_HTML)

@app.route('/health')
def health(): return jsonify(sanitize({'status': 'ok', 'version': '5.0-us-early-trend', 'time': dts(), 'scans': store.scan_count, 'signals': len(store.signals)}))

@app.route('/api/signals')
@login_req
def api_signals(): return jsonify(sanitize({'signals': store.signals, 'history': store.history[:200], 'stats': store.stats}))

@socketio.on('connect')
def ws_conn():
    store.connected += 1
    try: emit('init', store.get_state())
    except: pass

@socketio.on('disconnect')
def ws_disc(): store.connected = max(0, store.connected - 1)

@socketio.on('request_state')
def ws_req():
    try: emit('full_sync', store.get_state())
    except: pass


# ═══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
port = int(os.environ.get('PORT', 8000))
gevent.spawn(scanner)

if __name__ == '__main__':
    print("=" * 65)
    print("  🇺🇸 TCSM US STOCK PRO v5.0 — EARLY TREND DETECTION")
    print("  Strategy: CHoCH/BOS + OB + FVG + Triple Confluence + SmartMom")
    print(f"  🕐 Time: {dts()} ET | Stocks: {len(C.STOCKS)}")
    print(f"  🌐 Port: {port} | Login: admin / admin123")
    print("=" * 65)
    socketio.run(app, host='0.0.0.0', port=port, debug=False, use_reloader=False)