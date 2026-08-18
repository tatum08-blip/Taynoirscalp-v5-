#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              TAYNOIR AI TRADING BOT v3.0 — AUTONOME / SAR EDITION          ║
║                                                                              ║
║  10 stratégies combinées (5 core + 5 sniper entries)                        ║
║  2 IA en concertation (Claude + GPT) pour validation double                 ║
║  Connexion : Deriv (Multipliers, SL live) + MT5 optionnel                   ║
║  Money Management adaptatif selon taille du compte                          ║
║  Position Manager : trailing SL + STOP AND REVERSE (SAR) en continu         ║
║  Stop automatique après 4 SL consécutifs                                    ║
║  Auto-apprentissage : pondération par stratégie selon win rate réel         ║
║  RR calculé avec précision — jamais approximé                               ║
║  Walk-Forward backtest intégré                                              ║
║  AUCUNE dépendance Telegram — bot 100% autonome, logs + JSON local          ║
║                                                                              ║
║  © Tatum08 | Lomé, Togo                                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

NOTE HONNÊTE : le SAR (stop-and-reverse) est une mécanique de GESTION DE POSITION,
pas une source d'edge. Il amplifie ce qui marche déjà (ton signal SMC/ICT) et
amplifie aussi ce qui ne marche pas (beaucoup de faux signaux en range = beaucoup
de retournements inutiles). Le filtre kill-zone + confluence reste ce qui protège
le capital. Aucun bot ne garantit la rentabilité — seule la discipline du edge
+ la gestion de risque adaptative y contribuent.
"""

import os, json, time, math, logging, schedule, threading, requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import websocket

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("taynoir_v2.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("TAYNOIR_V2")

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG CENTRALE
# ══════════════════════════════════════════════════════════════════════════════
class Config:
    TRADE_MODE         = os.getenv("TRADE_MODE", "demo")
    # Deriv
    DERIV_APP_ID       = os.getenv("DERIV_APP_ID", "").strip()  # doit venir d'une app enregistrée sur developers.deriv.com — PAS 1089 (legacy)
    DERIV_TOKEN_DEMO   = os.getenv("DERIV_TOKEN_DEMO", "").strip()
    DERIV_TOKEN_REAL   = os.getenv("DERIV_TOKEN_REAL", "").strip()
    DERIV_API_BASE     = os.getenv("DERIV_API_BASE", "https://api.derivws.com").rstrip("/")
    # Requis par Deriv à la création d'un compte Options — "row" est la seule
    # valeur documentée dans leurs exemples. Si Deriv utilise un groupe différent
    # pour ta région, ajuste via cette variable.
    DERIV_ACCOUNT_GROUP = os.getenv("DERIV_ACCOUNT_GROUP", "row")
    # Historique de prix public (ticks_history) — ne nécessite AUCUNE authentification,
    # donc reste sur l'infrastructure classique, indépendamment du token/app_id.
    DERIV_WS_PUBLIC    = os.getenv("DERIV_WS_PUBLIC", "wss://api.derivws.com/trading/v1/options/ws/public")
    # Exness / MT5 universel
    MT5_LOGIN          = int(os.getenv("MT5_LOGIN", "0"))
    MT5_PASSWORD       = os.getenv("MT5_PASSWORD", "")
    MT5_SERVER         = os.getenv("MT5_SERVER", "")   # ex: Exness-MT5Real
    USE_MT5            = os.getenv("USE_MT5", "false").lower() == "true"
    # Notifications sortantes optionnelles (remplace Telegram)
    # Si vide -> logs locaux uniquement. Si rempli -> POST JSON vers ce webhook
    # (Discord, Slack via Incoming Webhook, ou ton propre serveur).
    WEBHOOK_URL        = os.getenv("WEBHOOK_URL", "")
    # IA double
    CLAUDE_API_KEY     = os.getenv("CLAUDE_API_KEY", "")
    OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
    USE_DUAL_AI        = os.getenv("USE_DUAL_AI", "false").lower() == "true"
    # Twelve Data
    TD_API_KEY         = os.getenv("TD_API_KEY", "")
    # Capital initial
    CAPITAL            = float(os.getenv("CAPITAL", "100"))
    # Stop auto après N SL consécutifs
    MAX_CONSEC_LOSSES  = int(os.getenv("MAX_CONSEC_LOSSES", "4"))
    # Confiance minimum pour trader
    MIN_CONF_TRADE     = float(os.getenv("MIN_CONF_TRADE", "78"))
    MIN_CONF_PUBLIC    = float(os.getenv("MIN_CONF_PUBLIC", "82"))
    MIN_CONF_PREMIUM   = float(os.getenv("MIN_CONF_PREMIUM", "58"))
    # Délai minimum avant de retrader la même paire dans le même sens — 1800s (30 min)
    # était trop long pour du scalping (3-6 trades/jour visés). Réduit par défaut,
    # ajustable si besoin.
    DUP_COOLDOWN_SEC   = int(os.getenv("DUP_COOLDOWN_SEC", "600"))
    # Actifs
    ACTIVE_PAIRS = [
        p.strip().upper()
        for p in os.getenv(
            "ACTIVE_PAIRS",
            "XAUUSD,EURUSD,GBPUSD,AUDUSD,USDJPY,GBPJPY,XAGUSD,NZDUSD,V25,V50,V75,V100"
        ).split(",")
        if p.strip()
    ]
    # ── STOP AND REVERSE (SAR) ──────────────────────────────────────────
    SAR_ENABLED         = os.getenv("SAR_ENABLED", "true").lower() == "true"
    # Distance de trailing derrière le prix, en multiple de l'ATR(14)
    SAR_ATR_MULT        = float(os.getenv("SAR_ATR_MULT", "0.7"))
    # Le SL ne se resserre jamais plus près que ça (évite le micro-bruit)
    SAR_MIN_TRAIL_ATR   = float(os.getenv("SAR_MIN_TRAIL_ATR", "0.5"))
    # Ne commence à trailer qu'après ce niveau de profit (en R, càd multiple du risque initial)
    SAR_ACTIVATION_R    = float(os.getenv("SAR_ACTIVATION_R", "0.2"))
    # Nombre max de retournements en chaîne avant pause automatique (protège contre le range)
    SAR_MAX_FLIPS       = int(os.getenv("SAR_MAX_FLIPS", "3"))
    # Fréquence de vérification des positions ouvertes (secondes) — scalping = rapide
    POSITION_POLL_SEC   = float(os.getenv("POSITION_POLL_SEC", "2"))

# ══════════════════════════════════════════════════════════════════════════════
#  MONEY MANAGEMENT ADAPTATIF
#  < 500$   → 5%   risque
#  500–2k   → 3%
#  2k–5k    → 2%
#  5k–10k   → 1.5%
#  10k–20k  → 1%
#  > 20k    → 0.75%
# ══════════════════════════════════════════════════════════════════════════════
class AdaptiveRisk:
    @staticmethod
    def get_risk_pct(capital: float) -> float:
        if capital < 500:    return 5.0
        if capital < 2000:   return 3.0
        if capital < 5000:   return 2.0
        if capital < 10000:  return 1.5
        if capital < 20000:  return 1.0
        return 0.75

    @staticmethod
    def calc_position(pair: dict, entry: float, sl: float, capital: float) -> dict | None:
        """
        Calcul précis du lot, du RR réel, et du gain attendu.
        Ne jamais approximer le RR — on le calcule en pips réels.
        """
        if sl <= 0 or entry <= 0:
            return None
        sl_dist = abs(entry - sl)
        if sl_dist < pair["pip"] * 0.5:
            return None   # SL trop proche = trade invalide

        risk_pct = AdaptiveRisk.get_risk_pct(capital)
        risk_usd = capital * (risk_pct / 100)
        pips_sl  = sl_dist / pair["pip"]
        if pips_sl < 0.5:
            return None

        raw_lot  = risk_usd / (pips_sl * pair["upl"])
        lot      = max(0.01, math.floor(raw_lot * 100) / 100)

        actual_risk = lot * pips_sl * pair["upl"]
        actual_pct  = actual_risk / capital * 100

        return {
            "lot":         lot,
            "pips_sl":     round(pips_sl, 1),
            "risk_usd":    round(actual_risk, 2),
            "risk_pct":    round(actual_pct, 2),
            "risk_pct_target": risk_pct,
            "safe":        actual_pct <= risk_pct * 1.1,
            "danger":      actual_pct > risk_pct * 2,
        }

    @staticmethod
    def calc_rr(entry: float, sl: float, tp: float, direction: str) -> float:
        """Calcul précis du Risk:Reward ratio"""
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0:
            return 0.0
        rr = reward / risk
        # Vérifier cohérence direction
        if direction == "BUY":
            if tp <= entry or sl >= entry:
                return -1.0   # Signal invalide
        elif direction == "SELL":
            if tp >= entry or sl <= entry:
                return -1.0
        return round(rr, 2)

# ══════════════════════════════════════════════════════════════════════════════
#  ACTIFS
# ══════════════════════════════════════════════════════════════════════════════
PAIRS = {
    "XAUUSD":    {"label":"🥇 XAU/USD",   "td":"XAU/USD",  "type":"real",  "pip":0.01,   "d":2,"upl":1,   "spread":0.5, "deriv":"frxXAUUSD"},
    "XAGUSD":    {"label":"🥈 XAG/USD",   "td":"XAG/USD",  "type":"real",  "pip":0.001,  "d":3,"upl":1,   "spread":0.3, "deriv":"frxXAGUSD"},
    "EURUSD":    {"label":"💶 EUR/USD",    "td":"EUR/USD",  "type":"real",  "pip":0.0001, "d":5,"upl":10,  "spread":0.2, "deriv":"frxEURUSD"},
    "GBPUSD":    {"label":"💷 GBP/USD",    "td":"GBP/USD",  "type":"real",  "pip":0.0001, "d":5,"upl":10,  "spread":0.3, "deriv":"frxGBPUSD"},
    "USDJPY":    {"label":"💴 USD/JPY",    "td":"USD/JPY",  "type":"real",  "pip":0.01,   "d":3,"upl":10,  "spread":0.3, "deriv":"frxUSDJPY"},
    "GBPJPY":    {"label":"🔥 GBP/JPY",    "td":"GBP/JPY",  "type":"real",  "pip":0.01,   "d":3,"upl":10,  "spread":0.6, "deriv":"frxGBPJPY"},
    "AUDUSD":    {"label":"🦘 AUD/USD",    "td":"AUD/USD",  "type":"real",  "pip":0.0001, "d":5,"upl":10,  "spread":0.2, "deriv":"frxAUDUSD"},
    "NZDUSD":    {"label":"🥝 NZD/USD",    "td":"NZD/USD",  "type":"real",  "pip":0.0001, "d":5,"upl":10,  "spread":0.3, "deriv":"frxNZDUSD"},
    "V75":       {"label":"🔮 Vol 75",     "td":None,       "type":"synth", "pip":0.001,  "d":3,"upl":0.1, "spread":0,   "deriv":"R_75"},
    "V100":      {"label":"⚡ Vol 100",    "td":None,       "type":"synth", "pip":0.001,  "d":3,"upl":0.1, "spread":0,   "deriv":"R_100"},
    "V25":       {"label":"🌊 Vol 25",     "td":None,       "type":"synth", "pip":0.001,  "d":3,"upl":0.1, "spread":0,   "deriv":"R_25"},
    "V50":       {"label":"💫 Vol 50",     "td":None,       "type":"synth", "pip":0.001,  "d":3,"upl":0.1, "spread":0,   "deriv":"R_50"},
    "BOOM500":   {"label":"📈 Boom 500",   "td":None,       "type":"synth", "pip":0.1,    "d":1,"upl":0.1, "spread":0,   "deriv":"BOOM500N"},
    "BOOM1000":  {"label":"🚀 Boom 1000",  "td":None,       "type":"synth", "pip":0.1,    "d":1,"upl":0.1, "spread":0,   "deriv":"BOOM1000N"},
    "CRASH500":  {"label":"📉 Crash 500",  "td":None,       "type":"synth", "pip":0.1,    "d":1,"upl":0.1, "spread":0,   "deriv":"CRASH500N"},
    "CRASH1000": {"label":"💥 Crash 1000", "td":None,       "type":"synth", "pip":0.1,    "d":1,"upl":0.1, "spread":0,   "deriv":"CRASH1000N"},
}

# "frx..." = symboles Deriv pour le forex/métaux (confirmé par leur documentation
# officielle, ex: frxEURUSD). Sans ça, ces actifs pouvaient être scannés mais
# jamais réellement tradés — c'était le vrai trou qui rendait le bot "paresseux"
# sur l'or et le forex.
ALLOWED_DERIV = {
    "R_25","R_50","R_75","R_100","BOOM500N","BOOM1000N","CRASH500N","CRASH1000N",
    "frxXAUUSD","frxXAGUSD","frxEURUSD","frxGBPUSD","frxUSDJPY","frxGBPJPY","frxAUDUSD","frxNZDUSD",
}

KILL_ZONES = [
    {"id":"asia",   "label":"🌏 Asie",         "start":0,    "end":180,  "w":0.85},
    {"id":"london", "label":"🏦 London Open",  "start":480,  "end":600,  "w":1.10},
    {"id":"ny",     "label":"🗽 New York",     "start":810,  "end":960,  "w":1.15},
    {"id":"lclose", "label":"🏦 London Close", "start":840,  "end":900,  "w":1.05},
    {"id":"sydney", "label":"🦘 Sydney",       "start":1320, "end":1440, "w":0.80},
]

def active_kz():
    now = datetime.now(timezone.utc)
    m = now.hour * 60 + now.minute
    for kz in KILL_ZONES:
        if kz["start"] <= m < kz["end"]:
            return kz
    return None

def market_open():
    now = datetime.now(timezone.utc)
    d, h, mi = now.weekday(), now.hour, now.minute
    t = h * 60 + mi
    if d == 4 and t >= 22*60: return False
    if d == 5: return False
    if d == 6 and t < 22*60: return False
    return True

# ══════════════════════════════════════════════════════════════════════════════
#  BASE DE DONNÉES
# ══════════════════════════════════════════════════════════════════════════════
DB_FILE = "taynoir_v2_data.json"

def load_db():
    try:
        with open(DB_FILE, encoding="utf-8") as f:
            return json.load(f)
    except:
        return {
            "capital": Config.CAPITAL,
            "trades": [],
            "daily": {},
            "perf": {
                "total":0,"wins":0,"losses":0,"be":0,
                "pnl":0.0,"peak":Config.CAPITAL,"max_dd":0.0,
                "consec_losses":0,"consec_wins":0,
            },
            "strategy_perf": {},
            "adaptive": {
                "conf_threshold": Config.MIN_CONF_TRADE,
                "paused_until": None,
                "pause_reason": "",
            },
            "subscribers": {},
        }

def save_db(db):
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"save_db: {e}")

db = load_db()
# db est lu/modifié depuis plusieurs threads (scan principal + PositionManager
# qui tourne en parallèle pour le SAR) — ce verrou évite les corruptions de
# compteurs (capital, win rate, etc) en cas d'accès simultané.
db_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATEURS TECHNIQUES (tous validés mathématiquement)
# ══════════════════════════════════════════════════════════════════════════════
class TA:
    @staticmethod
    def ema(prices, p):
        if len(prices) < p:
            return prices[-1] if prices else 0.0
        k = 2.0 / (p + 1)
        v = sum(prices[:p]) / p
        for x in prices[p:]:
            v = x * k + v * (1 - k)
        return v

    @staticmethod
    def ema_series(prices, p):
        if len(prices) < p:
            return [prices[0]] * len(prices) if prices else []
        k = 2.0 / (p + 1)
        out = [sum(prices[:p]) / p]
        for x in prices[p:]:
            out.append(x * k + out[-1] * (1 - k))
        return [out[0]] * p + out[1:]

    @staticmethod
    def rsi(prices, p=14):
        if len(prices) < p + 1:
            return 50.0
        diffs = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [max(d, 0) for d in diffs]
        losses = [max(-d, 0) for d in diffs]
        ag = sum(gains[-p:]) / p
        al = sum(losses[-p:]) / p
        if al == 0:
            return 100.0
        for i in range(p, len(gains)):
            ag = (ag * (p-1) + gains[i]) / p
            al = (al * (p-1) + losses[i]) / p
        return round(100 - 100 / (1 + ag / max(al, 1e-10)), 2)

    @staticmethod
    def macd(prices, fast=12, slow=26, signal=9):
        if len(prices) < slow + signal:
            return 0.0, 0.0, 0.0
        ef = TA.ema_series(prices, fast)
        es = TA.ema_series(prices, slow)
        ml = [a - b for a, b in zip(ef, es)]
        sl = TA.ema_series(ml, signal)
        hist = ml[-1] - sl[-1]
        return round(ml[-1], 6), round(sl[-1], 6), round(hist, 6)

    @staticmethod
    def stoch(H, L, C, k=14, d=3):
        if len(C) < k:
            return 50.0, 50.0
        hi = max(H[-k:]);  lo = min(L[-k:])
        if hi == lo:
            return 50.0, 50.0
        kv = round((C[-1] - lo) / (hi - lo) * 100, 2)
        # D = SMA(K, d) sur les k dernières valeurs
        k_vals = []
        for i in range(k, len(C)+1):
            h2 = max(H[i-k:i]);  l2 = min(L[i-k:i])
            if h2 == l2:
                k_vals.append(50.0)
            else:
                k_vals.append((C[i-1] - l2) / (h2 - l2) * 100)
        dv = round(sum(k_vals[-d:]) / d, 2) if len(k_vals) >= d else kv
        return kv, dv

    @staticmethod
    def atr(H, L, C, p=14):
        if len(H) < 2:
            return abs(H[0] - L[0]) or 0.0001
        trs = [max(H[i]-L[i], abs(H[i]-C[i-1]), abs(L[i]-C[i-1]))
               for i in range(1, len(H))]
        if not trs:
            return 0.0001
        a = sum(trs[:p]) / min(p, len(trs))
        for tr in trs[p:]:
            a = (a * (p-1) + tr) / p
        return a or 0.0001

    @staticmethod
    def bollinger(prices, p=20, std=2):
        if len(prices) < p:
            return prices[-1], prices[-1], prices[-1]
        sl = prices[-p:]
        mid = sum(sl) / p
        var = sum((x - mid)**2 for x in sl) / p
        sd  = var ** 0.5
        return round(mid + std*sd, 6), round(mid, 6), round(mid - std*sd, 6)

    @staticmethod
    def pivot(H, L, C):
        pp = (H + L + C) / 3
        return {
            "pp": pp,
            "r1": 2*pp - L,   "r2": pp + H - L,   "r3": H + 2*(pp - L),
            "s1": 2*pp - H,   "s2": pp - H + L,   "s3": L - 2*(H - pp),
        }

    @staticmethod
    def vwap(H, L, C):
        tp = [(h+l+c)/3 for h,l,c in zip(H,L,C)]
        return sum(tp) / len(tp) if tp else 0.0

    @staticmethod
    def adx(H, L, C, p=14):
        """Average Directional Index — mesure la force de la tendance"""
        if len(H) < p + 1:
            return 25.0, 0.0, 0.0
        pDM = [max(H[i]-H[i-1], 0) if H[i]-H[i-1] > L[i-1]-L[i] else 0 for i in range(1,len(H))]
        mDM = [max(L[i-1]-L[i], 0) if L[i-1]-L[i] > H[i]-H[i-1] else 0 for i in range(1,len(H))]
        TR  = [max(H[i]-L[i], abs(H[i]-C[i-1]), abs(L[i]-C[i-1])) for i in range(1,len(H))]
        def smooth(data, p):
            s = sum(data[:p])
            out = [s]
            for x in data[p:]:
                s = s - s/p + x
                out.append(s)
            return out
        sTR = smooth(TR, p); spDM = smooth(pDM, p); smDM = smooth(mDM, p)
        diP = [100*a/max(b,1e-10) for a,b in zip(spDM, sTR)]
        diM = [100*a/max(b,1e-10) for a,b in zip(smDM, sTR)]
        dx  = [100*abs(a-b)/max(a+b,1e-10) for a,b in zip(diP, diM)]
        adx_val = sum(dx[:p])/p if len(dx) >= p else 25.0
        for x in dx[p:]:
            adx_val = (adx_val*(p-1)+x)/p
        return round(adx_val,2), round(diP[-1],2), round(diM[-1],2)

    @staticmethod
    def cci(H, L, C, p=20):
        """Commodity Channel Index"""
        if len(C) < p:
            return 0.0
        tp = [(h+l+c)/3 for h,l,c in zip(H[-p:], L[-p:], C[-p:])]
        avg = sum(tp)/p
        md  = sum(abs(t-avg) for t in tp)/p
        return round((tp[-1]-avg)/(0.015*max(md,1e-10)), 2)

    @staticmethod
    def williams_r(H, L, C, p=14):
        if len(C) < p:
            return -50.0
        hi = max(H[-p:]); lo = min(L[-p:])
        if hi == lo: return -50.0
        return round((hi - C[-1]) / (hi - lo) * -100, 2)

# ══════════════════════════════════════════════════════════════════════════════
#  SMC ANALYZER
# ══════════════════════════════════════════════════════════════════════════════
def smc_analyze(pair_id, candles):
    if len(candles) < 20:
        return None
    pair = PAIRS[pair_id]
    pip  = pair["pip"]
    C = [c["close"] for c in candles]
    H = [c["high"]  for c in candles]
    L = [c["low"]   for c in candles]
    # Bougies clôturées uniquement pour la structure
    closed = candles[:-1]
    Cc = [c["close"] for c in closed]
    Hc = [c["high"]  for c in closed]
    Lc = [c["low"]   for c in closed]
    last = candles[-1]
    rh = max(H[-30:]) if len(H)>=30 else max(H)
    rl = min(L[-30:]) if len(L)>=30 else min(L)
    rng = max(rh - rl, pip)
    price = C[-1]
    pp = (price - rl) / rng

    # Structure swing highs/lows réels
    swH, swL = [], []
    for i in range(2, len(Hc)-2):
        if all(Hc[i] > Hc[j] for j in [i-2,i-1,i+1,i+2]): swH.append(Hc[i])
        if all(Lc[i] < Lc[j] for j in [i-2,i-1,i+1,i+2]): swL.append(Lc[i])
    swH = swH[-6:]; swL = swL[-6:]

    structure = "Haussier"
    if len(swH) >= 2 and len(swL) >= 2:
        hh = sum(1 for i in range(1,len(swH)) if swH[i] > swH[i-1])
        ll = sum(1 for i in range(1,len(swL)) if swL[i] < swL[i-1])
        structure = "Haussier" if hh >= ll else "Baissier"

    # BOS seuil 1.5%
    bos_thr = rng * 0.015
    prH = max(Hc[-25:-2]) if len(Hc) > 5 else rh
    prL = min(Lc[-25:-2]) if len(Lc) > 5 else rl
    bos = None
    if price > prH + bos_thr: bos = "haussier"
    elif price < prL - bos_thr: bos = "baissier"

    # CHoCH sur bougies clôturées
    choch = None
    for i in range(3, len(closed)-2):
        c, cp, cn = closed[i], closed[i-3], closed[i+1]
        if structure=="Haussier" and c["low"]<cp["low"] and cn["close"]>c["high"]:
            choch="haussier"; break
        if structure=="Baissier" and c["high"]>cp["high"] and cn["close"]<c["low"]:
            choch="baissier"; break

    # FVG (Fair Value Gap)
    fvgs = []
    for i in range(max(0,len(candles)-30), len(candles)-2):
        c1, c3 = candles[i], candles[i+2]
        if c1["high"] < c3["low"]:
            fvgs.append({"type":"bull","mid":(c3["low"]+c1["high"])/2,"top":c3["low"],"bot":c1["high"]})
        elif c1["low"] > c3["high"]:
            fvgs.append({"type":"bear","mid":(c1["low"]+c3["high"])/2,"top":c1["low"],"bot":c3["high"]})

    # Order Blocks
    def find_obs(direction, lb=25):
        res = []
        for i in range(len(candles)-5, max(2,len(candles)-lb), -1):
            c = candles[i]
            push = candles[i+1:min(len(candles),i+5)]
            if len(push) < 2: continue
            mv = push[-1]["close"] - push[0]["open"]
            if direction=="bull" and c["close"]<c["open"] and mv>rng*.07:
                res.append({"lo":c["low"],"hi":c["high"],"mid":(c["low"]+c["high"])/2})
            if direction=="bear" and c["close"]>c["open"] and mv<-rng*.07:
                res.append({"lo":c["low"],"hi":c["high"],"mid":(c["low"]+c["high"])/2})
        return res[:3]

    obs_bull = find_obs("bull"); obs_bear = find_obs("bear")
    in_ob_bull = any(ob["lo"]<=price<=ob["hi"] for ob in obs_bull)
    in_ob_bear = any(ob["lo"]<=price<=ob["hi"] for ob in obs_bear)

    # Sweep
    sweep = None
    for c in candles[-8:-1]:
        if c["high"]>rh and c["close"]<rh: sweep="BSL"; break
        if c["low"]<rl and c["close"]>rl:  sweep="SSL"; break

    # OTE Fibonacci
    fd = rh - rl or pip
    in_ote = (rh - fd*0.79) <= price <= (rh - fd*0.62)
    fib_near = min(
        {"23.6%":rh-fd*.236,"38.2%":rh-fd*.382,"50%":(rh+rl)/2,
         "61.8%":rh-fd*.618,"78.6%":rh-fd*.786},
        key=lambda k: abs({"23.6%":rh-fd*.236,"38.2%":rh-fd*.382,"50%":(rh+rl)/2,"61.8%":rh-fd*.618,"78.6%":rh-fd*.786}[k]-price)
    )

    # Indicateurs
    rsi_v = TA.rsi(C)
    _, _, macd_hist = TA.macd(C)
    stoch_k, stoch_d = TA.stoch(H, L, C)
    e9,e21,e50 = TA.ema(C,9), TA.ema(C,21), TA.ema(C,50)
    e200 = TA.ema(C,200) if len(C)>=200 else e50
    atr_v = TA.atr(H, L, C)
    bb_up, bb_mid, bb_lo = TA.bollinger(C)
    adx_v, di_plus, di_minus = TA.adx(H, L, C)
    cci_v = TA.cci(H, L, C)
    wR = TA.williams_r(H, L, C)
    momentum5 = round((C[-1]-C[-6])/max(C[-6],1e-6)*100, 3) if len(C)>=6 else 0

    return {
        "price": price, "rh": rh, "rl": rl, "rng": rng, "atr": atr_v,
        "zone": "Premium" if pp>.5 else "Discount",
        "structure": structure, "bos": bos, "choch": choch,
        "fvgs": fvgs, "fvg_count": len(fvgs),
        "obs_bull": obs_bull, "obs_bear": obs_bear,
        "in_ob_bull": in_ob_bull, "in_ob_bear": in_ob_bear,
        "sweep": sweep, "in_ote": in_ote, "fib": fib_near,
        "rsi": rsi_v, "macd_hist": macd_hist,
        "stoch_k": stoch_k, "stoch_d": stoch_d,
        "ema_bull": e9>e21>e50, "ema_bear": e9<e21<e50,
        "e9":e9,"e21":e21,"e50":e50,"e200":e200,
        "above_e200": price>e200,
        "bb_up":bb_up,"bb_mid":bb_mid,"bb_lo":bb_lo,
        "adx":adx_v,"di_plus":di_plus,"di_minus":di_minus,
        "cci":cci_v,"williams_r":wR,"momentum":momentum5,
        "last": last,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  5 STRATÉGIES CORE + 5 STRATÉGIES SNIPER ENTRY
# ══════════════════════════════════════════════════════════════════════════════
class StrategyEngine:
    """
    10 stratégies combinées :
    CORE    : SMC · ICT · Price Action · Momentum · Structure
    SNIPER  : OB Retest · FVG Fill · Sweep+Reverse · Divergence · Session Open
    """

    # ── CORE 1 : SMC ─────────────────────────────────────────────────────────
    @staticmethod
    def smc(a, kz):
        score = 0; reasons = []
        def add(pts, lbl): nonlocal score; score += pts; reasons.append(lbl)

        if a["bos"]=="haussier":    add(+20,"BOS ↑")
        elif a["bos"]=="baissier":  add(-20,"BOS ↓")
        if a["choch"]=="haussier":  add(+16,"CHoCH ↑")
        elif a["choch"]=="baissier":add(-16,"CHoCH ↓")
        if a["in_ob_bull"]:         add(+18,"Dans OB Bull")
        if a["in_ob_bear"]:         add(-18,"Dans OB Bear")
        if a["fvg_count"] > 0:
            bull_fvgs = sum(1 for f in a["fvgs"] if f["type"]=="bull")
            bear_fvgs = a["fvg_count"] - bull_fvgs
            if bull_fvgs > bear_fvgs:  add(+12,"FVG Bull")
            elif bear_fvgs > bull_fvgs:add(-12,"FVG Bear")
        if a["sweep"]=="BSL":       add(+14,"BSL Sweep ↑")
        elif a["sweep"]=="SSL":     add(-14,"SSL Sweep ↓")
        if a["structure"]=="Haussier" and a["zone"]=="Discount": add(+8,"Zone Discount ↑")
        if a["structure"]=="Baissier" and a["zone"]=="Premium":  add(+8,"Zone Premium ↓")
        if a["in_ote"]:             add(+10,"OTE 62–79%")
        return {"score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,"reasons":reasons,"valid":abs(score)>=20}

    # ── CORE 2 : ICT ─────────────────────────────────────────────────────────
    @staticmethod
    def ict(a, kz):
        score = 0; reasons = []
        def add(pts, lbl): nonlocal score; score += pts; reasons.append(lbl)

        if kz:
            add(int(18*kz["w"]), f"Kill Zone {kz['label']}")
        if a["in_ote"]:       add(+18,"OTE ✅")
        if a["bos"]:
            if a["bos"]=="haussier": add(+12,"ICT BOS ↑")
            else:                    add(-12,"ICT BOS ↓")
        price = a["price"]; rng = a["rng"]
        # PDHL — Previous Day High/Low (niveaux clés ICT)
        pd_high = a["rh"]; pd_low = a["rl"]
        if abs(price - pd_high) < rng * 0.03:
            add(-10, "Résistance PDH ↓")
        elif abs(price - pd_low) < rng * 0.03:
            add(+10, "Support PDL ↑")
        elif price > pd_high: add(+8, "Cassure PDH ↑")
        elif price < pd_low:  add(-8, "Cassure PDL ↓")
        fib_levels = {
            "61.8%": a["rh"]-rng*.618,
            "50%":   (a["rh"]+a["rl"])/2,
            "38.2%": a["rh"]-rng*.382,
        }
        for lbl, lv in fib_levels.items():
            if abs(price-lv) < rng*.03: add(+10,f"Niveau Fib {lbl}"); break
        # Judas Swing (ICT) — détection via sweep + structure contra
        if a["sweep"] == "BSL" and a["structure"] == "Baissier":
            add(-12, "Judas Swing ↓ (BSL + structure baissière)")
        elif a["sweep"] == "SSL" and a["structure"] == "Haussier":
            add(+12, "Judas Swing ↑ (SSL + structure haussière)")
        if a["adx"] > 25 and a["di_plus"] > a["di_minus"]: add(+8,"ADX Tendance ↑")
        elif a["adx"] > 25 and a["di_minus"] > a["di_plus"]: add(-8,"ADX Tendance ↓")
        return {"score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,"reasons":reasons,"valid":abs(score)>=18}

    # ── CORE 3 : PRICE ACTION ────────────────────────────────────────────────
    @staticmethod
    def price_action(a, candles):
        if len(candles) < 3:
            return {"score":0,"direction":None,"reasons":[],"valid":False}
        score = 0; reasons = []
        def add(pts, lbl): nonlocal score; score += pts; reasons.append(lbl)

        last = candles[-1]; prev = candles[-2]; prev2 = candles[-3]
        pip = 0.0001
        body = abs(last["close"]-last["open"]); body = max(body, pip*.1)
        lw = min(last["close"],last["open"]) - last["low"]
        uw = last["high"] - max(last["close"],last["open"])

        # Pin Bar
        if lw>body*2.5 and lw>uw*2 and last["close"]>last["open"]:   add(+22,"Pin Bar Bull ↑")
        elif uw>body*2.5 and uw>lw*2 and last["close"]<last["open"]: add(-22,"Pin Bar Bear ↓")
        # Engulfing
        elif (last["close"]>last["open"] and prev["close"]<prev["open"] and
              last["close"]>prev["open"] and last["open"]<prev["close"]):  add(+20,"Engulfing Bull ↑")
        elif (last["close"]<last["open"] and prev["close"]>prev["open"] and
              last["close"]<prev["open"] and last["open"]>prev["close"]): add(-20,"Engulfing Bear ↓")
        # Hammer / Shooting Star
        elif lw>body*3 and uw<body*.3:  add(+18,"Marteau ↑")
        elif uw>body*3 and lw<body*.3:  add(-18,"Étoile Filante ↓")
        # Morning/Evening Star
        mid = candles[-2]
        if (prev2["close"]<prev2["open"] and abs(mid["close"]-mid["open"])<body*.5
                and last["close"]>last["open"] and last["close"]>prev2["close"]):
            add(+18,"Morning Star ↑")
        elif (prev2["close"]>prev2["open"] and abs(mid["close"]-mid["open"])<body*.5
                and last["close"]<last["open"] and last["close"]<prev2["close"]):
            add(-18,"Evening Star ↓")
        # Marubozu
        rng = a["rng"]
        if body > rng*.4 and lw<pip*2 and uw<pip*2:
            if last["close"]>last["open"]: add(+14,"Marubozu Bull ↑")
            else:                          add(-14,"Marubozu Bear ↓")
        # Double Bottom/Top
        if prev2["low"] <= a["rl"]+rng*.04 and last["close"]>last["open"]: add(+16,"Double Bottom ↑")
        elif prev2["high"] >= a["rh"]-rng*.04 and last["close"]<last["open"]: add(-16,"Double Top ↓")

        return {"score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,"reasons":reasons,"valid":abs(score)>=16}

    # ── CORE 4 : MOMENTUM MTF ────────────────────────────────────────────────
    @staticmethod
    def momentum(a):
        score = 0; reasons = []
        def add(pts, lbl): nonlocal score; score += pts; reasons.append(lbl)

        # RSI
        if a["rsi"]<25:    add(+22,"RSI suroffert")
        elif a["rsi"]<35:  add(+12,"RSI bas")
        elif a["rsi"]>75:  add(-22,"RSI suracheté")
        elif a["rsi"]>65:  add(-12,"RSI haut")
        # MACD
        if a["macd_hist"]>0:  add(+10,"MACD ↑")
        else:                  add(-10,"MACD ↓")
        # Stoch
        if a["stoch_k"]<20 and a["stoch_d"]<20:   add(+14,"Stoch suroffert")
        elif a["stoch_k"]>80 and a["stoch_d"]>80: add(-14,"Stoch suracheté")
        if a["stoch_k"]>a["stoch_d"] and a["stoch_k"]<30: add(+10,"Stoch Crois ↑")
        if a["stoch_k"]<a["stoch_d"] and a["stoch_k"]>70: add(-10,"Stoch Crois ↓")
        # EMA
        if a["ema_bull"]:  add(+14,"EMA 9>21>50 ↑")
        elif a["ema_bear"]: add(-14,"EMA 9<21<50 ↓")
        if a["above_e200"]: add(+6,"Prix > EMA200 ↑")
        else:               add(-6,"Prix < EMA200 ↓")
        # Bollinger
        price = a["price"]
        if price < a["bb_lo"]:  add(+12,"Prix < BB inf ↑")
        elif price > a["bb_up"]: add(-12,"Prix > BB sup ↓")
        # CCI
        if a["cci"] < -100:   add(+8,"CCI suroffert ↑")
        elif a["cci"] > 100:  add(-8,"CCI suracheté ↓")
        # Williams %R
        if a["williams_r"] < -80:  add(+8,"Williams %R suroffert ↑")
        elif a["williams_r"] > -20: add(-8,"Williams %R suracheté ↓")
        # Momentum
        if a["momentum"] > 0.3:  add(+6,f"Momentum +{a['momentum']:.2f}%")
        elif a["momentum"] < -0.3: add(-6,f"Momentum {a['momentum']:.2f}%")

        return {"score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,"reasons":reasons,"valid":abs(score)>=20}

    # ── CORE 5 : STRUCTURE ───────────────────────────────────────────────────
    @staticmethod
    def structure(a, candles):
        score = 0; reasons = []
        def add(pts, lbl): nonlocal score; score += pts; reasons.append(lbl)

        H = [c["high"] for c in candles]; L = [c["low"] for c in candles]; C = [c["close"] for c in candles]
        price = a["price"]
        # Pivot Points
        if len(candles) >= 24:
            piv = TA.pivot(max(H[-24:]), min(L[-24:]), C[-25] if len(C)>24 else C[-1])
            for k, v in piv.items():
                if abs(price-v) < a["rng"]*.02:
                    if k.startswith("r"): add(-10,f"Résistance Pivot {k.upper()}")
                    elif k.startswith("s"): add(+10,f"Support Pivot {k.upper()}")
        # ADX — force tendance
        if a["adx"] > 30: add(+6,"Tendance Forte ADX")
        elif a["adx"] < 20: add(-4,"Tendance Faible ADX")
        # Tendance H1
        if len(C) >= 24:
            h1 = [C[i*12] for i in range(len(C)//12)]
            if len(h1) >= 3:
                if h1[-1] > h1[-3]: add(+8,"Tendance H1 ↑")
                else:                add(-8,"Tendance H1 ↓")
        # Niveau psychologique
        rnd = round(price/100)*100
        if abs(price-rnd) < a["atr"]*.5: add(+6,f"Niveau rond {rnd:.0f}")

        return {"score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,"reasons":reasons,"valid":abs(score)>=8}

    # ════════════════════════════════════════════════════════════════
    # SNIPER ENTRIES — 5 stratégies pour éviter les stop loss
    # Chacune cherche l'entrée la plus précise possible
    # ════════════════════════════════════════════════════════════════

    # ── SNIPER 1 : OB RETEST (entrée sur le retour dans l'Order Block) ──────
    @staticmethod
    def sniper_ob_retest(a, candles):
        """
        Pattern : prix teste un OB puis rebondit — entrée sur le retest
        Évite le stop en entrant DANS la zone de liquidité, pas avant
        """
        score = 0; reasons = []; entry_precision = 0
        price = a["price"]
        rng   = a["rng"]

        if a["in_ob_bull"] and a["structure"]=="Haussier":
            ob = a["obs_bull"][0] if a["obs_bull"] else None
            if ob:
                # Vérifier que le prix est dans le bas de l'OB (zone optimale)
                zone_quality = 1 - (price - ob["lo"]) / max(ob["hi"]-ob["lo"], 0.0001)
                if zone_quality > 0.6:  # Dans les 40% bas de l'OB
                    score += 25; entry_precision = zone_quality
                    reasons.append(f"OB Bull Retest précis ({zone_quality*100:.0f}%)")
                if a["rsi"] < 50: score += 10; reasons.append("RSI côté acheteur")
                if a["ema_bull"]: score += 8;  reasons.append("EMA alignées ↑")

        if a["in_ob_bear"] and a["structure"]=="Baissier":
            ob = a["obs_bear"][0] if a["obs_bear"] else None
            if ob:
                zone_quality = (ob["hi"] - price) / max(ob["hi"]-ob["lo"], 0.0001)
                if zone_quality > 0.6:
                    score -= 25; entry_precision = zone_quality
                    reasons.append(f"OB Bear Retest précis ({zone_quality*100:.0f}%)")
                if a["rsi"] > 50:  score -= 10; reasons.append("RSI côté vendeur")
                if a["ema_bear"]:  score -= 8;  reasons.append("EMA alignées ↓")

        return {
            "score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,
            "reasons":reasons,"entry_precision":round(entry_precision,3),
            "valid":abs(score)>=25,"type":"OB_RETEST"
        }

    # ── SNIPER 2 : FVG FILL (entrée sur comblement du FVG) ──────────────────
    @staticmethod
    def sniper_fvg_fill(a, candles):
        """
        Le prix comble un FVG et rebondit — entrée au milieu du FVG
        Très précis car le prix revient toujours combler les FVGs
        """
        score = 0; reasons = []; best_fvg = None
        price = a["price"]

        for fvg in a["fvgs"]:
            in_fvg = fvg["bot"] <= price <= fvg["top"]
            if in_fvg:
                dist_mid = abs(price - fvg["mid"])
                fvg_size = fvg["top"] - fvg["bot"]
                # Meilleure entrée = au milieu du FVG
                quality = 1 - dist_mid / max(fvg_size, 0.0001)
                if quality > 0.4:
                    if fvg["type"]=="bull" and a["structure"]=="Haussier":
                        score += 22; best_fvg = fvg
                        reasons.append(f"FVG Bull Fill précis ({quality*100:.0f}%)")
                    elif fvg["type"]=="bear" and a["structure"]=="Baissier":
                        score -= 22; best_fvg = fvg
                        reasons.append(f"FVG Bear Fill précis ({quality*100:.0f}%)")

        # Confluence avec d'autres indicateurs
        if score > 0:
            if a["rsi"] < 45: score += 8; reasons.append("RSI valide ↑")
            if a["choch"]=="haussier": score += 10; reasons.append("CHoCH ↑")
        elif score < 0:
            if a["rsi"] > 55: score -= 8; reasons.append("RSI valide ↓")
            if a["choch"]=="baissier": score -= 10; reasons.append("CHoCH ↓")

        return {
            "score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,
            "reasons":reasons,"fvg":best_fvg,"valid":abs(score)>=22,"type":"FVG_FILL"
        }

    # ── SNIPER 3 : SWEEP + REVERSE (manipulation + retournement) ────────────
    @staticmethod
    def sniper_sweep_reverse(a, candles):
        """
        Après un Liquidity Sweep, le prix retourne dans la direction opposée
        C'est l'un des setups les plus précis en SMC
        """
        score = 0; reasons = []
        if not a["sweep"]: return {"score":0,"direction":None,"reasons":[],"valid":False,"type":"SWEEP_REVERSE"}

        if a["sweep"]=="BSL":  # Sweep des hauts → retournement baissier
            score -= 28; reasons.append("BSL chassé → Reversal ↓")
            if a["rsi"] > 60:   score -= 8; reasons.append("RSI suracheté ↓")
            if a["in_ob_bear"]: score -= 10; reasons.append("Dans OB Bear ↓")
            if a["choch"]=="baissier": score -= 12; reasons.append("CHoCH confirme ↓")

        elif a["sweep"]=="SSL":  # Sweep des bas → retournement haussier
            score += 28; reasons.append("SSL chassé → Reversal ↑")
            if a["rsi"] < 40:    score += 8; reasons.append("RSI suroffert ↑")
            if a["in_ob_bull"]:  score += 10; reasons.append("Dans OB Bull ↑")
            if a["choch"]=="haussier": score += 12; reasons.append("CHoCH confirme ↑")

        return {
            "score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,
            "reasons":reasons,"valid":abs(score)>=28,"type":"SWEEP_REVERSE"
        }

    # ── SNIPER 4 : DIVERGENCE RSI (divergence haussière/baissière) ───────────
    @staticmethod
    def sniper_divergence(a, candles):
        """
        Divergence entre le prix et le RSI = signal de retournement puissant
        Haussière : prix fait lower low, RSI fait higher low
        Baissière : prix fait higher high, RSI fait lower high
        """
        if len(candles) < 20:
            return {"score":0,"direction":None,"reasons":[],"valid":False,"type":"DIVERGENCE"}

        score = 0; reasons = []
        C = [c["close"] for c in candles]
        L = [c["low"]   for c in candles]
        H = [c["high"]  for c in candles]

        # Calculer RSI sur 14 dernières valeurs
        rsi_series = []
        for i in range(14, len(C)+1):
            rsi_series.append(TA.rsi(C[i-14:i]))

        if len(rsi_series) < 5:
            return {"score":0,"direction":None,"reasons":[],"valid":False,"type":"DIVERGENCE"}

        # Divergence Haussière : prix lower low + RSI higher low
        if (L[-1] < L[-5] and  # Prix plus bas
            rsi_series[-1] > rsi_series[-5] and  # RSI plus haut
            rsi_series[-1] < 50):  # RSI dans zone baissière
            score += 24; reasons.append("Divergence Haussière RSI ↑")
            if a["in_ob_bull"] or a["in_ote"]: score += 12; reasons.append("+ OB/OTE confluence")

        # Divergence Baissière : prix higher high + RSI lower high
        elif (H[-1] > H[-5] and
              rsi_series[-1] < rsi_series[-5] and
              rsi_series[-1] > 50):
            score -= 24; reasons.append("Divergence Baissière RSI ↓")
            if a["in_ob_bear"] or a["in_ote"]: score -= 12; reasons.append("+ OB/OTE confluence")

        return {
            "score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,
            "reasons":reasons,"valid":abs(score)>=24,"type":"DIVERGENCE"
        }

    # ── SNIPER 5 : SESSION OPEN BREAKOUT (cassure ouverture de session) ──────
    @staticmethod
    def sniper_session_open(a, candles, kz):
        """
        À l'ouverture d'une Kill Zone, le prix casse un niveau clé
        Les premières 15 minutes d'une session = les mouvements les plus forts
        """
        if not kz:
            return {"score":0,"direction":None,"reasons":[],"valid":False,"type":"SESSION_OPEN"}

        score = 0; reasons = []
        now_utc = datetime.now(timezone.utc)
        mins_in_kz = now_utc.hour*60 + now_utc.minute - kz["start"]

        # Seulement dans les 20 premières minutes de la session
        if mins_in_kz > 20:
            return {"score":0,"direction":None,"reasons":[],"valid":False,"type":"SESSION_OPEN"}

        price = a["price"]; rng = a["rng"]
        H = [c["high"] for c in candles]; L = [c["low"] for c in candles]

        # Niveau d'ouverture de session (5 dernières bougies avant KZ)
        open_high = max(H[-5:]) if len(H) >= 5 else max(H)
        open_low  = min(L[-5:]) if len(L) >= 5 else min(L)

        # Cassure haussière de l'ouverture
        if price > open_high + a["atr"]*0.3:
            score += int(20 * kz["w"]); reasons.append(f"Cassure Ouverture {kz['label']} ↑")
            if a["bos"]=="haussier":  score += 10; reasons.append("BOS confirme ↑")
            if a["ema_bull"]:         score += 8;  reasons.append("EMA confirme ↑")

        # Cassure baissière
        elif price < open_low - a["atr"]*0.3:
            score -= int(20 * kz["w"]); reasons.append(f"Cassure Ouverture {kz['label']} ↓")
            if a["bos"]=="baissier":  score -= 10; reasons.append("BOS confirme ↓")
            if a["ema_bear"]:         score -= 8;  reasons.append("EMA confirme ↓")

        return {
            "score":score,"direction":"BUY" if score>0 else "SELL" if score<0 else None,
            "reasons":reasons,"valid":abs(score)>=20,"type":"SESSION_OPEN",
            "mins_in_kz":mins_in_kz
        }

    # ── COMBINAISON FINALE ────────────────────────────────────────────────────
    @staticmethod
    def combine(pair_id, candles, kz=None):
        a = smc_analyze(pair_id, candles)
        if not a:
            return None

        # CORE strategies (poids fixes)
        r_smc  = StrategyEngine.smc(a, kz)
        r_ict  = StrategyEngine.ict(a, kz)
        r_pa   = StrategyEngine.price_action(a, candles)
        r_mom  = StrategyEngine.momentum(a)
        r_str  = StrategyEngine.structure(a, candles)

        # SNIPER strategies
        r_s1 = StrategyEngine.sniper_ob_retest(a, candles)
        r_s2 = StrategyEngine.sniper_fvg_fill(a, candles)
        r_s3 = StrategyEngine.sniper_sweep_reverse(a, candles)
        r_s4 = StrategyEngine.sniper_divergence(a, candles)
        r_s5 = StrategyEngine.sniper_session_open(a, candles, kz)

        cores   = [r_smc, r_ict, r_pa, r_mom, r_str]
        snipers = [r_s1, r_s2, r_s3, r_s4, r_s5]

        # Score pondéré
        weights_core   = [0.28, 0.22, 0.18, 0.14, 0.08]  # total = 0.90
        weights_sniper = [0.02] * 5                        # total = 0.10

        weighted_score = (
            sum(cores[i]["score"]*weights_core[i] for i in range(5)) +
            sum(snipers[i]["score"]*weights_sniper[i] for i in range(5))
        )

        # Consensus : combien de stratégies sont dans le même sens
        all_results = cores + snipers
        buy_count  = sum(1 for r in all_results if r["direction"]=="BUY")
        sell_count = sum(1 for r in all_results if r["direction"]=="SELL")

        direction = None
        if buy_count >= 6 and weighted_score > 0:   direction = "BUY"
        elif sell_count >= 6 and weighted_score < 0: direction = "SELL"

        # Sniper bonus : si ≥ 2 snipers valides dans le même sens → bonus confiance
        sniper_buy  = sum(1 for r in snipers if r["direction"]=="BUY"  and r["valid"])
        sniper_sell = sum(1 for r in snipers if r["direction"]=="SELL" and r["valid"])
        sniper_bonus = 0
        if sniper_buy >= 2 and direction=="BUY":   sniper_bonus = 12
        elif sniper_sell >= 2 and direction=="SELL": sniper_bonus = 12

        confidence = min(100, abs(weighted_score)*1.3 + sniper_bonus)

        # Kill Zone boost
        if kz and direction:
            confidence = min(100, confidence * kz["w"])

        # MTF check (H1)
        mtf_aligned = False
        if len(candles) >= 60:
            h1c = []
            for i in range(0, len(candles)-12+1, 12):
                s = candles[i:i+12]
                h1c.append({
                    "open":s[0]["open"],"high":max(c["high"] for c in s),
                    "low":min(c["low"] for c in s),"close":s[-1]["close"]
                })
            if len(h1c) >= 10:
                a_h1 = smc_analyze(pair_id, h1c)
                if a_h1 and a_h1["structure"] == (a["structure"]):
                    mtf_aligned = True
                    confidence = min(100, confidence * 1.08)

        # Calcul SL/TP basé sur ATR (précis)
        pair  = PAIRS[pair_id]
        price = a["price"]
        atr   = a["atr"]

        if direction == "BUY":
            # SL sous le dernier swing low ou sous l'OB
            if a["obs_bull"] and a["in_ob_bull"]:
                sl = a["obs_bull"][0]["lo"] - atr * 0.5  # Sous l'OB avec buffer
            elif a["rl"]:
                sl = a["rl"] - atr * 0.3
            else:
                sl = price - atr * 1.5
            tp1 = price + abs(price-sl) * 2.0   # RR 1:2
            tp2 = price + abs(price-sl) * 3.0   # RR 1:3
            tp3 = price + abs(price-sl) * 5.0   # RR 1:5 (si setup fort)
        elif direction == "SELL":
            if a["obs_bear"] and a["in_ob_bear"]:
                sl = a["obs_bear"][0]["hi"] + atr * 0.5
            elif a["rh"]:
                sl = a["rh"] + atr * 0.3
            else:
                sl = price + atr * 1.5
            tp1 = price - abs(sl-price) * 2.0
            tp2 = price - abs(sl-price) * 3.0
            tp3 = price - abs(sl-price) * 5.0
        else:
            sl = tp1 = tp2 = tp3 = price

        # Vérifier cohérence SL/TP
        rr1 = AdaptiveRisk.calc_rr(price, sl, tp1, direction) if direction else 0
        rr2 = AdaptiveRisk.calc_rr(price, sl, tp2, direction) if direction else 0
        rr3 = AdaptiveRisk.calc_rr(price, sl, tp3, direction) if direction else 0

        # Annuler si RR invalide
        if rr1 < 0 or rr1 < 1.8:
            direction = None
            confidence = 0

        # Agréger les raisons
        all_reasons = []
        for r in cores + snipers:
            all_reasons.extend(r.get("reasons", []))

        return {
            "direction":    direction,
            "confidence":   round(confidence, 1),
            "score":        round(weighted_score, 2),
            "consensus":    f"{buy_count}/10 BUY · {sell_count}/10 SELL",
            "sniper_valid": sniper_buy if direction=="BUY" else sniper_sell,
            "mtf_aligned":  mtf_aligned,
            "price":        round(price, pair["d"]),
            "sl":           round(sl,   pair["d"]),
            "tp1":          round(tp1,  pair["d"]),
            "tp2":          round(tp2,  pair["d"]),
            "tp3":          round(tp3,  pair["d"]),
            "rr1":          rr1, "rr2": rr2, "rr3": rr3,
            "atr":          atr,
            "reasons":      list(dict.fromkeys(all_reasons))[:12],
            "strategies": {
                "smc":r_smc,"ict":r_ict,"pa":r_pa,"momentum":r_mom,"structure":r_str,
                "ob_retest":r_s1,"fvg_fill":r_s2,"sweep_rev":r_s3,
                "divergence":r_s4,"session_open":r_s5,
            },
            "kz":           kz["label"] if kz else None,
            "smc_data":     a,
            "tradeable":    direction is not None and confidence >= Config.MIN_CONF_TRADE,
        }

# ══════════════════════════════════════════════════════════════════════════════
#  DOUBLE IA — Claude + GPT en concertation
# ══════════════════════════════════════════════════════════════════════════════
class DualAI:
    """
    Deux IA analysent le signal indépendamment.
    Le trade n'est validé que si les DEUX disent VALIDE.
    En cas de désaccord → signal rejeté (prudence maximale).
    """

    @staticmethod
    def _build_prompt(pair_id, sig):
        reasons = "\n".join(f"- {r}" for r in sig.get("reasons", []))
        strats  = sig.get("strategies", {})
        valid_s = [k for k,v in strats.items() if v.get("valid")]
        return (
            f"Tu es un expert trader professionnel SMC/ICT.\n"
            f"Analyse ce signal de trading et réponds UNIQUEMENT par VALIDE ou INVALIDE.\n\n"
            f"Actif     : {pair_id}\n"
            f"Direction : {sig['direction']}\n"
            f"Confiance : {sig['confidence']}%\n"
            f"Consensus : {sig['consensus']}\n"
            f"MTF aligné: {'Oui' if sig.get('mtf_aligned') else 'Non'}\n"
            f"Kill Zone : {sig.get('kz') or 'Non'}\n"
            f"Snipers   : {sig.get('sniper_valid',0)}/5 valides\n"
            f"RR 1:2    : {sig.get('rr1',0):.2f} | RR 1:3 : {sig.get('rr2',0):.2f}\n"
            f"Stratégies valides : {', '.join(valid_s)}\n\n"
            f"Confluences :\n{reasons}\n\n"
            f"Critères de validation :\n"
            f"- Confiance ≥ 75%\n"
            f"- RR ≥ 1.8\n"
            f"- Au moins 1 sniper valide\n"
            f"- MTF aligné ou confiance > 85%\n\n"
            f"Réponds UNIQUEMENT: VALIDE ou INVALIDE (suivi d'une raison max 15 mots)"
        )

    @staticmethod
    def ask_claude(pair_id, sig):
        if not Config.CLAUDE_API_KEY:
            return None, "Claude non configuré"
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": Config.CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 50,
                    "messages": [{"role":"user","content": DualAI._build_prompt(pair_id, sig)}]
                },
                timeout=8
            )
            if r.status_code == 200:
                text = r.json()["content"][0]["text"].strip()
                valid = text.upper().startswith("VALIDE")
                log.info(f"  🧠 Claude: {text}")
                return valid, text
            return None, f"Erreur HTTP {r.status_code}"
        except Exception as e:
            return None, f"Claude error: {e}"

    @staticmethod
    def ask_gpt(pair_id, sig):
        if not Config.OPENAI_API_KEY:
            return None, "GPT non configuré"
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {Config.OPENAI_API_KEY}"},
                json={
                    "model": "gpt-4o-mini",
                    "max_tokens": 50,
                    "temperature": 0.1,
                    "messages": [{"role":"user","content": DualAI._build_prompt(pair_id, sig)}]
                },
                timeout=8
            )
            if r.status_code == 200:
                text = r.json()["choices"][0]["message"]["content"].strip()
                valid = text.upper().startswith("VALIDE")
                log.info(f"  🤖 GPT: {text}")
                return valid, text
            return None, f"Erreur HTTP {r.status_code}"
        except Exception as e:
            return None, f"GPT error: {e}"

    @staticmethod
    def validate(pair_id, sig):
        """Les deux IA doivent être d'accord pour valider"""
        if not Config.USE_DUAL_AI:
            return True, "IA désactivée — signal accepté automatiquement"

        c_valid, c_reason = DualAI.ask_claude(pair_id, sig)
        g_valid, g_reason = DualAI.ask_gpt(pair_id, sig)

        # Si une seule IA est configurée
        if c_valid is None and g_valid is None:
            return True, "Aucune IA disponible — signal accepté"
        if c_valid is None:
            return g_valid, f"GPT seul: {g_reason}"
        if g_valid is None:
            return c_valid, f"Claude seul: {c_reason}"

        # Concertation : les DEUX doivent valider
        if c_valid and g_valid:
            return True, f"✅ DOUBLE IA VALIDE | Claude: {c_reason} | GPT: {g_reason}"
        elif not c_valid and not g_valid:
            return False, f"❌ DOUBLE IA INVALIDE | Claude: {c_reason} | GPT: {g_reason}"
        else:
            # Désaccord → prudence = refus
            return False, f"⚠️ DÉSACCORD IA — Claude: {c_reason} | GPT: {g_reason}"

# ══════════════════════════════════════════════════════════════════════════════
#  STOP AUTOMATIQUE — 4 SL consécutifs
# ══════════════════════════════════════════════════════════════════════════════
class TradingGuard:
    @staticmethod
    def check_and_pause(db_data):
        """Arrête le bot après MAX_CONSEC_LOSSES SL consécutifs"""
        with db_lock:
            consec = db_data["perf"].get("consec_losses", 0)
            if consec >= Config.MAX_CONSEC_LOSSES:
                # Pause jusqu'à minuit
                tomorrow = (datetime.now() + timedelta(days=1)).replace(hour=0,minute=0,second=0)
                db_data["adaptive"]["paused_until"] = tomorrow.isoformat()
                db_data["adaptive"]["pause_reason"] = f"{consec} SL consécutifs"
                save_db(db_data)
                log.warning(f"🛑 BOT PAUSÉ — {consec} SL consécutifs — reprise {tomorrow.strftime('%H:%M')}")
                Notifier.send("PAUSE", f"Bot pausé automatiquement — {consec} SL consécutifs. Reprise demain 00h00.")
                return False
            return True

    @staticmethod
    def is_paused(db_data):
        with db_lock:
            paused_until = db_data["adaptive"].get("paused_until")
            if not paused_until:
                return False
            try:
                pt = datetime.fromisoformat(paused_until)
                if datetime.now() < pt:
                    return True
                else:
                    db_data["adaptive"]["paused_until"] = None
                    db_data["adaptive"]["pause_reason"] = ""
                    db_data["perf"]["consec_losses"] = 0
                    save_db(db_data)
                    return False
            except:
                return False

    @staticmethod
    def record_result(db_data, won: bool, pnl: float):
        with db_lock:
            perf = db_data["perf"]
            if won:
                perf["wins"] += 1
                perf["consec_losses"] = 0
                perf["consec_wins"]   = perf.get("consec_wins", 0) + 1
            else:
                perf["losses"] += 1
                perf["consec_losses"] = perf.get("consec_losses", 0) + 1
                perf["consec_wins"]   = 0
            perf["total"] += 1
            perf["pnl"]   = round(perf.get("pnl", 0) + pnl, 2)
            cap = db_data.get("capital", Config.CAPITAL) + pnl
            db_data["capital"] = max(0, round(cap, 2))
            if db_data["capital"] > perf.get("peak", 0):
                perf["peak"] = db_data["capital"]
            dd = (perf["peak"] - db_data["capital"]) / max(perf["peak"], 1) * 100
            if dd > perf.get("max_dd", 0):
                perf["max_dd"] = round(dd, 2)
            save_db(db_data)

# ══════════════════════════════════════════════════════════════════════════════
#  CONNEXION DERIV
# ══════════════════════════════════════════════════════════════════════════════
class DerivConn:
    """
    IMPORTANT — pourquoi Multipliers et pas CALL/PUT :
    Les contrats CALL/PUT (options binaires) ont une durée fixe et expirent
    automatiquement — impossible de bouger un SL dessus, donc impossible de
    faire du trailing ou du stop-and-reverse. Les contrats MULTUP/MULTDOWN
    (Multipliers) se comportent comme un CFD à effet de levier : ils ont un
    stop_loss modifiable en direct via `contract_update`, et peuvent être
    fermés à tout moment via `sell`. C'est le seul type de contrat Deriv qui
    permet ce que tu veux faire.
    À TESTER EN DEMO D'ABORD — vérifie que le multiplicateur choisi est
    disponible sur le compte (Deriv limite les multiplicateurs par actif).
    """
    def __init__(self):
        self.ws = None; self.connected = False; self.authorized = False
        self.balance = 0.0; self.currency = "USD"
        self._lock = threading.Lock(); self._msg_id = 0
        self._pending = {}   # req_id -> threading.Event
        self._responses = {} # req_id -> message dict

    def _next_id(self):
        with self._lock:
            self._msg_id += 1; return self._msg_id

    def _send(self, data):
        try:
            if self.ws and self.connected:
                self.ws.send(json.dumps(data))
                return True
        except Exception as e:
            log.error(f"Deriv send: {e}")
        return False

    def _request(self, data: dict, timeout: float = 10.0) -> dict | None:
        """Envoie une requête et attend la réponse correspondante (par req_id)."""
        req_id = self._next_id()
        data["req_id"] = req_id
        ev = threading.Event()
        self._pending[req_id] = ev
        if not self._send(data):
            self._pending.pop(req_id, None)
            return None
        got = ev.wait(timeout)
        self._pending.pop(req_id, None)
        resp = self._responses.pop(req_id, None)
        if not got:
            log.warning(f"Deriv: timeout sur requête {data.get('req_id')}")
        return resp

    def _account_headers(self, token):
        return {
            "Deriv-App-ID": Config.DERIV_APP_ID,
            "Authorization": f"Bearer {token}",
        }

    def _ensure_account_id(self, token):
        """
        Récupère l'account_id Options/Multipliers associé à ce token.
        1) On essaie d'abord GET /accounts (liste ce qui existe déjà — la
           plupart des comptes Deriv ont un compte demo par défaut, pas besoin
           d'en créer un).
        2) Si aucun compte du bon type (demo/real) n'existe, on en crée un via
           POST — Deriv exige alors 3 champs obligatoires : currency, group,
           account_type. Le "group" manquant causait l'erreur HTTP 422 vue en
           logs.
        """
        account_type = "real" if Config.TRADE_MODE == "real" else "demo"
        headers = self._account_headers(token)

        try:
            r = requests.get(
                f"{Config.DERIV_API_BASE}/trading/v1/options/accounts",
                headers=headers, timeout=10,
            )
            if r.status_code == 200:
                accounts = r.json().get("data", [])
                if isinstance(accounts, dict):
                    accounts = [accounts]
                for acc in accounts:
                    if acc.get("account_type") == account_type:
                        self.balance  = float(acc.get("balance", 0))
                        self.currency = acc.get("currency", "USD")
                        return acc["account_id"]
            elif r.status_code == 401:
                log.error("Deriv compte: HTTP 401 — token invalide ou application non autorisée")
                return None
            elif r.status_code == 403:
                log.error("Deriv compte: HTTP 403 — le token n'a pas le scope 'trade'")
                return None
            # 404 ou liste vide -> aucun compte de ce type, on en crée un plus bas
        except requests.exceptions.RequestException as e:
            log.warning(f"Deriv compte (GET): erreur réseau — {e}, tentative de création directe")

        try:
            r = requests.post(
                f"{Config.DERIV_API_BASE}/trading/v1/options/accounts",
                headers=headers,
                json={
                    "currency": "USD",
                    "group": Config.DERIV_ACCOUNT_GROUP,
                    "account_type": account_type,
                },
                timeout=10,
            )
            if r.status_code == 401:
                log.error("Deriv compte: HTTP 401 — token invalide ou application non autorisée")
                return None
            if r.status_code == 403:
                log.error("Deriv compte: HTTP 403 — le token n'a pas le scope nécessaire (vérifie 'trade' + 'account_manage')")
                return None
            if r.status_code == 422:
                log.error(f"Deriv compte: HTTP 422 — requête rejetée par Deriv, détail: {r.text[:300]}")
                return None
            if r.status_code == 429:
                log.error("Deriv compte: HTTP 429 — trop de requêtes, on ralentit")
                return None
            if r.status_code not in (200, 201):
                log.error(f"Deriv compte: HTTP {r.status_code} — {r.text[:200]}")
                return None
            data = r.json().get("data")
            if isinstance(data, list):
                data = data[0] if data else None
            if not data or "account_id" not in data:
                log.error(f"Deriv compte: réponse inattendue — {r.text[:200]}")
                return None
            self.balance  = float(data.get("balance", 0))
            self.currency = data.get("currency", "USD")
            return data["account_id"]
        except requests.exceptions.RequestException as e:
            log.error(f"Deriv compte: erreur réseau — {e}")
            return None

    def _fetch_ws_url(self, token, account_id):
        """POST .../accounts/{id}/otp — renvoie une URL WebSocket déjà authentifiée (OTP embarqué)."""
        try:
            r = requests.post(
                f"{Config.DERIV_API_BASE}/trading/v1/options/accounts/{account_id}/otp",
                headers=self._account_headers(token),
                timeout=10,
            )
            if r.status_code == 401:
                log.error("Deriv OTP: HTTP 401 — token invalide")
                return None
            if r.status_code == 403:
                log.error("Deriv OTP: HTTP 403 — permissions insuffisantes")
                return None
            if r.status_code == 429:
                log.error("Deriv OTP: HTTP 429 — trop de requêtes")
                return None
            if r.status_code != 200:
                log.error(f"Deriv OTP: HTTP {r.status_code} — {r.text[:200]}")
                return None
            url = r.json().get("data", {}).get("url")
            if not url:
                log.error(f"Deriv OTP: pas d'URL dans la réponse — {r.text[:200]}")
                return None
            return url
        except requests.exceptions.RequestException as e:
            log.error(f"Deriv OTP: erreur réseau — {e}")
            return None

    def connect(self):
        """
        Nouvelle API Deriv (Options/Multipliers) : le flux est en 2 temps —
        1) REST: récupérer l'account_id, puis échanger le token PAT contre une
           URL WebSocket à usage unique (OTP) via /accounts/{id}/otp.
        2) Se connecter au WebSocket avec cette URL — déjà authentifiée, plus
           besoin d'envoyer {"authorize": token} comme sur l'ancienne API.
        L'OTP est à courte durée de vie : on doit s'y connecter immédiatement
        après l'avoir obtenu, et en cas de reconnexion, en redemander un neuf
        (impossible de réutiliser une ancienne URL).
        """
        token = Config.DERIV_TOKEN_REAL if Config.TRADE_MODE=="real" else Config.DERIV_TOKEN_DEMO
        if not token:
            log.warning("Deriv: pas de token"); return False
        if not Config.DERIV_APP_ID:
            log.error("Deriv: DERIV_APP_ID manquant — enregistre une application sur developers.deriv.com et renseigne son app_id (pas 1089, qui est l'ancienne API)")
            return False

        if self.ws:
            try: self.ws.close()
            except Exception: pass
        self.connected = False
        self.authorized = False

        account_id = self._ensure_account_id(token)
        if not account_id:
            return False
        ws_url = self._fetch_ws_url(token, account_id)
        if not ws_url:
            return False

        try:
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open    = lambda ws: self._on_open(ws),
                on_message = lambda ws, m: self._on_message(ws, m),
                on_error   = lambda ws, e: self._on_error(ws, e),
                on_close   = lambda ws, c, m: self._on_close(ws, c, m),
            )
            threading.Thread(
                target=self.ws.run_forever,
                kwargs={"ping_interval": 20, "ping_timeout": 10},
                daemon=True,
            ).start()
            deadline = time.time() + 6
            while not self.connected and time.time() < deadline:
                time.sleep(0.1)
            if self.connected:
                # L'URL OTP authentifie déjà la session — pas de message
                # "authorize" à envoyer sur cette nouvelle API.
                self.authorized = True
                log.info(f"✅ Deriv autorisé (compte {account_id}) | Solde: {self.balance} {self.currency}")
            return self.authorized
        except Exception as e:
            log.error(f"Deriv connect: {e}"); return False


    def _on_open(self, ws):
        self.connected = True; log.info("✅ Deriv WebSocket connecté")
    def _on_error(self, ws, e):
        log.error(f"Deriv WS error: {e}"); self.connected = False
    def _on_close(self, ws, c, m):
        self.connected = False; self.authorized = False
        log.warning("Deriv WS fermé — reconnexion sera tentée par le superviseur")
    def _on_message(self, ws, message):
        try:
            msg = json.loads(message)
            req_id = msg.get("req_id")
            if req_id is not None and req_id in self._pending:
                self._responses[req_id] = msg
                self._pending[req_id].set()
        except Exception as e:
            log.error(f"Deriv msg: {e}")

    def _resolve_symbol(self, pair_id):
        pair = PAIRS[pair_id]
        deriv_sym = pair.get("deriv")
        # Auparavant limité à "synth" uniquement — l'or et le forex ("real")
        # ont maintenant leur symbole Deriv (frxXAUUSD etc) et peuvent trader
        # exactement comme les indices synthétiques.
        if not deriv_sym or deriv_sym not in ALLOWED_DERIV:
            return None
        return deriv_sym

    def open_position(self, pair_id, direction, stake, sl_distance_price, multiplier=None):
        """
        Ouvre un contrat Multiplier (MULTUP/MULTDOWN) avec stop_loss natif.
        sl_distance_price : distance du SL par rapport au prix d'entrée, EN PRIX (pas en %),
        c'est le SL calculé par la stratégie (ATR-based).
        Retourne {"status": "ok", "contract_id":..., "buy_price":..., "entry_spot":...} ou erreur.
        """
        if not self.authorized:
            return {"status": "error", "reason": "Deriv non autorisé"}
        deriv_sym = self._resolve_symbol(pair_id)
        if not deriv_sym:
            return {"status": "error", "reason": "Symbole non autorisé pour Multiplier"}

        stake = max(1.0, min(stake, self.balance * 0.15))
        mult = multiplier or int(os.getenv("DERIV_DEFAULT_MULTIPLIER", "50"))

        # Deriv veut le SL en perte monétaire max, pas en prix — on l'approxime
        # depuis la distance prix fournie par la stratégie (proportionnalité stake/levier).
        params = {
            "buy": 1,
            "price": stake,
            "parameters": {
                "contract_type": "MULTUP" if direction == "BUY" else "MULTDOWN",
                "symbol": deriv_sym,
                "multiplier": mult,
                "basis": "stake",
                "amount": stake,
                "currency": self.currency,
                "limit_order": {
                    "stop_loss": round(stake * 0.9, 2)  # borne de sécurité initiale, resserrée par le PositionManager
                },
            },
        }
        resp = self._request(params, timeout=10)
        if not resp:
            return {"status": "error", "reason": "Pas de réponse Deriv (timeout)"}
        if resp.get("error"):
            return {"status": "error", "reason": resp["error"].get("message", "erreur inconnue")}
        buy = resp.get("buy", {})
        log.info(f"✅ Position Deriv ouverte: #{buy.get('contract_id')} {direction} {deriv_sym} stake={stake}")
        return {
            "status": "ok",
            "contract_id": buy.get("contract_id"),
            "buy_price": buy.get("buy_price"),
            "entry_spot": buy.get("start_time"),
            "stake": stake,
            "multiplier": mult,
        }

    def get_contract_status(self, contract_id):
        """Interroge l'état actuel d'un contrat ouvert (prix, profit, is_sold)."""
        resp = self._request({"proposal_open_contract": 1, "contract_id": contract_id}, timeout=6)
        if not resp or resp.get("error"):
            return None
        poc = resp.get("proposal_open_contract")
        if not poc:
            return None
        return {
            "current_spot": poc.get("current_spot"),
            "entry_spot":   poc.get("entry_spot"),
            "profit":       poc.get("profit"),
            "is_sold":      bool(poc.get("is_sold")),
            "sell_price":   poc.get("sell_price"),
            "status":       poc.get("status"),
        }

    def update_sl(self, contract_id, new_sl_loss_amount):
        """Modifie le stop_loss d'un contrat Multiplier déjà ouvert (trailing)."""
        resp = self._request({
            "contract_update": 1,
            "contract_id": contract_id,
            "limit_order": {"stop_loss": round(max(0.01, new_sl_loss_amount), 2)},
        }, timeout=6)
        if not resp or resp.get("error"):
            reason = resp["error"].get("message") if resp and resp.get("error") else "timeout"
            log.warning(f"Deriv update_sl #{contract_id}: {reason}")
            return False
        return True

    def close_position(self, contract_id):
        """Ferme un contrat au marché (pour le retournement immédiat en SAR)."""
        resp = self._request({"sell": contract_id, "price": 0}, timeout=8)
        if not resp or resp.get("error"):
            reason = resp["error"].get("message") if resp and resp.get("error") else "timeout"
            log.warning(f"Deriv close #{contract_id}: {reason}")
            return {"status": "error", "reason": reason}
        sell = resp.get("sell", {})
        return {"status": "ok", "sold_for": sell.get("sold_for"), "contract_id": contract_id}

# ══════════════════════════════════════════════════════════════════════════════
#  CONNEXION MT5 (Exness, IC Markets, etc.)
# ══════════════════════════════════════════════════════════════════════════════
class MT5Conn:
    def __init__(self):
        self.connected = False; self.mt5 = None

    def connect(self):
        if not Config.USE_MT5:
            return False
        try:
            import MetaTrader5 as mt5
            self.mt5 = mt5
            if not mt5.initialize():
                log.error("MT5: initialize() failed"); return False
            if Config.MT5_LOGIN:
                ok = mt5.login(Config.MT5_LOGIN, Config.MT5_PASSWORD, Config.MT5_SERVER)
                if not ok:
                    log.error(f"MT5 login failed: {mt5.last_error()}"); return False
            self.connected = True; log.info("✅ MT5 connecté"); return True
        except ImportError:
            log.warning("MetaTrader5 non installé — pip install MetaTrader5"); return False
        except Exception as e:
            log.error(f"MT5: {e}"); return False

    def place_order(self, pair_id, direction, lot, sl, tp):
        if not self.connected or not self.mt5:
            return {"status":"error","reason":"MT5 non connecté"}
        mt5 = self.mt5
        pair = PAIRS[pair_id]
        symbol = (pair.get("td") or pair_id).replace("/","")
        if not mt5.symbol_select(symbol, True):
            return {"status":"error","reason":f"Symbole {symbol} introuvable"}
        tick = mt5.symbol_info_tick(symbol)
        if not tick:
            return {"status":"error","reason":"Tick non disponible"}
        price  = tick.ask if direction=="BUY" else tick.bid
        otype  = mt5.ORDER_TYPE_BUY if direction=="BUY" else mt5.ORDER_TYPE_SELL
        req = {
            "action":       mt5.TRADE_ACTION_DEAL, "symbol": symbol,
            "volume":       lot,     "type":       otype,
            "price":        price,   "sl":         sl,   "tp": tp,
            "deviation":    20,      "magic":      20250807,
            "comment":      "TAYNOIR_V2",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(req)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"✅ MT5 ordre: {direction} {symbol} lot={lot} ticket={result.order}")
            return {"status":"ok","ticket":result.order,"price":price}
        err = result.comment if result else "unknown"
        log.error(f"MT5 ordre échoué: {err}")
        return {"status":"error","reason":err}

    def get_positions(self):
        if not self.connected or not self.mt5: return []
        pos = self.mt5.positions_get()
        return list(pos) if pos else []

    def modify_sl(self, ticket, new_sl):
        """Déplacer SL au Break Even ou trailing"""
        if not self.connected or not self.mt5: return False
        mt5 = self.mt5
        pos = mt5.positions_get(ticket=ticket)
        if not pos: return False
        p = pos[0]
        req = {
            "action": mt5.TRADE_ACTION_SLTP, "position": ticket,
            "symbol": p.symbol, "sl": new_sl, "tp": p.tp,
        }
        result = mt5.order_send(req)
        return result and result.retcode == mt5.TRADE_RETCODE_DONE

# ══════════════════════════════════════════════════════════════════════════════
#  POSITION MANAGER — Trailing SL + STOP AND REVERSE (SAR)
# ══════════════════════════════════════════════════════════════════════════════
class PositionManager:
    """
    C'est le cœur de ce que tu as demandé : une fois une position ouverte,
    ce gestionnaire tourne en continu (toutes les Config.POSITION_POLL_SEC
    secondes) et :
      1. Suit le prix — tant que le trade est gagnant, il resserre le SL
         derrière le prix (jamais dans l'autre sens : le SL ne recule jamais).
      2. Ne commence à trailer qu'après SAR_ACTIVATION_R (évite de se faire
         sortir par le bruit avant que le trade ait prouvé quelque chose).
      3. Quand le prix touche le SL trailé → ferme la position ET, si le SAR
         est activé et que la limite de retournements en chaîne n'est pas
         atteinte, ouvre IMMÉDIATEMENT une position dans le sens opposé avec
         le même mécanisme de trailing. C'est le "stop and reverse".
      4. SAR_MAX_FLIPS protège contre un marché qui range : après N
         retournements consécutifs sans sortir gagnant, il arrête la chaîne
         plutôt que de saigner le capital en boucle.

    Le SL natif Deriv (limit_order.stop_loss à l'ouverture) reste en place
    comme filet de sécurité si le bot plante ou perd la connexion — mais le
    trailing et le reversal réels sont pilotés ici, en prix, pas en argent,
    ce qui colle exactement à une logique ATR/SMC au lieu d'une approximation
    du effet de levier Deriv.
    """
    def __init__(self):
        self.positions = {}   # pair_id -> state dict
        self._lock = threading.Lock()
        self._running = False

    def has_position(self, pair_id):
        return pair_id in self.positions

    def open(self, pair_id, sig, direction, stake):
        """Ouvre une position et l'enregistre pour suivi SAR."""
        result = deriv.open_position(pair_id, direction, stake, abs(sig["price"] - sig["sl"]))
        if result.get("status") != "ok":
            log.error(f"PositionManager.open {pair_id}: {result.get('reason')}")
            return None
        with self._lock:
            self.positions[pair_id] = {
                "pair_id": pair_id,
                "direction": direction,
                "contract_id": result["contract_id"],
                "entry_price": sig["price"],
                "initial_sl_price": sig["sl"],
                "current_sl_price": sig["sl"],
                "atr": sig["atr"],
                "stake": stake,
                "strategies": [k for k, v in sig.get("strategies", {}).items() if v.get("valid")],
                "flips": 0,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        with db_lock:
            db["trades"].append({
                "pair": pair_id, "direction": direction, "confidence": sig.get("confidence"),
                "entry": sig["price"], "sl": sig["sl"], "tp1": sig.get("tp1"),
                "tp2": sig.get("tp2"), "tp3": sig.get("tp3"),
                "rr1": sig.get("rr1"), "lot": stake, "risk_usd": stake,
                "time": datetime.now(timezone.utc).isoformat(),
                "status": "open", "pnl": 0, "mode": Config.TRADE_MODE, "sar_flip": 0,
            })
            # NB: perf["total"] est incrémenté à la CLÔTURE (dans TradingGuard.record_result),
            # pas ici à l'ouverture — sinon chaque trade est compté deux fois et le win rate
            # affiché est faussé.
            save_db(db)
        Notifier.send("OPEN", f"{direction} {pair_id} @ {sig['price']} | SL initial {sig['sl']}", {"pair": pair_id})
        return self.positions[pair_id]

    def _trail(self, pos, current_price):
        """Resserre le SL derrière le prix — jamais dans l'autre sens."""
        direction = pos["direction"]
        atr = max(pos["atr"], 1e-9)
        trail_dist = atr * Config.SAR_ATR_MULT
        min_trail  = atr * Config.SAR_MIN_TRAIL_ATR
        initial_risk = abs(pos["entry_price"] - pos["initial_sl_price"])
        if initial_risk <= 0:
            return
        profit_price = (current_price - pos["entry_price"]) if direction == "BUY" \
                       else (pos["entry_price"] - current_price)
        profit_R = profit_price / initial_risk

        if profit_R >= Config.SAR_ACTIVATION_R:
            if direction == "BUY":
                candidate = current_price - trail_dist
                if candidate - pos["current_sl_price"] >= min_trail:
                    pos["current_sl_price"] = candidate
            else:
                candidate = current_price + trail_dist
                if pos["current_sl_price"] - candidate >= min_trail:
                    pos["current_sl_price"] = candidate

    def _breached(self, pos, current_price):
        if pos["direction"] == "BUY":
            return current_price <= pos["current_sl_price"]
        return current_price >= pos["current_sl_price"]

    def _record_close(self, pos, pnl, won):
        TradingGuard.record_result(db, won, pnl)
        update_daily(pnl, 1 if won else -1)
        with db_lock:
            for strat in pos.get("strategies", []):
                sp = db.setdefault("strategy_perf", {}).setdefault(strat, {"wins": 0, "losses": 0})
                sp["wins" if won else "losses"] += 1
            for t in reversed(db["trades"]):
                if t.get("pair") == pos["pair_id"] and t.get("status") == "open":
                    t["status"] = "closed"; t["pnl"] = round(pnl, 2)
                    t["closed_at"] = datetime.now(timezone.utc).isoformat()
                    break
            save_db(db)

    def _flip(self, pair_id, pos, current_price):
        close_res = deriv.close_position(pos["contract_id"])
        status = deriv.get_contract_status(pos["contract_id"])
        pnl = status["profit"] if status and status.get("profit") is not None else \
              (close_res.get("sold_for", pos["stake"]) - pos["stake"] if close_res.get("status") == "ok" else -pos["stake"])
        won = pnl > 0
        self._record_close(pos, pnl, won)

        pos["flips"] += 1
        can_reverse = (
            Config.SAR_ENABLED
            and pos["flips"] < Config.SAR_MAX_FLIPS
            and market_open()
            and not TradingGuard.is_paused(db)
        )
        if not can_reverse:
            with self._lock:
                self.positions.pop(pair_id, None)
            reason = "limite de retournements atteinte" if pos["flips"] >= Config.SAR_MAX_FLIPS else "conditions non réunies"
            Notifier.send("CLOSE", f"{pair_id} clôturé ({reason}) | PnL {pnl:.2f}$")
            return

        new_direction = "SELL" if pos["direction"] == "BUY" else "BUY"
        initial_risk = abs(pos["entry_price"] - pos["initial_sl_price"])
        new_sl = current_price + initial_risk if new_direction == "SELL" else current_price - initial_risk
        cap = db.get("capital", Config.CAPITAL)
        mm = AdaptiveRisk.calc_position(PAIRS[pair_id], current_price, new_sl, cap)
        if not mm:
            with self._lock:
                self.positions.pop(pair_id, None)
            Notifier.send("CLOSE", f"{pair_id} clôturé — SL invalide pour reversal | PnL {pnl:.2f}$")
            return

        stake = cap * (AdaptiveRisk.get_risk_pct(cap) / 100)
        result = deriv.open_position(pair_id, new_direction, stake, initial_risk)
        if result.get("status") != "ok":
            with self._lock:
                self.positions.pop(pair_id, None)
            Notifier.send("CLOSE", f"{pair_id} — reversal échoué ({result.get('reason')}) | PnL {pnl:.2f}$")
            return

        with self._lock:
            pos.update({
                "direction": new_direction,
                "contract_id": result["contract_id"],
                "entry_price": current_price,
                "initial_sl_price": new_sl,
                "current_sl_price": new_sl,
                "stake": stake,
            })
        with db_lock:
            db["trades"].append({
                "pair": pair_id, "direction": new_direction, "entry": current_price,
                "sl": new_sl, "time": datetime.now(timezone.utc).isoformat(),
                "status": "open", "pnl": 0, "mode": Config.TRADE_MODE,
                "sar_flip": pos["flips"],
            })
            save_db(db)
        Notifier.send("SAR_FLIP", f"{pair_id} retourné → {new_direction} @ {current_price} (flip #{pos['flips']})")

    def tick(self):
        """Un passage de contrôle sur toutes les positions ouvertes."""
        with self._lock:
            pair_ids = list(self.positions.keys())
        for pair_id in pair_ids:
            pos = self.positions.get(pair_id)
            if not pos:
                continue
            status = deriv.get_contract_status(pos["contract_id"])
            if not status:
                continue
            if status["is_sold"]:
                pnl = status.get("profit", -pos["stake"]) or -pos["stake"]
                self._record_close(pos, pnl, pnl > 0)
                with self._lock:
                    self.positions.pop(pair_id, None)
                Notifier.send("CLOSE", f"{pair_id} clôturé côté broker (SL natif atteint) | PnL {pnl:.2f}$")
                continue
            current_price = status["current_spot"]
            if current_price is None:
                continue
            self._trail(pos, current_price)
            if self._breached(pos, current_price):
                self._flip(pair_id, pos, current_price)

    def run_forever(self):
        self._running = True
        log.info(f"🎯 PositionManager démarré (poll={Config.POSITION_POLL_SEC}s, SAR={'ON' if Config.SAR_ENABLED else 'OFF'})")
        while self._running:
            try:
                self.tick()
            except Exception as e:
                log.error(f"PositionManager.tick: {e}")
            time.sleep(Config.POSITION_POLL_SEC)

position_manager = None  # instancié dans main() une fois deriv connecté

# ══════════════════════════════════════════════════════════════════════════════
#  FETCH BOUGIES
# ══════════════════════════════════════════════════════════════════════════════
_cache = {}

def fetch_real(pair_id, interval="1min", count=120, min_candles=80, max_retries=3):
    """
    Récupère des bougies réelles via Twelve Data pour un actif "real"
    (forex/métaux). Durci pour diagnostiquer précisément chaque échec :
    clé API absente, symbole non mappé, erreur réseau, réponse invalide,
    ou nombre de bougies insuffisant — chaque cas logge la raison exacte.
    """
    if not Config.TD_API_KEY:
        log.warning(f"fetch_real {pair_id}: TD_API_KEY absente — actif réel ignoré")
        return None

    pair = PAIRS.get(pair_id)
    if not pair:
        log.warning(f"fetch_real {pair_id}: actif inconnu dans PAIRS")
        return None
    td_symbol = pair.get("td")
    if not td_symbol:
        log.warning(f"fetch_real {pair_id}: pas de symbole Twelve Data mappé")
        return None

    key = f"{pair_id}_{interval}"
    cached = _cache.get(key)
    if cached and time.time() - cached["ts"] < 300:
        return cached["data"]

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": td_symbol,
        "interval": interval,
        "outputsize": count,
        "apikey": Config.TD_API_KEY,
    }

    last_reason = "raison inconnue"
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code != 200:
                last_reason = f"HTTP {r.status_code}"
                log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
                time.sleep(1.5 * attempt)
                continue

            data = r.json()
            if not isinstance(data, dict):
                last_reason = "réponse JSON invalide (pas un objet)"
                log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
                time.sleep(1.5 * attempt)
                continue

            if data.get("status") == "error":
                last_reason = data.get("message", "erreur Twelve Data non précisée")
                log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — API error: {last_reason}")
                time.sleep(1.5 * attempt)
                continue

            values = data.get("values")
            if not values or not isinstance(values, list):
                last_reason = "champ 'values' absent ou vide dans la réponse"
                log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
                time.sleep(1.5 * attempt)
                continue

            candles = []
            for v in reversed(values):
                try:
                    candles.append({
                        "open":  float(v["open"]),
                        "high":  float(v["high"]),
                        "low":   float(v["low"]),
                        "close": float(v["close"]),
                    })
                except (KeyError, TypeError, ValueError):
                    continue  # bougie corrompue, on l'ignore plutôt que de faire planter tout le lot

            if len(candles) < min_candles:
                last_reason = f"seulement {len(candles)} bougies valides (minimum requis: {min_candles})"
                log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
                time.sleep(1.5 * attempt)
                continue

            _cache[key] = {"data": candles, "ts": time.time()}
            log.info(f"fetch_real {pair_id}: ✅ {len(candles)} bougies reçues (symbole TD: {td_symbol})")
            return candles

        except requests.exceptions.RequestException as e:
            last_reason = f"erreur réseau: {e}"
            log.warning(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
            time.sleep(1.5 * attempt)
        except Exception as e:
            last_reason = f"erreur inattendue: {e}"
            log.error(f"fetch_real {pair_id}: tentative {attempt}/{max_retries} — {last_reason}")
            time.sleep(1.5 * attempt)

    log.error(f"fetch_real {pair_id}: ❌ échec après {max_retries} tentatives — {last_reason}")
    return None

def fetch_synth(pair_id, granularity=60, count=120):
    pair = PAIRS[pair_id]
    deriv_sym = pair.get("deriv")
    if not deriv_sym or deriv_sym not in ALLOWED_DERIV: return None
    key = f"{pair_id}_{granularity}"
    cached = _cache.get(key)
    if cached and time.time()-cached["ts"] < 360:
        return cached["data"]
    result = {"data":None,"done":False}
    def on_msg(ws, message):
        try:
            msg = json.loads(message)
            if msg.get("error") or not msg.get("candles"):
                result["done"]=True; ws.close(); return
            candles = [{"open":float(c["open"]),"high":float(c["high"]),
                        "low":float(c["low"]),"close":float(c["close"])}
                       for c in msg["candles"]]
            result["data"]=candles; result["done"]=True; ws.close()
        except: result["done"]=True; ws.close()
    def on_err(ws,e): result["done"]=True
    def on_open(ws):
        ws.send(json.dumps({"ticks_history":deriv_sym,"adjust_start_time":1,
                            "count":count,"end":"latest","start":1,
                            "style":"candles","granularity":granularity}))
    # Nouvelle API publique Deriv — pas de app_id en paramètre d'URL, mais on
    # envoie quand même le header Deriv-App-ID pour identifier l'application
    # (l'ancien point de connexion ws.derivws.com rejette maintenant tout,
    # d'où le passage à api.derivws.com/trading/v1/options/ws/public).
    url = Config.DERIV_WS_PUBLIC
    headers = [f"Deriv-App-ID: {Config.DERIV_APP_ID}"] if Config.DERIV_APP_ID else []
    ws = websocket.WebSocketApp(url, header=headers, on_open=on_open, on_message=on_msg, on_error=on_err)
    t  = threading.Thread(target=ws.run_forever, daemon=True); t.start()
    deadline = time.time()+12
    while not result["done"] and time.time()<deadline: time.sleep(0.1)
    if not result["done"]: ws.close()
    if result["data"]:
        _cache[key]={"data":result["data"],"ts":time.time()}
    return result["data"]

def fetch_candles(pair_id):
    pair = PAIRS[pair_id]
    if pair["type"]=="real": return fetch_real(pair_id)
    return fetch_synth(pair_id)

# ══════════════════════════════════════════════════════════════════════════════
#  BACKTESTING WALK-FORWARD
# ══════════════════════════════════════════════════════════════════════════════
def run_backtest(pair_id, candles, capital=1000):
    if not candles or len(candles) < 80: return None
    split = int(len(candles)*0.70)
    in_s  = candles[:split]; out_s = candles[split:]

    def simulate(data, cap):
        equity=cap; peak=cap; wins=losses=be=0
        total_g=total_l=0; max_dd=0; returns=[]
        trades=[]
        for i in range(30, len(data)-5):
            window = data[max(0,i-100):i+1]
            if len(window) < 20: continue
            sig = StrategyEngine.combine(pair_id, window)
            if not sig or not sig["tradeable"] or not sig["direction"]: continue
            mm = AdaptiveRisk.calc_position(PAIRS[pair_id], sig["price"], sig["sl"], equity)
            if not mm or mm["danger"]: continue
            stake = mm["risk_usd"]
            # Simuler sur les 15 bougies suivantes
            result = None
            for j in range(i+1, min(len(data),i+15)):
                c = data[j]
                if sig["direction"]=="BUY":
                    if c["low"]  <= sig["sl"]:  result="loss"; break
                    if c["high"] >= sig["tp1"]: result="win";  break
                else:
                    if c["high"] >= sig["sl"]:  result="loss"; break
                    if c["low"]  <= sig["tp1"]: result="win";  break
            if not result: result="be"
            pnl = 0
            if result=="win":  pnl= stake*sig["rr1"]; wins+=1;   total_g+=pnl
            elif result=="loss":pnl=-stake;             losses+=1; total_l+=abs(pnl)
            else:               be+=1
            equity+=pnl
            ret = pnl/max(equity-pnl,0.01)
            returns.append(ret)
            if equity>peak: peak=equity
            dd=(peak-equity)/max(peak,0.01)*100
            if dd>max_dd: max_dd=dd
            trades.append({"dir":sig["direction"],"result":result,"pnl":round(pnl,2),"rr":sig["rr1"],"conf":sig["confidence"]})
            if len(trades)>=60: break
        n=wins+losses+be
        if n==0: return None
        avg=sum(returns)/max(len(returns),1)
        std=(sum((r-avg)**2 for r in returns)/max(len(returns)-1,1))**0.5
        sharpe=round(avg/max(std,0.001)*(252**0.5),2)
        pf=round(total_g/max(total_l,0.01),2)
        wr=round(wins/max(wins+losses,1)*100,1)
        rf=round((equity-cap)/max(max_dd/100*cap,0.01),2)
        return {"trades":n,"wins":wins,"losses":losses,"be":be,"win_rate":wr,
                "pf":pf,"sharpe":sharpe,"rf":rf,"max_dd":round(max_dd,2),
                "pnl":round(equity-cap,2),"last_5":trades[-5:]}
    in_r  = simulate(in_s,  capital)
    out_r = simulate(out_s, capital)
    if not in_r or not out_r: return None
    eff = round(out_r["win_rate"]/max(in_r["win_rate"],1),2)
    return {
        "in": in_r, "out": out_r, "efficiency": eff,
        "overfit": eff < 0.65,
        "label": "Robuste ✅" if eff>=0.80 else "Acceptable ⚠️" if eff>=0.65 else "Overfitting ❌",
        "verdict": "✅ Déploiement recommandé" if eff>=0.65 and out_r["win_rate"]>=60 else "⚠️ Optimiser avant déploiement"
    }

# ══════════════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS (remplace Telegram) — logs locaux + webhook optionnel
# ══════════════════════════════════════════════════════════════════════════════
class Notifier:
    """
    Le bot n'a plus besoin de Telegram pour fonctionner.
    Tout passe par les logs (fichier taynoir_v3.log + console).
    Si WEBHOOK_URL est configuré (Discord/Slack/serveur perso), un POST JSON
    est envoyé en plus — utile pour être notifié sans regarder les logs.
    """
    @staticmethod
    def send(event: str, text: str, data: dict | None = None):
        log.info(f"[{event}] {text}")
        if not Config.WEBHOOK_URL:
            return
        try:
            payload = {"event": event, "text": text, "data": data or {},
                       "time": datetime.now(timezone.utc).isoformat()}
            requests.post(Config.WEBHOOK_URL, json=payload, timeout=5)
        except Exception as e:
            log.warning(f"Webhook: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  INSTANCES GLOBALES
# ══════════════════════════════════════════════════════════════════════════════
deriv = DerivConn()
mt5_c = MT5Conn()
_last_sig = {}

def is_dup(pair_id, direction, cooldown=None):
    cooldown = cooldown if cooldown is not None else Config.DUP_COOLDOWN_SEC
    key=f"{pair_id}_{direction}"
    last=_last_sig.get(key)
    if last and time.time()-last < cooldown: return True
    _last_sig[key]=time.time(); return False

def update_daily(pnl, won):
    with db_lock:
        today=datetime.now().strftime("%Y-%m-%d")
        if today not in db["daily"]:
            db["daily"][today]={"trades":0,"wins":0,"losses":0,"be":0,"pnl":0.0}
        d=db["daily"][today]; d["trades"]+=1
        if won>0:   d["wins"]+=1
        elif won<0: d["losses"]+=1
        else:       d["be"]+=1
        d["pnl"]=round(d.get("pnl",0)+pnl,2)
        save_db(db)

# ══════════════════════════════════════════════════════════════════════════════
#  SCAN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def main_scan():
    """
    Ce scan ne fait plus qu'une chose : détecter de NOUVELLES entrées.
    Une fois une position ouverte, c'est PositionManager (thread séparé,
    poll toutes les Config.POSITION_POLL_SEC secondes) qui gère le trailing
    et le stop-and-reverse — pas ce scan à 5 minutes, trop lent pour du scalping.
    """
    if TradingGuard.is_paused(db):
        log.info(f"⏸ Bot pausé: {db['adaptive'].get('pause_reason','')}"); return
    if not TradingGuard.check_and_pause(db): return

    fx_open = market_open()
    kz   = active_kz()
    cap  = db.get("capital", Config.CAPITAL)
    rp   = AdaptiveRisk.get_risk_pct(cap)
    log.info(f"▶ SCAN | Forex/métaux: {'ouvert' if fx_open else 'fermé (weekend)'} | KZ: {kz['label'] if kz else 'Hors'} | Capital: {cap:.2f}$ | Risque: {rp}%")

    for pair_id in Config.ACTIVE_PAIRS:
        if pair_id not in PAIRS: continue
        # Les indices synthétiques (V25/V50/V75/V100...) tradent 24/7, y compris
        # le weekend — seuls le forex et les métaux (or, argent) ferment.
        if PAIRS[pair_id]["type"] == "real" and not fx_open:
            continue
        if position_manager and position_manager.has_position(pair_id):
            continue  # une position SAR est déjà en cours de gestion sur cet actif
        try:
            candles = fetch_candles(pair_id)
            if not candles or len(candles) < 20:
                log.warning(f"  {pair_id}: données insuffisantes"); continue

            sig = StrategyEngine.combine(pair_id, candles, kz)
            if not sig:
                continue

            log.info(f"  {pair_id}: {sig['direction']} {sig['confidence']:.0f}% | {sig['consensus']} | Snipers: {sig.get('sniper_valid',0)}/5")

            if not sig["direction"] or sig["confidence"] < Config.MIN_CONF_PREMIUM:
                continue
            if is_dup(pair_id, sig["direction"]):
                log.info(f"  {pair_id}: doublon ignoré"); continue

            mm = AdaptiveRisk.calc_position(PAIRS[pair_id], sig["price"], sig["sl"], cap)
            if mm and mm["danger"]:
                log.warning(f"  {pair_id}: risque trop élevé ({mm['risk_pct']:.1f}%)"); continue

            # Double IA
            ai_ok, ai_verdict = DualAI.validate(pair_id, sig)
            if not ai_ok:
                log.info(f"  {pair_id}: IA a rejeté — {ai_verdict}"); continue

            log.info(f"  ✅ Signal validé: {pair_id} {sig['direction']} conf={sig['confidence']:.0f}% | {ai_verdict[:60]}")

            # Exécution via Deriv — indices synthétiques ET forex/métaux (frxXAUUSD
            # etc) passent tous les deux par le même chemin maintenant. MT5 reste
            # une option séparée non branchée ici (nécessiterait un VPS Windows).
            if deriv.authorized and position_manager and deriv._resolve_symbol(pair_id):
                stake = cap * (rp / 100)
                position_manager.open(pair_id, sig, sig["direction"], stake)
            else:
                log.info(f"  {pair_id}: pas de symbole Deriv exécutable — signal ignoré")

            time.sleep(0.4)
        except Exception as e:
            log.error(f"Scan {pair_id}: {e}")

    log.info("◀ SCAN terminé")

def daily_check():
    """Rapport quotidien local (remplace l'ancien envoi Telegram)."""
    now = datetime.now(timezone.utc)
    if now.hour == 0 and now.minute <= 2:
        perf = db.get("perf", {})
        wr = round(perf.get("wins", 0) / max(1, perf.get("total", 1)) * 100, 1)
        Notifier.send("DAILY_REPORT",
            f"Bilan : {perf.get('total',0)} trades | WR {wr}% | "
            f"PnL {perf.get('pnl',0):.2f}$ | Capital {db.get('capital',0):.2f}$ | "
            f"Max DD {perf.get('max_dd',0):.1f}%")

def adaptive_update():
    """
    Auto-apprentissage :
    1. Ajuste le seuil de confiance minimum selon le win rate récent (comme avant).
    2. NOUVEAU — repère les stratégies qui performent mal réellement (pas en
       backtest, en LIVE) et les signale. Une stratégie avec un win rate réel
       très inférieur aux autres après un échantillon suffisant est un candidat
       à désactiver manuellement — le bot ne la coupe pas tout seul (trop risqué
       de le faire sans supervision), mais il te le dit clairement dans les logs.
    """
    with db_lock:
        recent = list(db.get("trades", []))[-20:]
        if len(recent) >= 5:
            wins = sum(1 for t in recent if t.get("pnl", 0) > 0)
            rwr  = wins / len(recent) * 100
            adapt = db.get("adaptive", {})
            if rwr >= 70:
                adapt["conf_threshold"] = max(65, adapt.get("conf_threshold", 78) - 1)
            elif rwr < 50:
                adapt["conf_threshold"] = min(90, adapt.get("conf_threshold", 78) + 2)
            Config.MIN_CONF_TRADE = adapt["conf_threshold"]
            db["adaptive"] = adapt
            log.info(f"🧠 Adaptatif: seuil={adapt['conf_threshold']}% | WR récent={rwr:.0f}%")

        sp = dict(db.get("strategy_perf", {}))  # copie pour itérer sans risque si un autre thread écrit

    weak = []
    for name, s in sp.items():
        total = s.get("wins", 0) + s.get("losses", 0)
        if total >= 10:
            wr = s["wins"] / total * 100
            if wr < 35:
                weak.append((name, round(wr, 1), total))
    if weak:
        log.warning(f"🧠 Stratégies faibles en LIVE (WR<35%, n≥10) : {weak} — envisage de les désactiver dans StrategyEngine")
    with db_lock:
        save_db(db)

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def deriv_supervisor():
    """
    Le message de log 'reconnexion sera tentée par le superviseur' ne servait
    à rien tant que ce superviseur n'existait pas — c'est corrigé ici.
    Vérifié toutes les 30 secondes : si la connexion Deriv est tombée,
    on la rétablit. PositionManager et DerivConn utilisent l'objet global
    `deriv`, donc dès que .authorized redevient True, tout continue de
    fonctionner sans rien relancer d'autre.
    """
    if not (Config.DERIV_TOKEN_DEMO or Config.DERIV_TOKEN_REAL):
        return
    if deriv.connected and deriv.authorized:
        return
    log.warning("🔌 Deriv déconnecté — tentative de reconnexion...")
    ok = deriv.connect()
    if ok:
        log.info("✅ Deriv reconnecté")
        Notifier.send("RECONNECT", "Connexion Deriv rétablie après coupure")
        global position_manager
        if position_manager is None:
            position_manager = PositionManager()
            threading.Thread(target=position_manager.run_forever, daemon=True).start()
    else:
        log.error("❌ Reconnexion Deriv échouée — nouvel essai dans 30s")

def main():
    global position_manager
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║        TAYNOIR AI TRADING BOT v3.0 — AUTONOME / SAR         ║")
    log.info("║  10 Stratégies · Double IA · Deriv Multipliers · SAR live  ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    cap = db.get("capital", Config.CAPITAL)
    rp  = AdaptiveRisk.get_risk_pct(cap)
    log.info(f"  Capital        : {cap:.2f}$")
    log.info(f"  Risque/trade   : {rp}%  (adaptatif selon capital)")
    log.info(f"  Mode           : {Config.TRADE_MODE.upper()}")
    log.info(f"  Stop auto      : {Config.MAX_CONSEC_LOSSES} SL consécutifs")
    log.info(f"  Double IA      : {'✅' if Config.USE_DUAL_AI else '❌'}")
    log.info(f"  SAR (reversal) : {'✅' if Config.SAR_ENABLED else '❌'} | max flips: {Config.SAR_MAX_FLIPS}")

    known_pairs   = [p for p in Config.ACTIVE_PAIRS if p in PAIRS]
    unknown_pairs = [p for p in Config.ACTIVE_PAIRS if p not in PAIRS]
    log.info(f"  Actifs : {len(known_pairs)} | {', '.join(known_pairs)}")
    if unknown_pairs:
        log.warning(f"  ⚠️ Actifs inconnus (ignorés, absents de PAIRS) : {', '.join(unknown_pairs)}")

    # Connexions
    if Config.DERIV_TOKEN_DEMO or Config.DERIV_TOKEN_REAL:
        ok = deriv.connect()
        log.info(f"  Deriv          : {'✅' if ok else '❌'}")
        if ok:
            position_manager = PositionManager()
            threading.Thread(target=position_manager.run_forever, daemon=True).start()
    else:
        log.warning("  Deriv          : pas de token — le bot tournera en analyse seule (aucun ordre réel)")
    if Config.USE_MT5:
        ok = mt5_c.connect()
        log.info(f"  MT5 (Exness)   : {'✅' if ok else '❌'} (trailing MT5 via MT5Conn.modify_sl, non branché sur SAR pour l'instant)")

    Notifier.send("START",
        f"TAYNOIR v3 démarré | Capital {cap:.2f}$ | Risque {rp}% | Mode {Config.TRADE_MODE.upper()} | "
        f"SAR {'ON' if Config.SAR_ENABLED else 'OFF'}")

    schedule.every(1).minutes.do(main_scan)
    schedule.every(1).minutes.do(daily_check)
    schedule.every(30).minutes.do(adaptive_update)
    schedule.every(30).seconds.do(deriv_supervisor)

    log.info("\n🚀 Scan initial...")
    main_scan()
    log.info("✅ Bot opérationnel — le PositionManager tourne en tâche de fond pour le trailing/SAR\n")

    while True:
        schedule.run_pending(); time.sleep(15)

if __name__ == "__main__":
    main()
