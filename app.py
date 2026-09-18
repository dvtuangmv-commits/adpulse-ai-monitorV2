import base64
import csv
import io
import json
import mimetypes
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests
from PIL import Image

BASE = Path(__file__).resolve().parent
DB = BASE / "data.db"
MEDIA = BASE / "media"
MEDIA.mkdir(exist_ok=True)
PORT = int(os.getenv("PORT", "8787"))
SYNC_LOCK = threading.Lock()

TIKTOK_BASE = "https://business-api.tiktok.com/open_api/v1.3"
META_API_VERSION = os.getenv("META_GRAPH_VERSION", "v26.0")
META_BASE = f"https://graph.facebook.com/{META_API_VERSION}"
OPENAI_BASE = "https://api.openai.com/v1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS campaigns (
  campaign_key TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  platform TEXT NOT NULL,
  account_id TEXT,
  active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS metric_rows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_key TEXT NOT NULL,
  ts TEXT NOT NULL,
  granularity TEXT NOT NULL,
  source TEXT NOT NULL,
  currency TEXT,
  spend REAL DEFAULT 0,
  impressions INTEGER DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  conversions REAL DEFAULT 0,
  revenue REAL DEFAULT 0,
  reach INTEGER DEFAULT 0,
  frequency REAL DEFAULT 0,
  ctr REAL DEFAULT NULL,
  cpc REAL DEFAULT NULL,
  cpm REAL DEFAULT NULL,
  cvr REAL DEFAULT NULL,
  roas REAL DEFAULT NULL,
  purchases REAL DEFAULT NULL,
  raw_json TEXT,
  pulled_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metric_campaign_ts ON metric_rows(campaign_key, ts);
CREATE TABLE IF NOT EXISTS sync_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT,
  started_at TEXT,
  finished_at TEXT,
  status TEXT,
  message TEXT,
  rows_loaded INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""

DEFAULT_SETTINGS = {
    "analysis_window": "1",
    "poll_seconds": "300",
    "baseline_hours": "24",
    "min_spend_for_alert": "100000",
    "min_clicks_for_alert": "30",
    "delta_alert_pct": "20",
    "target_roas_enabled": "0",
    "target_roas": "3",
    "ai_model": "gpt-5.6-luna",
}


def now_utc():
    return datetime.now(timezone.utc)


def iso_now():
    return now_utc().isoformat()


def db_conn():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    c = db_conn()
    c.executescript(SCHEMA)
    for k, v in DEFAULT_SETTINGS.items():
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))
    c.commit()
    c.close()


def get_settings():
    c = db_conn()
    rows = c.execute("SELECT key,value FROM settings").fetchall()
    c.close()
    return {r["key"]: r["value"] for r in rows}


def set_settings(values):
    c = db_conn()
    for k, v in values.items():
        if k in DEFAULT_SETTINGS:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))
    c.commit(); c.close()


def parse_num(v, default=0.0):
    if v is None or v == "":
        return default
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    try:
        return float(s)
    except Exception:
        return default


def parse_int(v, default=0):
    return int(round(parse_num(v, default)))


def safe_dt(s, tz=None):
    if not s:
        return None
    s = str(s).strip()
    s = s.replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.fromisoformat(s) if fmt is None else datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz or timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    return None


def ratio(a, b):
    return a / b if b else None


def derive(row):
    row["ctr"] = ratio(row["clicks"] * 100, row["impressions"])
    row["cpc"] = ratio(row["spend"], row["clicks"])
    row["cpm"] = ratio(row["spend"] * 1000, row["impressions"])
    row["cvr"] = ratio(row["conversions"] * 100, row["clicks"])
    row["roas"] = ratio(row["revenue"], row["spend"])
    return row


def account_credentials(platform):
    if platform == "TikTok":
        return os.getenv("TIKTOK_ACCESS_TOKEN", "").strip(), os.getenv("TIKTOK_ADVERTISER_ID", "").strip()
    return os.getenv("META_ACCESS_TOKEN", "").strip(), os.getenv("META_AD_ACCOUNT_ID", "").strip().replace("act_", "")


def tiktok_request(path, params):
    token, _ = account_credentials("TikTok")
    if not token:
        raise RuntimeError("Thiếu TIKTOK_ACCESS_TOKEN")
    r = requests.get(
        f"{TIKTOK_BASE}{path}",
        params=params,
        headers={"Access-Token": token, "Accept": "application/json"},
        timeout=40,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"TikTok HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    if data.get("code") not in (0, "0", None):
        raise RuntimeError(f"TikTok API {data.get('code')}: {data.get('message')}")
    return data


def tiktok_info():
    token, advertiser_id = account_credentials("TikTok")
    if not token or not advertiser_id:
        raise RuntimeError("Cần TIKTOK_ACCESS_TOKEN và TIKTOK_ADVERTISER_ID")
    payload = json.dumps([advertiser_id])
    data = tiktok_request("/advertiser/info/", {
        "advertiser_ids": payload,
        "fields": json.dumps(["advertiser_id", "name", "currency", "timezone", "status"]),
    })
    items = (data.get("data") or {}).get("list") or (data.get("data") or {}).get("data") or []
    item = items[0] if items else {}
    return item


def tiktok_report(start_date, end_date, dimensions, metrics, page_size=1000):
    advertiser_id = account_credentials("TikTok")[1]
    rows = []
    page = 1
    while True:
        params = {
            "advertiser_id": advertiser_id,
            "page": page,
            "page_size": page_size,
            "data_level": "AUCTION_CAMPAIGN",
            "report_type": "BASIC",
            "dimensions": json.dumps(dimensions),
            "metrics": json.dumps(metrics),
            "start_date": start_date,
            "end_date": end_date,
            "query_mode": "CHUNK",
        }
        data = tiktok_request("/report/integrated/get/", params)
        d = data.get("data") or {}
        page_info = d.get("page_info") or {}
        batch = d.get("list") or []
        rows.extend(batch)
        total = parse_int(page_info.get("total_number"), len(rows))
        if not batch or len(rows) >= total or len(batch) < page_size:
            break
        page += 1
        if page > 50:
            break
    return rows


def insert_campaign(campaign_key, name, platform, account_id):
    c = db_conn()
    c.execute("INSERT INTO campaigns(campaign_key,name,platform,account_id,active) VALUES(?,?,?,?,1) ON CONFLICT(campaign_key) DO UPDATE SET name=excluded.name,platform=excluded.platform,account_id=excluded.account_id,active=1", (campaign_key, name, platform, account_id))
    c.commit(); c.close()


def insert_metric(**m):
    c = db_conn()
    c.execute("""INSERT INTO metric_rows(campaign_key,ts,granularity,source,currency,spend,impressions,clicks,conversions,revenue,reach,frequency,ctr,cpc,cpm,cvr,roas,purchases,raw_json,pulled_at)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        m["campaign_key"], m["ts"], m["granularity"], m["source"], m.get("currency"), m.get("spend",0), m.get("impressions",0), m.get("clicks",0), m.get("conversions",0), m.get("revenue",0), m.get("reach",0), m.get("frequency",0), m.get("ctr"), m.get("cpc"), m.get("cpm"), m.get("cvr"), m.get("roas"), m.get("purchases"), json.dumps(m.get("raw",{}), ensure_ascii=False), m.get("pulled_at", iso_now())
    ))
    c.commit(); c.close()


def clear_recent(platform, since_dt):
    c = db_conn()
    c.execute("DELETE FROM metric_rows WHERE source=? AND ts>=?", (platform, since_dt.isoformat()))
    c.commit(); c.close()


def sync_tiktok():
    started = iso_now()
    c = db_conn(); cur = c.cursor(); cur.execute("INSERT INTO sync_runs(platform,started_at,status) VALUES(?,?,?)", ("TikTok", started, "running")); run_id = cur.lastrowid; c.commit(); c.close()
    try:
        info = tiktok_info()
        tz_name = info.get("timezone") or os.getenv("TIKTOK_TIMEZONE", "Asia/Ho_Chi_Minh")
        try: tz = ZoneInfo(tz_name)
        except Exception: tz = timezone(timedelta(hours=7))
        now_local = now_utc().astimezone(tz)
        start_date = (now_local.date() - timedelta(days=2)).isoformat()
        end_date = now_local.date().isoformat()

        # Hourly delivery metrics: supported through stat_time_hour.
        delivery_metrics = ["spend", "impressions", "clicks", "ctr", "cpc", "cpm"]
        hourly_rows = tiktok_report(start_date, end_date, ["campaign_id", "stat_time_hour"], delivery_metrics)
        # Conversion/revenue at TikTok Shop is kept on the platform-supported daily granularity.
        shop_metrics = ["onsite_shopping", "total_onsite_shopping_value", "onsite_shopping_roas"]
        daily_rows = tiktok_report(start_date, end_date, ["campaign_id", "stat_time_day"], shop_metrics)

        clear_recent("TikTok-hourly", now_utc() - timedelta(days=3))
        clear_recent("TikTok-daily", now_utc() - timedelta(days=3))
        count = 0
        for r in hourly_rows:
            cid = str(r.get("dimensions",{}).get("campaign_id") or r.get("campaign_id") or "unknown")
            name = str(r.get("metrics",{}).get("campaign_name") or r.get("campaign_name") or cid)
            dims = r.get("dimensions") or {}
            met = r.get("metrics") or r
            ts_local = safe_dt(dims.get("stat_time_hour"), tz)
            if not ts_local: continue
            key = f"TikTok:{cid}"
            insert_campaign(key,name,"TikTok",account_credentials("TikTok")[1])
            insert_metric(campaign_key=key, ts=ts_local.isoformat(), granularity="hour", source="TikTok-hourly", currency=info.get("currency"), spend=parse_num(met.get("spend")), impressions=parse_int(met.get("impressions")), clicks=parse_int(met.get("clicks")), ctr=parse_num(met.get("ctr"), None), cpc=parse_num(met.get("cpc"), None), cpm=parse_num(met.get("cpm"), None), raw=r)
            count += 1
        for r in daily_rows:
            cid = str(r.get("dimensions",{}).get("campaign_id") or r.get("campaign_id") or "unknown")
            dims = r.get("dimensions") or {}
            met = r.get("metrics") or r
            ts_local = safe_dt(dims.get("stat_time_day"), tz)
            if not ts_local: continue
            key = f"TikTok:{cid}"
            name = str(r.get("metrics",{}).get("campaign_name") or r.get("campaign_name") or cid)
            insert_campaign(key,name,"TikTok",account_credentials("TikTok")[1])
            purchases = parse_num(met.get("onsite_shopping"))
            revenue = parse_num(met.get("total_onsite_shopping_value"))
            roas = parse_num(met.get("onsite_shopping_roas"), None)
            insert_metric(campaign_key=key, ts=ts_local.isoformat(), granularity="day", source="TikTok-daily", currency=info.get("currency"), purchases=purchases, conversions=purchases, revenue=revenue, roas=roas, raw=r)
            count += 1
        finish = iso_now()
        c = db_conn(); c.execute("UPDATE sync_runs SET finished_at=?,status=?,message=?,rows_loaded=? WHERE id=?", (finish,"success",f"timezone={tz_name}",count,run_id)); c.commit(); c.close()
        return {"ok":True,"platform":"TikTok","rows":count,"timezone":tz_name,"pulled_at":finish}
    except Exception as e:
        c = db_conn(); c.execute("UPDATE sync_runs SET finished_at=?,status=?,message=? WHERE id=?", (iso_now(),"error",str(e)[:1000],run_id)); c.commit(); c.close()
        raise


def meta_request(path, params):
    token, account_id = account_credentials("Meta")
    if not token: raise RuntimeError("Thiếu META_ACCESS_TOKEN")
    p = dict(params); p["access_token"] = token
    r = requests.get(f"{META_BASE}{path}", params=p, timeout=40)
    if r.status_code >= 400:
        raise RuntimeError(f"Meta HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"Meta API: {data['error'].get('message','Unknown error')}")
    return data


def meta_insights(since_date, until_date):
    _, account_id = account_credentials("Meta")
    if not account_id: raise RuntimeError("Thiếu META_AD_ACCOUNT_ID")
    fields = ["campaign_id","campaign_name","account_currency","impressions","clicks","spend","reach","frequency","ctr","cpc","cpm","actions","action_values","purchase_roas"]
    params = {
        "level":"campaign",
        "time_range": json.dumps({"since": since_date, "until": until_date}),
        "breakdowns":"hourly_stats_aggregated_by_advertiser_time_zone",
        "fields": ",".join(fields),
        "limit": 500,
    }
    out=[]; url=f"/act_{account_id}/insights"
    while True:
        data=meta_request(url,params)
        out.extend(data.get("data",[]))
        nxt=(data.get("paging") or {}).get("next")
        if not nxt or len(out) > 10000: break
        # paging next already includes version and token parameters; use raw requests call securely.
        token=account_credentials("Meta")[0]
        rr=requests.get(nxt,headers={"Accept":"application/json"},timeout=40)
        if rr.status_code>=400: raise RuntimeError(f"Meta paging HTTP {rr.status_code}: {rr.text[:500]}")
        data=rr.json(); out.extend(data.get("data",[]))
        while (data.get("paging") or {}).get("next") and len(out)<=10000:
            rr=requests.get(data["paging"]["next"],timeout=40)
            if rr.status_code>=400: raise RuntimeError(f"Meta paging HTTP {rr.status_code}: {rr.text[:500]}")
            data=rr.json(); out.extend(data.get("data",[]))
        break
    return out


def extract_action(items, wanted):
    for x in items or []:
        if x.get("action_type") in wanted:
            return parse_num(x.get("value"))
    return 0.0


def sync_meta():
    started=iso_now(); c=db_conn(); cur=c.cursor(); cur.execute("INSERT INTO sync_runs(platform,started_at,status) VALUES(?,?,?)",("Meta",started,"running")); run_id=cur.lastrowid;c.commit();c.close()
    try:
        _, account_id=account_credentials("Meta")
        today=now_utc().date(); start=today-timedelta(days=2)
        rows=meta_insights(start.isoformat(),today.isoformat())
        clear_recent("Meta-hourly", now_utc()-timedelta(days=3)); count=0
        for r in rows:
            cid=str(r.get("campaign_id") or "unknown"); name=r.get("campaign_name") or cid
            key=f"Meta:{cid}"; insert_campaign(key,name,"Meta",account_id)
            hr=r.get("hourly_stats_aggregated_by_advertiser_time_zone")
            ts=safe_dt(hr)
            if not ts: ts=safe_dt(r.get("date_start"))
            if not ts: continue
            spend=parse_num(r.get("spend")); imps=parse_int(r.get("impressions")); clicks=parse_int(r.get("clicks")); reach=parse_int(r.get("reach")); freq=parse_num(r.get("frequency"))
            purchases=extract_action(r.get("actions"),{"omni_purchase","purchase","offsite_conversion.fb_pixel_purchase"})
            revenue=extract_action(r.get("action_values"),{"omni_purchase","purchase","offsite_conversion.fb_pixel_purchase"})
            if not revenue and r.get("purchase_roas") is not None:
                pr=parse_num(r.get("purchase_roas"),None); revenue=spend*pr if pr is not None else 0
            insert_metric(campaign_key=key,ts=ts.isoformat(),granularity="hour",source="Meta-hourly",currency=r.get("account_currency"),spend=spend,impressions=imps,clicks=clicks,conversions=purchases,revenue=revenue,reach=reach,frequency=freq,ctr=parse_num(r.get("ctr"),None),cpc=parse_num(r.get("cpc"),None),cpm=parse_num(r.get("cpm"),None),purchases=purchases,roas=ratio(revenue,spend),raw=r)
            count+=1
        finish=iso_now(); c=db_conn(); c.execute("UPDATE sync_runs SET finished_at=?,status=?,message=?,rows_loaded=? WHERE id=?",(finish,"success",f"graph_api={META_API_VERSION}",count,run_id));c.commit();c.close();return {"ok":True,"platform":"Meta","rows":count,"pulled_at":finish}
    except Exception as e:
        c=db_conn(); c.execute("UPDATE sync_runs SET finished_at=?,status=?,message=? WHERE id=?",(iso_now(),"error",str(e)[:1000],run_id));c.commit();c.close();raise


def sync_all():
    if not SYNC_LOCK.acquire(blocking=False): return
    try:
        results=[]
        if account_credentials("TikTok")[0] and account_credentials("TikTok")[1]:
            try: results.append(sync_tiktok())
            except Exception as e: results.append({"ok":False,"platform":"TikTok","error":str(e)})
        if account_credentials("Meta")[0] and account_credentials("Meta")[1]:
            try: results.append(sync_meta())
            except Exception as e: results.append({"ok":False,"platform":"Meta","error":str(e)})
        return results
    finally:
        SYNC_LOCK.release()


def latest_syncs():
    c=db_conn(); rows=c.execute("SELECT platform,MAX(finished_at) finished_at FROM sync_runs WHERE status='success' GROUP BY platform").fetchall(); errors=c.execute("SELECT platform,message,finished_at FROM sync_runs WHERE status='error' ORDER BY id DESC LIMIT 10").fetchall(); c.close();return {r['platform']:r['finished_at'] for r in rows}, [dict(r) for r in errors]


def get_rows(window_h, campaign_key=None, source_hourly_only=False):
    c=db_conn(); where="1=1"; args=[]
    if campaign_key:
        where += " AND campaign_key=?"; args.append(campaign_key)
    if source_hourly_only:
        where += " AND granularity='hour'"
    # Anchor to most recent hourly timestamp so incomplete current API periods remain consistent.
    latest=c.execute(f"SELECT MAX(ts) x FROM metric_rows WHERE {where}",args).fetchone()['x']
    if not latest: c.close(); return [], None
    anchor=safe_dt(latest); since=anchor-timedelta(hours=window_h)
    args2=args+[since.isoformat(),anchor.isoformat()]
    rows=c.execute(f"SELECT * FROM metric_rows WHERE {where} AND ts>=? AND ts<=? ORDER BY ts ASC",args2).fetchall(); c.close(); return [dict(r) for r in rows], anchor


def aggregate(rows):
    out=defaultdict(lambda:{"spend":0,"impressions":0,"clicks":0,"conversions":0,"revenue":0,"reach":0,"frequency_num":0,"frequency_den":0,"campaign_key":"","name":"","platform":"","currency":"","sources":set(),"granularities":set()})
    for r in rows:
        b=out[r['campaign_key']]; b['campaign_key']=r['campaign_key']; b['name']=campaign_name(r['campaign_key']); b['platform']=campaign_platform(r['campaign_key']); b['currency']=r.get('currency') or b['currency']; b['spend']+=r['spend'] or 0; b['impressions']+=r['impressions'] or 0; b['clicks']+=r['clicks'] or 0; b['conversions']+=r['conversions'] or 0; b['revenue']+=r['revenue'] or 0; b['reach']=max(b['reach'],r['reach'] or 0); b['frequency_num']+=(r['frequency'] or 0)*(r['reach'] or 0); b['frequency_den']+=(r['reach'] or 0); b['sources'].add(r['source']); b['granularities'].add(r['granularity'])
    ans=[]
    for b in out.values():
        b['frequency']=ratio(b['frequency_num'],b['frequency_den']) or 0
        for k in ('frequency_num','frequency_den'): b.pop(k,None)
        b=derive(b); b['sources']=sorted(b['sources']); b['granularities']=sorted(b['granularities']); ans.append(b)
    ans.sort(key=lambda x:x['spend'],reverse=True); return ans


def campaign_name(key):
    c=db_conn(); r=c.execute("SELECT name FROM campaigns WHERE campaign_key=?",(key,)).fetchone(); c.close(); return r['name'] if r else key

def campaign_platform(key):
    return key.split(":",1)[0] if ":" in key else ""


def same_window_rows(campaign_key, window_h):
    rows, anchor=get_rows(window_h,campaign_key,True)
    if not anchor:return [],[],None
    cur=rows
    prev_since=anchor-timedelta(hours=2*window_h); prev_anchor=anchor-timedelta(hours=window_h)
    c=db_conn(); rs=c.execute("SELECT * FROM metric_rows WHERE campaign_key=? AND granularity='hour' AND ts>=? AND ts<? ORDER BY ts ASC",(campaign_key,prev_since.isoformat(),prev_anchor.isoformat())).fetchall(); c.close(); return [dict(x) for x in cur],[dict(x) for x in rs],anchor


def agg_simple(rows):
    s={k:0 for k in ('spend','impressions','clicks','conversions','revenue','reach')}
    for r in rows:
        for k in s: s[k]+=r.get(k) or 0
    return derive(s)


def pct_change(a,b):
    if b in (None,0):return None
    return (a-b)/b*100


def diagnosis_for(cur,prev,settings):
    delta=float(settings.get('delta_alert_pct','20')); min_spend=float(settings.get('min_spend_for_alert','100000')); min_clicks=float(settings.get('min_clicks_for_alert','30'))
    issues=[]; evidence=[]; checks=[]; sev="OK"
    metrics=['spend','impressions','clicks','ctr','cpc','cpm','cvr','roas']
    changes={m:pct_change(cur.get(m),prev.get(m)) for m in metrics}
    def add(s,issue,ev,check):
        nonlocal sev
        issues.append(issue); evidence.append(ev); checks.append(check); sev=s if s=="CRITICAL" or (s=="HIGH" and sev=="OK") else sev
    enough=cur.get('spend',0)>=min_spend or cur.get('clicks',0)>=min_clicks
    if not prev or not enough:
        return {"severity":"DATA_LIMITED","issues":["Chưa đủ dữ liệu baseline"],"evidence":["Cần ít nhất một cửa sổ trước tương đương và đủ volume để tránh kết luận trên mẫu quá nhỏ."],"checks":["Chờ thêm dữ liệu hoặc giảm ngưỡng volume nếu account nhỏ."],"changes":changes,"basis":"So sánh với cửa sổ trước cùng độ dài; chưa đủ mẫu để kết luận."}
    if changes['ctr'] is not None and changes['ctr'] <= -delta:
        add("HIGH","CTR giảm đáng kể",f"CTR {cur['ctr']:.2f}% vs {prev['ctr']:.2f}% ({changes['ctr']:.1f}%).","Kiểm tra creative/hook, format, placement và tệp bị fatigue.")
    if changes['cpm'] is not None and changes['cpm'] >= delta:
        add("HIGH","CPM tăng đáng kể",f"CPM {cur['cpm']:.0f} vs {prev['cpm']:.0f} ({changes['cpm']:.1f}%).","Kiểm tra auction pressure, audience size/overlap, placement và thay đổi bid/budget.")
    if changes['cpc'] is not None and changes['cpc'] >= delta:
        add("HIGH","CPC tăng đáng kể",f"CPC {cur['cpc']:.0f} vs {prev['cpc']:.0f} ({changes['cpc']:.1f}%).","Xác định CPC tăng do CTR giảm hay CPM tăng; ưu tiên sửa nguyên nhân cấp funnel phía trước.")
    if changes['cvr'] is not None and changes['cvr'] <= -delta and cur['clicks']>=min_clicks:
        add("HIGH","CVR giảm đáng kể",f"CVR {cur['cvr']:.2f}% vs {prev['cvr']:.2f}% ({changes['cvr']:.1f}%).","Đối chiếu landing/product page, giá/voucher, checkout và tracking event.")
    if changes['roas'] is not None and changes['roas'] <= -delta and cur['spend']>=min_spend:
        add("CRITICAL","ROAS giảm đáng kể",f"ROAS {cur['roas']:.2f}x vs {prev['roas']:.2f}x ({changes['roas']:.1f}%).","Phân rã xem CTR/CPC/CVR/giá trị đơn hàng hay attribution thay đổi là yếu tố kéo ROAS.")
    if not issues:
        evidence.append("Không thấy biến động vượt guardrail tương đối so với cửa sổ trước.")
    # Cause hypotheses are explicitly derived from observed relationships, not asserted as facts.
    if changes['ctr'] is not None and changes['ctr'] <= -delta and changes['cpm'] is not None and abs(changes['cpm']) < delta/2:
        evidence.append("Mẫu hình: CTR giảm trong khi CPM khá ổn định → tín hiệu ưu tiên kiểm tra creative/audience match.")
    if changes['cpm'] is not None and changes['cpm'] >= delta and changes['ctr'] is not None and abs(changes['ctr']) < delta/2:
        evidence.append("Mẫu hình: CPM tăng trong khi CTR khá ổn định → tín hiệu ưu tiên kiểm tra auction/audience/placement.")
    if changes['cvr'] is not None and changes['cvr'] <= -delta and changes['ctr'] is not None and abs(changes['ctr']) < delta/2:
        evidence.append("Mẫu hình: click efficiency ổn nhưng CVR giảm → ưu tiên kiểm tra offer/landing/tracking.")
    return {"severity":sev,"issues":issues or ["Không có vấn đề nổi bật theo guardrail tương đối"],"evidence":evidence,"checks":checks or ["Tiếp tục theo dõi cửa sổ kế tiếp."],"changes":changes,"basis":"Biến động % so với cửa sổ ngay trước cùng độ dài; không dùng benchmark thị trường mặc định."}


def overall_metrics(rows):
    return agg_simple(rows) if rows else derive({'spend':0,'impressions':0,'clicks':0,'conversions':0,'revenue':0,'reach':0})


def build_dashboard(window_h):
    rows,anchor=get_rows(window_h,None,True)
    data=aggregate(rows)
    all_daily=[]
    if data:
        # Attach latest platform-attributed daily conversion fields without pretending they are hourly.
        c=db_conn(); latest_day=(anchor or now_utc()).date().isoformat();
        dr=c.execute("SELECT * FROM metric_rows WHERE granularity='day' AND ts<=? ORDER BY ts DESC",((anchor or now_utc()).isoformat(),)).fetchall();c.close();
        daily_map={}
        for r in dr:
            if r['campaign_key'] not in daily_map and r['ts'][:10] >= latest_day:
                daily_map[r['campaign_key']]=dict(r)
        for d in data:
            x=daily_map.get(d['campaign_key']);
            if x:
                d['attributed_revenue_available']=x['revenue']; d['attributed_roas_available']=x['roas']; d['purchases_available']=x['purchases']; d['conversion_granularity']=x['granularity'];
            else:
                d['attributed_revenue_available']=None;d['attributed_roas_available']=None;d['purchases_available']=None;d['conversion_granularity']=None
    diag=[]
    s=get_settings()
    for d in data:
        cur,prev,_=same_window_rows(d['campaign_key'],window_h)
        ca=agg_simple(cur); pa=agg_simple(prev) if prev else {}
        diag.append({"campaign_key":d['campaign_key'],"name":d['name'],"platform":d['platform'],"diagnosis":diagnosis_for(ca,pa,s),"current":ca,"previous":pa})
    return data,diag,anchor


def ai_analyze(payload):
    key=os.getenv("OPENAI_API_KEY","").strip()
    if not key: raise RuntimeError("Thiếu OPENAI_API_KEY")
    model=get_settings().get('ai_model',os.getenv('OPENAI_MODEL','gpt-5.6-luna'))
    system=("Bạn là chuyên gia phân tích quảng cáo. Chỉ sử dụng số liệu trong JSON được cung cấp. "
            "Không bịa benchmark thị trường, không khẳng định nhân quả khi chỉ có dữ liệu aggregate. "
            "Mỗi nguyên nhân phải ghi là 'giả thuyết' và nêu bằng chứng quan sát được. Trả lời tiếng Việt, ngắn gọn, gồm: vấn đề, bằng chứng, giả thuyết nguyên nhân, việc cần kiểm tra, hành động đề xuất.")
    body={"model":model,"input":[{"role":"system","content":[{"type":"input_text","text":system}]},{"role":"user","content":[{"type":"input_text","text":json.dumps(payload,ensure_ascii=False)}]}],"max_output_tokens":1000}
    r=requests.post(f"{OPENAI_BASE}/responses",headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},json=body,timeout=60)
    if r.status_code>=400: raise RuntimeError(f"OpenAI HTTP {r.status_code}: {r.text[:500]}")
    j=r.json(); text=j.get('output_text')
    if not text:
        parts=[]
        for item in j.get('output',[]):
            for c in item.get('content',[]):
                if c.get('type')=='output_text': parts.append(c.get('text',''))
        text='\n'.join(parts)
    return text.strip()


def openai_video(image_bytes,prompt,seconds=8,size="720x1280"):
    key=os.getenv("OPENAI_API_KEY","").strip()
    if not key: raise RuntimeError("Thiếu OPENAI_API_KEY")
    ext=".png"; mime="image/png"
    try:
        im=Image.open(io.BytesIO(image_bytes)); ext="."+(im.format or "PNG").lower(); mime=Image.MIME.get(im.format or "PNG","image/png")
    except Exception: pass
    files={"input_reference":(f"reference{ext}",image_bytes,mime)}
    data={"model":os.getenv("OPENAI_VIDEO_MODEL","sora-2"),"prompt":prompt,"seconds":str(seconds),"size":size}
    r=requests.post(f"{OPENAI_BASE}/videos",headers={"Authorization":f"Bearer {key}"},data=data,files=files,timeout=90)
    if r.status_code>=400: raise RuntimeError(f"OpenAI video HTTP {r.status_code}: {r.text[:600]}")
    return r.json()


def poll_openai_video(video_id, max_wait=180):
    key=os.getenv("OPENAI_API_KEY","").strip()
    if not key: raise RuntimeError("Thiếu OPENAI_API_KEY")
    deadline=time.time()+max_wait
    while time.time()<deadline:
        r=requests.get(f"{OPENAI_BASE}/videos/{video_id}",headers={"Authorization":f"Bearer {key}"},timeout=40)
        if r.status_code>=400: raise RuntimeError(f"OpenAI video status HTTP {r.status_code}: {r.text[:500]}")
        j=r.json(); status=j.get('status')
        if status=='completed':
            rr=requests.get(f"{OPENAI_BASE}/videos/{video_id}/content",headers={"Authorization":f"Bearer {key}"},timeout=120)
            if rr.status_code>=400: raise RuntimeError(f"OpenAI content HTTP {rr.status_code}: {rr.text[:500]}")
            fn=f"sora_{video_id}.mp4"; (MEDIA/fn).write_bytes(rr.content); return {"file":fn,"meta":j}
        if status in ('failed','cancelled'): raise RuntimeError((j.get('error') or {}).get('message') or f"Video status {status}")
        time.sleep(5)
    raise RuntimeError("Video generation chưa hoàn tất trong thời gian chờ 180 giây; hãy dùng video_id để poll lại.")


def import_csv_text(text, platform, mapping=None):
    # Generic importer for real exports; heuristics prefer platform-native field names.
    reader=csv.DictReader(io.StringIO(text)); count=0
    mapping=mapping or {}
    norm=lambda s: re.sub(r'[^a-z0-9]','',str(s).lower())
    rows=list(reader)
    for raw in rows:
        keys={norm(k):k for k in raw.keys()}
        def val(cands):
            for cand in cands:
                if cand in mapping and mapping[cand] in raw:return raw[mapping[cand]]
                if norm(cand) in keys:return raw[keys[norm(cand)]]
            return None
        name=val(['campaign_name','campaign name','campaign','campaignname'])
        if not name: continue
        ts=val(['stat_time_hour','hour','date','date_start','start_date','time']) or iso_now()
        dt=safe_dt(ts)
        if not dt: dt=now_utc()
        spend=parse_num(val(['spend','cost','amount_spent']))
        imps=parse_int(val(['impressions']))
        clicks=parse_int(val(['clicks','destination_clicks','link_clicks']))
        conv=parse_num(val(['conversions','conversion','purchases','onsite_shopping','complete_payment']))
        revenue=parse_num(val(['revenue','purchase_value','conversion_value','total_onsite_shopping_value','action_values']))
        currency=val(['currency','account_currency'])
        key=f"CSV:{platform}:{norm(name)}"; insert_campaign(key,str(name),platform,"csv")
        insert_metric(campaign_key=key,ts=dt.isoformat(),granularity="hour",source=f"CSV-{platform}",currency=currency,spend=spend,impressions=imps,clicks=clicks,conversions=conv,revenue=revenue,raw=raw)
        count+=1
    return count


def env_status():
    return {
        "TikTok": {"configured": bool(account_credentials('TikTok')[0] and account_credentials('TikTok')[1]),"advertiser_id": bool(account_credentials('TikTok')[1])},
        "Meta": {"configured": bool(account_credentials('Meta')[0] and account_credentials('Meta')[1]),"ad_account_id": bool(account_credentials('Meta')[1])},
        "OpenAI": {"configured": bool(os.getenv('OPENAI_API_KEY'))},
    }

HTML = r'''<!doctype html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AdPulse AI Monitor V2</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root{--bg:#07111e;--card:#0d1b2c;--card2:#132742;--line:#203754;--text:#edf5ff;--muted:#91a8c5;--blue:#65a8ff;--green:#39d58a;--warn:#ffbf69;--bad:#ff7180}*{box-sizing:border-box}body{margin:0;background:linear-gradient(140deg,#06101b,#0a1524 60%,#0e1b2d);color:var(--text);font:14px Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial}header{position:sticky;top:0;z-index:10;background:rgba(6,16,27,.94);backdrop-filter:blur(12px);border-bottom:1px solid var(--line);padding:16px 22px;display:flex;align-items:center;justify-content:space-between}.brand{font-size:20px;font-weight:850}.brand span{color:var(--blue)}.status{padding:7px 10px;border-radius:999px;border:1px solid #2a4663;color:var(--muted);font-size:12px}main{max-width:1550px;margin:auto;padding:20px}.toolbar{display:flex;gap:9px;align-items:center;flex-wrap:wrap}.seg{display:flex;border:1px solid var(--line);border-radius:10px;overflow:hidden;background:var(--card)}.seg button{border:0;background:transparent;color:var(--muted);padding:10px 15px;cursor:pointer}.seg button.active{background:var(--blue);color:#06111e;font-weight:800}.btn,.select,input,textarea{background:#0b1727;border:1px solid var(--line);color:var(--text);border-radius:10px;padding:10px 12px}.btn{cursor:pointer}.btn.primary{background:var(--blue);color:#06111e;border-color:var(--blue);font-weight:800}.spacer{flex:1}.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-top:16px}.kpi{background:linear-gradient(160deg,var(--card),#0a1728);border:1px solid var(--line);border-radius:14px;padding:14px;min-height:112px}.kpi .label{font-size:12px;color:var(--muted)}.kpi .v{font-size:25px;font-weight:850;margin-top:9px}.kpi .sub{font-size:11px;color:var(--muted);margin-top:3px}.layout{display:grid;grid-template-columns:1.6fr 1fr;gap:16px}.panel{background:rgba(13,27,44,.9);border:1px solid var(--line);border-radius:14px;padding:16px;margin-top:16px}.panel h3{margin:0 0 12px;font-size:14px}.chart{height:320px}.table{width:100%;border-collapse:collapse;font-size:12px}.table th,.table td{padding:10px 8px;border-bottom:1px solid #1b2d45;text-align:left;white-space:nowrap}.table th{color:var(--muted)}.tag{display:inline-block;border-radius:7px;padding:4px 7px;font-size:10px;font-weight:800}.OK{background:#133725;color:#99efbf}.HIGH{background:#4b3418;color:#ffd08f}.CRITICAL{background:#511a24;color:#ffb3bc}.DATA_LIMITED{background:#2a3040;color:#cfd9e7}.muted{color:var(--muted)}.issue{border:1px solid var(--line);padding:12px;border-radius:12px;background:#0b1727;margin:8px 0}.issue .row{margin-top:6px;line-height:1.5;font-size:12px}.note{color:var(--muted);font-size:11px;line-height:1.55}.form{display:grid;grid-template-columns:1fr 1fr;gap:10px}.form label{font-size:11px;color:var(--muted)}.form input,.form textarea{width:100%;margin-top:5px}.full{grid-column:1/-1}.dataBadge{font-size:10px;color:#8db7e8}.error{color:#ff9aa5}.success{color:#98f0bd}.hidden{display:none}.footer{text-align:center;color:#66809f;font-size:11px;padding:20px}.sourceBox{display:grid;grid-template-columns:1fr 1fr;gap:12px}.sourceCard{border:1px solid var(--line);border-radius:12px;padding:13px;background:#0b1727}.sourceCard h4{margin:0 0 8px}.sourceCard code{font-size:10px}.smallgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.alertline{padding:8px 10px;border-radius:9px;background:#0a1625;border:1px solid var(--line);font-size:11px;margin-top:8px}@media(max-width:1150px){.grid{grid-template-columns:repeat(3,1fr)}.layout{grid-template-columns:1fr}}@media(max-width:680px){main{padding:12px}.grid{grid-template-columns:repeat(2,1fr)}.form,.sourceBox,.smallgrid{grid-template-columns:1fr}}
</style></head><body>
<header><div class="brand">AdPulse <span>AI Monitor V2</span></div><div class="status" id="status">ĐANG KIỂM TRA...</div></header>
<main>
<div class="toolbar"><div class="seg"><button class="win active" data-w="1">1h</button><button class="win" data-w="3">3h</button><button class="win" data-w="5">5h</button></div><span class="muted">Polling <b id="pollTxt">300s</b></span><button class="btn" onclick="syncNow()">↻ Đồng bộ dữ liệu thật</button><button class="btn" onclick="location.href='#sources'">Nguồn dữ liệu</button><div class="spacer"></div><button class="btn primary" onclick="location.href='#video'">+ Tạo video AI</button></div>
<div id="banner" class="panel"></div>
<div class="grid" id="kpis"></div>
<div class="layout"><div class="panel"><h3>Hiệu quả theo campaign — cửa sổ phân tích</h3><div id="roasChart" class="chart"></div></div><div class="panel"><h3>Spend vs Revenue — lưu ý về attribution</h3><div id="spendChart" class="chart"></div><p class="note">Revenue/ROAS chỉ được hiển thị theo độ phân giải mà nền tảng thực sự cung cấp. Không nội suy doanh thu theo giờ khi API không cung cấp số liệu theo giờ.</p></div></div>
<div class="panel"><h3>Campaign monitoring</h3><div style="overflow:auto"><table class="table"><thead><tr><th>Campaign</th><th>Nguồn</th><th>Spend</th><th>Impr.</th><th>CTR</th><th>CPC</th><th>CPM</th><th>CVR</th><th>ROAS</th><th>Data basis</th><th>Status</th></tr></thead><tbody id="rows"></tbody></table></div></div>
<div class="panel"><h3>Phân tích: vấn đề → bằng chứng → giả thuyết → kiểm tra</h3><div id="diagnosis"></div></div>
<div class="panel"><h3>Thiết lập guardrail</h3><div class="form"><div><label>Delta cảnh báo (%)</label><input id="delta_alert_pct" type="number"></div><div><label>Min spend cửa sổ (VNĐ)</label><input id="min_spend_for_alert" type="number"></div><div><label>Min clicks cửa sổ</label><input id="min_clicks_for_alert" type="number"></div><div><label>Baseline hours</label><input id="baseline_hours" type="number"></div><div><label>Target ROAS (business target)</label><input id="target_roas" type="number" step="0.1"></div><div><label>Target ROAS enabled (0/1)</label><input id="target_roas_enabled" type="number" min="0" max="1"></div><div><label>Polling seconds</label><input id="poll_seconds" type="number"></div></div><br><button class="btn primary" onclick="saveSettings()">Lưu cấu hình</button><p class="note">Guardrail không phải benchmark thị trường. Các cảnh báo mặc định dựa trên biến động so với cửa sổ trước cùng độ dài. Target ROAS chỉ có hiệu lực khi bạn chủ động bật.</p></div>
<div class="panel" id="sources"><h3>Kết nối dữ liệu thật</h3><div class="sourceBox"><div class="sourceCard"><h4>TikTok Ads</h4><div id="ttState" class="muted">...</div><p class="note">Render Environment Variables:</p><code>TIKTOK_ACCESS_TOKEN</code><br><code>TIKTOK_ADVERTISER_ID</code><p class="note">Bản này dùng TikTok API for Business reporting. Delivery metrics lấy theo giờ; TikTok Shop purchase/revenue/ROAS giữ nguyên độ phân giải nền tảng hỗ trợ, không bịa số theo giờ.</p></div><div class="sourceCard"><h4>Meta Ads</h4><div id="metaState" class="muted">...</div><p class="note">Render Environment Variables:</p><code>META_ACCESS_TOKEN</code><br><code>META_AD_ACCOUNT_ID</code><br><code>META_GRAPH_VERSION</code><p class="note">Meta Insights dùng breakdown theo giờ của advertiser time zone. Purchase/revenue lấy từ action/action_values/purchase_roas khi nền tảng trả về.</p></div><div class="sourceCard"><h4>OpenAI</h4><div id="aiState" class="muted">...</div><p class="note">Render Environment Variable:</p><code>OPENAI_API_KEY</code><p class="note">Dùng cho AI explanation và Sora 2 video generation. API key chỉ nằm ở server.</p></div></div></div>
<div class="panel"><h3>Import CSV dữ liệu thật (dự phòng)</h3><input id="csvFile" type="file" accept=".csv,text/csv"><select id="csvPlatform" class="select"><option>TikTok</option><option>Meta</option></select><button class="btn" onclick="importCSV()">Import CSV</button><div id="csvMsg" class="note"></div></div>
<div class="panel" id="video"><h3>AI Video — Sora 2 + ảnh tham chiếu</h3><div class="form"><div class="full"><label>Ảnh sản phẩm</label><input id="img" type="file" accept="image/*"></div><div class="full"><label>Prompt</label><textarea id="prompt">Video quảng cáo dọc 9:16 cho sản phẩm boxer nam, giữ đúng hình dáng và màu sắc sản phẩm từ ảnh tham chiếu, camera chuyển động mượt, premium, realistic, clean ecommerce lighting, nhấn mạnh chất liệu ice silk/siêu mềm và co giãn 4 chiều.</textarea></div><div><label>Model</label><select id="videoModel" class="select" style="width:100%"><option value="sora-2">Sora 2</option><option value="sora-2-pro">Sora 2 Pro</option></select></div><div><label>Seconds</label><select id="videoSeconds" class="select" style="width:100%"><option value="4">4</option><option value="8" selected>8</option><option value="12">12</option></select></div><div><label>Size</label><select id="videoSize" class="select" style="width:100%"><option value="720x1280" selected>720x1280</option><option value="1280x720">1280x720</option><option value="1024x1792">1024x1792</option><option value="1792x1024">1792x1024</option></select></div><div style="display:flex;align-items:end"><button class="btn primary" style="width:100%" onclick="makeVideo()">Generate video</button></div></div><div id="videoOut" class="note"></div></div>
<div class="footer">AdPulse AI Monitor V2 • Live-only • Không seed dữ liệu giả • Phân tích có nguồn và độ phân giải dữ liệu</div>
</main>
<script>
let windowHours=1,timer=null;
const fmt=n=>new Intl.NumberFormat('vi-VN').format(Math.round(n||0));const money=n=>n==null?'—':fmt(n)+' ₫';
async function api(path,opt){const r=await fetch(path,opt);const j=await r.json();if(!r.ok||j.error)throw new Error(j.error||'Request failed');return j}
async function load(){try{const j=await api('/api/dashboard?window='+windowHours);document.getElementById('status').textContent=j.status;document.getElementById('pollTxt').textContent=j.settings.poll_seconds+'s';document.getElementById('ttState').textContent=j.env.TikTok.configured?'Đã cấu hình credentials':'Chưa cấu hình credentials';document.getElementById('metaState').textContent=j.env.Meta.configured?'Đã cấu hình credentials':'Chưa cấu hình credentials';document.getElementById('aiState').textContent=j.env.OpenAI.configured?'Đã cấu hình OPENAI_API_KEY':'Chưa cấu hình OPENAI_API_KEY';
 const o=j.overall;const cards=[['Spend',money(o.spend),'Cửa sổ '+windowHours+'h'],['Impressions',fmt(o.impressions),'Dữ liệu hourly'],['Clicks',fmt(o.clicks),'Dữ liệu hourly'],['CTR',o.ctr==null?'—':o.ctr.toFixed(2)+'%',''],['CPC',money(o.cpc),''],['CPM',money(o.cpm),'']];document.getElementById('kpis').innerHTML=cards.map(x=>`<div class="kpi"><div class="label">${x[0]}</div><div class="v">${x[1]}</div><div class="sub">${x[2]}</div></div>`).join('');
 let banner=`<div><b>Data status:</b> ${j.status} · <span class="dataBadge">${j.anchor||'chưa có dữ liệu'}</span></div><div class="note" style="margin-top:6px">${j.sourceNote}</div>`; if(j.errors.length)banner+=j.errors.map(e=>`<div class="alertline error">${e.platform}: ${e.message}</div>`).join('');document.getElementById('banner').innerHTML=banner;
 document.getElementById('rows').innerHTML=j.data.map(d=>{const diag=j.diag.find(x=>x.campaign_key===d.campaign_key)?.diagnosis||{severity:'OK'};const roas=d.attributed_roas_available==null?'—':d.attributed_roas_available.toFixed(2)+'x';const basis=d.conversion_granularity?d.conversion_granularity:'hourly';return `<tr><td>${d.name}</td><td>${d.platform}</td><td>${money(d.spend)}</td><td>${fmt(d.impressions)}</td><td>${d.ctr==null?'—':d.ctr.toFixed(2)+'%'}</td><td>${money(d.cpc)}</td><td>${money(d.cpm)}</td><td>${d.cvr==null?'—':d.cvr.toFixed(2)+'%'}</td><td>${roas}</td><td>${basis}</td><td><span class="tag ${diag.severity}">${diag.severity}</span></td></tr>`}).join('')||`<tr><td colspan="11" class="muted">Chưa có dữ liệu live. Cấu hình credentials rồi bấm “Đồng bộ dữ liệu thật”.</td></tr>`;
 document.getElementById('diagnosis').innerHTML=j.diag.map(d=>{const a=d.diagnosis;return `<div class="issue"><div><b>${d.name}</b> <span class="tag ${a.severity}">${a.severity}</span></div><div class="row"><b>Vấn đề:</b> ${a.issues.join(' · ')}</div><div class="row"><b>Bằng chứng:</b> ${a.evidence.join(' · ')}</div><div class="row"><b>Kiểm tra:</b> ${a.checks.join(' · ')}</div><div class="row"><span class="muted">Cơ sở:</span> ${a.basis}</div><button class="btn" onclick='askAI(${JSON.stringify(d).replaceAll("'","&#39;")})'>AI giải thích sâu</button><div id="ai_${d.campaign_key.replace(/[^a-z0-9]/gi,'_')}" class="row"></div></div>`}).join('')||'<div class="note">Chưa có diagnosis vì chưa có dữ liệu đủ để so sánh.</div>';
 plot(j.data);['delta_alert_pct','min_spend_for_alert','min_clicks_for_alert','baseline_hours','target_roas','target_roas_enabled','poll_seconds'].forEach(k=>document.getElementById(k).value=j.settings[k]);if(timer)clearTimeout(timer);timer=setTimeout(load,Math.max(20,Number(j.settings.poll_seconds))*1000);
 }catch(e){document.getElementById('status').textContent='ERROR';document.getElementById('banner').innerHTML='<div class="error">'+e.message+'</div>'}}
function plot(ds){const names=ds.map(d=>d.name),r=ds.map(d=>d.attributed_roas_available);Plotly.react('roasChart',[{x:names,y:r,type:'bar',text:r.map(v=>v==null?'':v.toFixed(2)+'x'),textposition:'auto'}],{margin:{l:35,r:10,t:10,b:90},paper_bgcolor:'transparent',plot_bgcolor:'transparent',font:{color:'#cbd8ea'},yaxis:{title:'Platform ROAS'},xaxis:{tickangle:-35}},{displayModeBar:false});Plotly.react('spendChart',[{x:names,y:ds.map(d=>d.spend),type:'bar',name:'Spend'},{x:names,y:ds.map(d=>d.attributed_revenue_available||0),type:'bar',name:'Attributed revenue (available granularity)'}],{barmode:'group',margin:{l:55,r:10,t:10,b:90},paper_bgcolor:'transparent',plot_bgcolor:'transparent',font:{color:'#cbd8ea'},yaxis:{title:'VNĐ'},xaxis:{tickangle:-35}},{displayModeBar:false})}
async function syncNow(){document.getElementById('status').textContent='ĐANG ĐỒNG BỘ...';try{const j=await api('/api/sync',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});document.getElementById('status').textContent='ĐỒNG BỘ XONG';load()}catch(e){document.getElementById('status').textContent='SYNC ERROR';alert(e.message)}}
async function saveSettings(){const b={};['delta_alert_pct','min_spend_for_alert','min_clicks_for_alert','baseline_hours','target_roas','target_roas_enabled','poll_seconds'].forEach(k=>b[k]=document.getElementById(k).value);await api('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});load()}
async function importCSV(){const f=document.getElementById('csvFile').files[0];if(!f){alert('Chọn CSV');return}const text=await f.text();const j=await api('/api/import_csv',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({platform:document.getElementById('csvPlatform').value,text})});document.getElementById('csvMsg').textContent='Đã import '+j.rows+' dòng dữ liệu thật.';load()}
async function askAI(d){const id='ai_'+d.campaign_key.replace(/[^a-z0-9]/gi,'_');document.getElementById(id).textContent='AI đang phân tích...';try{const j=await api('/api/ai/diagnose',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});document.getElementById(id).textContent=j.text}catch(e){document.getElementById(id).textContent='AI error: '+e.message}}
async function makeVideo(){const f=document.getElementById('img').files[0];if(!f){alert('Chọn ảnh');return}document.getElementById('videoOut').textContent='Đang tạo video...';const b64=await new Promise((res,rej)=>{const r=new FileReader();r.onload=()=>res(r.result.split(',')[1]);r.onerror=rej;r.readAsDataURL(f)});try{const j=await api('/api/video',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({image_base64:b64,prompt:document.getElementById('prompt').value,model:document.getElementById('videoModel').value,seconds:Number(document.getElementById('videoSeconds').value),size:document.getElementById('videoSize').value})});document.getElementById('videoOut').innerHTML=`<video controls style="width:100%;max-width:520px;border-radius:12px;margin-top:12px" src="/media/${j.file}"></video><div class="note"><a href="/media/${j.file}" download>Tải MP4</a></div>`}catch(e){document.getElementById('videoOut').textContent='Video error: '+e.message}}
document.querySelectorAll('.win').forEach(b=>b.onclick=()=>{document.querySelectorAll('.win').forEach(x=>x.classList.remove('active'));b.classList.add('active');windowHours=Number(b.dataset.w);load()});load();
</script></body></html>'''


class Handler(BaseHTTPRequestHandler):
    def json(self,obj,status=200):
        b=json.dumps(obj,ensure_ascii=False).encode();self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
    def body(self):
        n=int(self.headers.get('Content-Length','0'));return self.rfile.read(n)
    def do_GET(self):
        p=urlparse(self.path)
        if p.path=='/':
            b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b);return
        if p.path.startswith('/media/'):
            fn=os.path.basename(p.path);fp=MEDIA/fn
            if not fp.exists():self.send_error(404);return
            data=fp.read_bytes();self.send_response(200);self.send_header('Content-Type',mimetypes.guess_type(str(fp))[0] or 'application/octet-stream');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        if p.path=='/api/dashboard':
            q=parse_qs(p.query); w=int(q.get('window',['1'])[0]); data,diag,anchor=build_dashboard(w); overall=overall_metrics(get_rows(w,None,True)[0]); syncs,errors=latest_syncs(); env=env_status(); configured=any(v['configured'] for v in env.values() if isinstance(v,dict)); status='LIVE DATA' if data else ('READY — CHỜ KẾT NỐI' if not configured else 'CONNECTED — CHƯA CÓ DỮ LIỆU')
            note='Số liệu chỉ hiển thị từ API/CSV bạn kết nối. Phân tích cửa sổ 1h/3h/5h dùng dữ liệu hourly. TikTok Shop conversion/revenue/ROAS có thể chỉ được nền tảng cung cấp ở granularity ngày; hệ thống giữ nguyên granularity đó, không nội suy.'
            self.json({'status':status,'settings':get_settings(),'overall':overall,'data':data,'diag':diag,'anchor':anchor.isoformat() if anchor else None,'syncs':syncs,'errors':errors,'env':env,'sourceNote':note});return
        self.send_error(404)
    def do_POST(self):
        p=urlparse(self.path)
        try: body=json.loads(self.body().decode() or '{}')
        except Exception:self.json({'error':'JSON invalid'},400);return
        try:
            if p.path=='/api/sync': self.json({'results':sync_all()});return
            if p.path=='/api/settings':set_settings(body);self.json({'ok':True});return
            if p.path=='/api/ai/diagnose':self.json({'text':ai_analyze(body)});return
            if p.path=='/api/import_csv':
                rows=import_csv_text(body.get('text',''),body.get('platform','CSV'));self.json({'ok':True,'rows':rows});return
            if p.path=='/api/video':
                img=base64.b64decode(body.get('image_base64',''));prompt=body.get('prompt','');model=body.get('model','sora-2');seconds=int(body.get('seconds',8));size=body.get('size','720x1280');
                os.environ['OPENAI_VIDEO_MODEL']=model
                job=openai_video(img,prompt,seconds,size); result=poll_openai_video(job['id']); self.json({'ok':True,**result,'job_id':job['id']});return
            self.send_error(404)
        except Exception as e:self.json({'error':str(e)},500)
    def log_message(self,*args):pass


def background_loop():
    while True:
        try:
            poll=int(get_settings().get('poll_seconds','300'))
            sync_all()
        except Exception:
            pass
        time.sleep(max(60,poll))


def main():
    init_db()
    threading.Thread(target=background_loop,daemon=True).start()
    server=ThreadingHTTPServer(('0.0.0.0',PORT),Handler)
    print(f'AdPulse AI Monitor V2 listening on 0.0.0.0:{PORT}')
    server.serve_forever()

if __name__=='__main__':main()
