#!/usr/bin/env python3
"""
ADS DASHBOARD — CRM Sync Server (Cloud / Render.com)
──────────────────────────────────────────────────────
Це хмарна версія crm_proxy.py.
Запускається на Render.com і доступна з будь-якого пристрою.

Локальна версія (crm_proxy.py) залишається без змін.
"""

import http.server
import urllib.request
import urllib.error
import json
import os
import time
import threading
from datetime import datetime, timedelta, timezone

# ══════════════════════════════════════════════
#  НАЛАШТУВАННЯ  (ті самі, що в crm_proxy.py)
# ══════════════════════════════════════════════
API_KEY  = "1PtL9c3Qc1S8iiRDgazx2Yv1"
CRM_BASE = "https://api.keepincrm.com/v1"
DELAY_MS = 200   # Зменшено з 650 мс → повний синк ~4000 лідів займе ~30-40 сек замість 150+
INC_DAYS = 14
PORT     = int(os.environ.get('PORT', 8765))   # Render задає PORT сам
# ══════════════════════════════════════════════

# На Render файлова система тимчасова — використовуємо /tmp
DATA_FILE = '/tmp/crm_data.json'

CATMAP = {'А': 'A', 'В': 'B', 'С': 'C', 'A': 'A', 'B': 'B', 'C': 'C'}


def iso_to_dmy(iso):
    if not iso: return ''
    try:
        dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
        offset_h = 3 if 4 <= dt.month <= 10 else 2
        dt_ua = dt.astimezone(timezone(timedelta(hours=offset_h)))
        return f"{dt_ua.day:02d}.{dt_ua.month:02d}.{dt_ua.year}"
    except Exception:
        return ''


def parse_crm_date(s):
    if not s: return ''
    s = str(s).strip()
    if len(s) >= 10 and s[2] == '.' and s[5] == '.': return s[:10]
    return iso_to_dmy(s)


def crm_item_to_row(item, status, is_realizatsiya=False):
    cf = item.get('custom_fields') or {}
    cat_raw = str(cf.get('Категорія клієнта') or '').strip()
    archive_status = item.get('archive_status') or {}
    stage = item.get('stage') or {}
    responsible = item.get('main_responsible') or {}

    if is_realizatsiya:
        date_val = (parse_crm_date(str(cf.get('Дата продажу') or '').strip())
                    or iso_to_dmy(item.get('ordered_at') or item.get('created_at') or ''))
    else:
        date_val = iso_to_dmy(item.get('ordered_at') or item.get('created_at') or '')

    return {
        'id':            item.get('id'),
        'manager':       str(responsible.get('name') or '').strip(),
        'date':          date_val,
        'creo':          str(cf.get('Крео')     or '').strip() or 'Не вказано',
        'adset':         str(cf.get('Адсет')    or '').strip() or 'Не вказано',
        'campaign':      str(cf.get('Кампанія') or '').strip() or 'Не вказано',
        'ad_funnel':     str(cf.get('Воронка')  or '').strip() or 'Не вказано',
        'category':      CATMAP.get(cat_raw),
        'status':        status,
        'stage':         str(stage.get('name')          or '').strip(),
        'reason':        str(archive_status.get('name') or '').strip(),
        'form_purpose':  str(cf.get('Мета встановлення')   or '').strip(),
        'form_budget':   str(cf.get('Бюджет')               or '').strip(),
        'form_timeline': str(cf.get('Строки встановлення')  or '').strip(),
    }


def fetch_page(endpoint, page):
    sep = '&' if '?' in endpoint else '?'
    url = f"{CRM_BASE}{endpoint}{sep}page={page}"
    req = urllib.request.Request(url, headers={
        'X-Auth-Token': API_KEY,
        'Accept':       'application/json',
        'User-Agent':   'AdsDashboard/2.0',
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            if attempt == 2: raise
            time.sleep(2)


def sync_crm(mode, send_event):
    date_filter = ''
    # Render free tier wipes /tmp on every restart — always fetch full 2026 data
    # INC_DAYS incremental window intentionally not used for cloud

    active_rows, lost_rows, won_rows = [], [], []
    FB_SOURCE_ID = 6
    CUT_DATE = '2026-01-01'

    def process_item(item, status):
        funnel_id    = (item.get('funnel') or {}).get('id')
        funnel_title = (item.get('funnel') or {}).get('title', '')
        cf           = item.get('custom_fields') or {}
        ordered_at   = item.get('ordered_at') or item.get('created_at') or ''
        year         = int(ordered_at[:4]) if len(ordered_at) >= 4 else 0
        result       = item.get('result')

        is_b2c          = (funnel_id == 1 or 'B2C' in funnel_title or 'В2С' in funnel_title or 'Кінцевий' in funnel_title)
        is_realizatsiya = (funnel_id == 3 or 'Реаліз' in funnel_title)

        if not is_b2c and not is_realizatsiya: return []
        if year < 2026: return []

        source_id   = (item.get('source') or {}).get('id')
        source_name = str((item.get('source') or {}).get('name') or '').strip()
        custom_src  = str(cf.get('Джерело') or '').strip()
        is_fb = (source_id == FB_SOURCE_ID or source_name == 'FB Ads' or custom_src == 'FB Ads')
        if not is_fb: return []

        if is_b2c and status == 'active' and result is not None: return []
        if is_realizatsiya and status == 'lost': return []

        out = []
        if is_realizatsiya:
            lead_row = crm_item_to_row(item, 'active', is_realizatsiya=False)
            if lead_row.get('date'): out.append((lead_row, 'active'))
            sale_date = str(cf.get('Дата продажу') or '').strip()
            if sale_date:
                won_row = crm_item_to_row(item, 'won', is_realizatsiya=True)
                if won_row.get('date'): out.append((won_row, 'won'))
        else:
            row = crm_item_to_row(item, status)
            if row.get('date'): out.append((row, status))
        return out

    def fetch_segment(label, endpoint, phase_start, phase_len, status):
        page = 1
        total_pages = 1
        while page <= total_pages:
            try:
                data = fetch_page(endpoint, page)
            except urllib.error.HTTPError as e:
                send_event({'type': 'error', 'message': f"CRM API {e.code}: {e.reason}"}); return False
            except Exception as e:
                send_event({'type': 'error', 'message': f"Помилка: {str(e)}"}); return False

            items = data.get('items') or []
            pg    = data.get('pagination') or {}
            total_pages = int(pg.get('total_pages') or 1)

            for item in items:
                for row, kind in process_item(item, status):
                    if kind == 'won':    won_rows.append(row)
                    elif kind == 'active': active_rows.append(row)
                    elif kind == 'lost':   lost_rows.append(row)

            pct = phase_start + (page / total_pages) * phase_len
            send_event({
                'type': 'progress',
                'label': f"{label}: {page}/{total_pages}  ·  лідів:{len(active_rows)+len(lost_rows)}  програних:{len(lost_rows)}  оплат:{len(won_rows)}",
                'pct': round(min(pct, 99), 1),
            })
            page += 1
            if page <= total_pages: time.sleep(DELAY_MS / 1000)
        return True

    df = date_filter
    # Використовуємо created_at_gteq (не ordered_at_gteq):
    # ordered_at — дата замовлення, порожня у більшості лідів → фільтр відсікає їх.
    # created_at — дата створення ліда, завжди заповнена → повертає всі ліди з початку року.
    seg1 = f"/agreements?q%5Bcreated_at_gteq%5D={CUT_DATE}&q%5Bresult_blank%5D=1{df}"
    if not fetch_segment("Активні 2026+", seg1, 0, 50, 'active'): return

    seg3 = f"/agreements?q%5Bresult_eq%5D=failed&q%5Bcreated_at_gteq%5D={CUT_DATE}{df}"
    if not fetch_segment("Програні 2026+", seg3, 50, 49, 'lost'): return

    # Incremental merge (in-memory on cloud)
    if mode == 'inc' and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, encoding='utf-8') as f:
                existing = json.load(f)
            active_map = {r['id']: r for r in existing.get('active', []) if r.get('id')}
            lost_map   = {r['id']: r for r in existing.get('lost',   []) if r.get('id')}
            won_map    = {r['id']: r for r in existing.get('won',    []) if r.get('id')}
            active_noid = [r for r in existing.get('active', []) if not r.get('id')]
            lost_noid   = [r for r in existing.get('lost',   []) if not r.get('id')]
            won_noid    = [r for r in existing.get('won',    []) if not r.get('id')]
            for r in active_rows:
                if r.get('id'): active_map[r['id']] = r; lost_map.pop(r['id'], None)
                else: active_noid.append(r)
            for r in lost_rows:
                if r.get('id'): lost_map[r['id']] = r; active_map.pop(r['id'], None)
                else: lost_noid.append(r)
            for r in won_rows:
                if r.get('id'): won_map[r['id']] = r
                else: won_noid.append(r)
            active_rows[:] = list(active_map.values()) + active_noid
            lost_rows[:]   = list(lost_map.values())   + lost_noid
            won_rows[:]    = list(won_map.values())     + won_noid
        except Exception as e:
            print(f"Merge failed: {e}")

    payload = {
        'active':    active_rows,
        'lost':      lost_rows,
        'won':       won_rows,
        'total':     len(active_rows) + len(lost_rows) + len(won_rows),
        'synced_at': datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as e:
        print(f"Save failed: {e}")

    # Стрімимо дані через SSE-чанки → клієнт не робить окремий /api/data запит
    _CH = 300
    def _chunks(lst):
        for i in range(0, max(1, len(lst)), _CH):
            yield lst[i:i+_CH]
    for c in _chunks(active_rows): send_event({'type': 'data_active', 'rows': c})
    for c in _chunks(lost_rows):   send_event({'type': 'data_lost',   'rows': c})
    for c in _chunks(won_rows):    send_event({'type': 'data_won',    'rows': c})

    send_event({
        'type': 'done_signal',
        'total': payload['total'],
        'active_count': len(active_rows),
        'lost_count':   len(lost_rows),
        'won_count':    len(won_rows),
    })


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # Silent on cloud

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/sync'):
            self._serve_sync(); return
        if self.path == '/api/data':
            self._serve_data(); return
        if self.path == '/api/ping':
            self._json({'ok': True, 'server': 'cloud'}); return
        # Root — health check для Render
        if self.path in ('/', '/health'):
            self._json({'ok': True, 'service': 'ads-dashboard-crm-proxy'}); return
        self.send_response(404); self.end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin',  '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, ngrok-skip-browser-warning')

    def _json(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._cors()
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_data(self):
        if not os.path.exists(DATA_FILE):
            self._json({'ok': False, 'message': 'Немає даних. Виконай синхронізацію.'}); return
        with open(DATA_FILE, 'rb') as f:
            raw = f.read()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._cors()
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _serve_sync(self):
        mode = 'full' if 'mode=full' in self.path else 'inc'
        self.send_response(200)
        self.send_header('Content-Type',      'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control',     'no-cache')
        self.send_header('X-Accel-Buffering', 'no')
        self._cors()
        self.end_headers()

        def send_event(obj):
            try:
                line = 'data: ' + json.dumps(obj, ensure_ascii=False) + '\n\n'
                self.wfile.write(line.encode('utf-8'))
                self.wfile.flush()
            except BrokenPipeError:
                pass

        try:
            sync_crm(mode, send_event)
        except Exception as e:
            send_event({'type': 'error', 'message': str(e)})


def main():
    print(f'CRM Proxy Cloud running on port {PORT}')
    # Bind to 0.0.0.0 — обов'язково для Render
    server = http.server.HTTPServer(('0.0.0.0', PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('Stopped.')


if __name__ == '__main__':
    main()
