#!/usr/bin/env python3
"""
ADS DASHBOARD — CRM Sync Server
────────────────────────────────
Запуск:   python3 crm_proxy.py
Дашборд:  http://localhost:8765

Сервер:
  1. Роздає ads_dashboard.html за адресою http://localhost:8765
  2. При натисканні кнопки в дашборді — сам завантажує всі угоди з KeepinCRM
  3. Повертає дані через SSE (Server-Sent Events) з прогресом у реальному часі
  4. Не потребує жодних налаштувань у браузері

Зміни налаштувань — тільки у цьому файлі (перший блок нижче).
"""

import http.server
import urllib.request
import urllib.error
import json
import os
import time
import threading
import socket
import socketserver
from datetime import datetime, timedelta, timezone

# ══════════════════════════════════════════════
#  НАЛАШТУВАННЯ
# ══════════════════════════════════════════════
API_KEY  = "1PtL9c3Qc1S8iiRDgazx2Yv1"   # KeepinCRM API-ключ
PORT     = 8765                            # Порт локального сервера
CRM_BASE = "https://api.keepincrm.com/v1" # Базовий URL API
DELAY_MS = 200                             # Затримка між запитами (мс) — ліміт 100 req/min
INC_DAYS = 5                               # Глибина інкрементального синку (днів)
# ══════════════════════════════════════════════

class _ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True  # не блокує shutdown

def _get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return 'localhost'

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_FILE = os.path.join(SCRIPT_DIR, "ads_dashboard.html")
DATA_FILE      = os.path.join(SCRIPT_DIR, "crm_data.json")

# Cyrillic → Latin category mapping
CATMAP = {'А': 'A', 'В': 'B', 'С': 'C', 'A': 'A', 'B': 'B', 'C': 'C'}


def iso_to_dmy(iso: str) -> str:
    """Convert ISO datetime string to DD.MM.YYYY in Ukrainian local time.
    Ukraine: EET (UTC+2) Nov–Mar, EEST (UTC+3) Apr–Oct.
    """
    if not iso:
        return ''
    try:
        dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
        # Ukrainian timezone offset: +3 in summer (Apr-Oct), +2 in winter (Nov-Mar)
        offset_h = 3 if 4 <= dt.month <= 10 else 2
        dt_ua = dt.astimezone(timezone(timedelta(hours=offset_h)))
        return f"{dt_ua.day:02d}.{dt_ua.month:02d}.{dt_ua.year}"
    except Exception:
        return ''


def parse_crm_date(s: str) -> str:
    """Parse CRM date field to DD.MM.YYYY.
    Handles: 'DD.MM.YYYY', 'YYYY-MM-DD', ISO datetime."""
    if not s:
        return ''
    s = str(s).strip()
    # Already DD.MM.YYYY
    if len(s) >= 10 and s[2] == '.' and s[5] == '.':
        return s[:10]
    # ISO or YYYY-MM-DD
    return iso_to_dmy(s)


def crm_item_to_row(item: dict, status: str, is_realizatsiya: bool = False) -> dict:
    """Convert one CRM agreement to dashboard DATA row format"""
    cf = item.get('custom_fields') or {}
    cat_raw = str(cf.get('Категорія клієнта') or '').strip()
    archive_status = item.get('archive_status') or {}
    stage = item.get('stage') or {}
    responsible = item.get('main_responsible') or {}

    # Дата: для Реалізація — "Дата продажу", для решти — ordered_at
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
        'last_note':     str(item.get('last_note')       or '').strip(),
        'reason':        str(archive_status.get('name') or '').strip(),
        'form_purpose':  str(cf.get('Мета встановлення')   or '').strip(),
        'form_budget':   str(cf.get('Бюджет')               or '').strip(),
        'form_timeline': str(cf.get('Строки встановлення')  or '').strip(),
    }


def fetch_page(endpoint: str, page: int) -> dict:
    """Fetch one page from CRM API (with retry)"""
    sep = '&' if '?' in endpoint else '?'
    url = f"{CRM_BASE}{endpoint}{sep}page={page}"
    req = urllib.request.Request(url, headers={
        'X-Auth-Token': API_KEY,
        'Accept':       'application/json',
        'User-Agent':   'AdsDashboard/2.0',
    })
    for attempt in range(3):  # 3 спроби
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            if attempt == 2:
                raise
            print(f"  Retry {attempt+1}/3 for page {page}...")
            time.sleep(2)


def sync_crm(mode: str, send_event):
    """
    Fetch all CRM data and stream progress events.
    send_event(dict) — callback to send SSE event to client.
    mode: 'full' | 'inc'
    """
    # Build date filter for incremental mode
    date_filter = ''
    if mode == 'inc':
        since = datetime.now(tz=timezone.utc) - timedelta(days=INC_DAYS)
        date_filter = f"&q%5Bcreated_at_gteq%5D={since.strftime('%Y-%m-%d')}"

    active_rows = []
    lost_rows   = []
    won_rows    = []

    FB_SOURCE_ID = 6
    CUT_DATE     = '2026-01-01'

    def process_item(item, status):
        """
        Фільтрує угоду. Повертає список (row, kind) для вставки у відповідні масиви.

        Правила:
        - Тільки воронки Кінцевий B2C і Реалізація
        - Тільки 2026+ (ordered_at)
        - Тільки FB Ads: source.id==6 або source.name=='FB Ads' або cf['Джерело']=='FB Ads'
        - B2C active-сегмент: пропускаємо якщо result != null
          (програні прийдуть окремо в сегменті seg3 зі статусом 'lost')
        - Реалізація result=failed — пропускаємо
        - Реалізація: кожна угода рахується як ліда (ordered_at) + як продаж (Дата продажу),
          якщо "Дата продажу" заповнена
        """
        funnel_id    = (item.get('funnel') or {}).get('id')
        funnel_title = (item.get('funnel') or {}).get('title', '')
        cf           = item.get('custom_fields') or {}
        ordered_at   = item.get('ordered_at') or item.get('created_at') or ''
        year         = int(ordered_at[:4]) if len(ordered_at) >= 4 else 0
        result       = item.get('result')  # None=активна, 'failed'=програна

        is_b2c          = (funnel_id == 1 or 'B2C' in funnel_title
                           or 'В2С' in funnel_title or 'Кінцевий' in funnel_title)
        is_realizatsiya = (funnel_id == 3 or 'Реаліз' in funnel_title)

        # Тільки потрібні воронки
        if not is_b2c and not is_realizatsiya:
            return []

        # Тільки 2026+
        if year < 2026:
            return []

        # Фільтр по FB Ads: системне поле source або кастомне поле "Джерело"
        source_id   = (item.get('source') or {}).get('id')
        source_name = str((item.get('source') or {}).get('name') or '').strip()
        custom_src  = str(cf.get('Джерело') or '').strip()
        is_fb = (source_id == FB_SOURCE_ID
                 or source_name == 'FB Ads'
                 or custom_src  == 'FB Ads')
        if not is_fb:
            return []

        # B2C active-сегмент: не беремо якщо result != null
        # (захист від дублікатів — програні прийдуть окремо через seg3)
        if is_b2c and status == 'active' and result is not None:
            return []

        # Реалізація закриті (прийшли через seg3) — не беремо
        if is_realizatsiya and status == 'lost':
            return []

        out = []

        if is_realizatsiya:
            # 1. Рахуємо як ліда (дата = ordered_at, щоб не зменшувався підрахунок лідів)
            lead_row = crm_item_to_row(item, 'active', is_realizatsiya=False)
            if lead_row.get('date'):
                out.append((lead_row, 'active'))

            # 2. Рахуємо як продаж тільки якщо "Дата продажу" заповнена
            sale_date = str(cf.get('Дата продажу') or '').strip()
            if sale_date:
                won_row = crm_item_to_row(item, 'won', is_realizatsiya=True)
                if won_row.get('date'):
                    out.append((won_row, 'won'))
        else:
            row = crm_item_to_row(item, status)
            if row.get('date'):
                out.append((row, status))

        return out

    def fetch_segment(label, endpoint, phase_start, phase_len, status):
        """Завантажує один сегмент угод з прогресом."""
        page = 1
        total_pages = 1
        while page <= total_pages:
            try:
                data = fetch_page(endpoint, page)
            except urllib.error.HTTPError as e:
                send_event({'type': 'error', 'message': f"CRM API {e.code}: {e.reason}"})
                return False
            except Exception as e:
                send_event({'type': 'error', 'message': f"Помилка: {str(e)}"})
                return False

            items = data.get('items') or []
            pg    = data.get('pagination') or {}
            total_pages = int(pg.get('total_pages') or 1)

            for item in items:
                for row, kind in process_item(item, status):
                    if kind == 'won':
                        won_rows.append(row)
                    elif kind == 'active':
                        active_rows.append(row)
                    elif kind == 'lost':
                        lost_rows.append(row)

            pct = phase_start + (page / total_pages) * phase_len
            send_event({
                'type':  'progress',
                'label': f"{label}: {page}/{total_pages}  ·  лідів:{len(active_rows)+len(lost_rows)}  програних:{len(lost_rows)}  оплат:{len(won_rows)}",
                'pct':   round(min(pct, 99), 1),
            })
            page += 1
            if page <= total_pages:
                time.sleep(DELAY_MS / 1000)
        return True

    # ── Сегменти ─────────────────────────────────────────────────────
    #
    #  Тільки 2026+ дані (CUT_DATE = 2026-01-01)
    #  seg1: активні (result=null) — result_blank=1 в URL + перевірка в process_item
    #  seg3: програні (result=failed)
    #
    #  Дублікатів немає: seg1 містить тільки result=null, seg3 — тільки result=failed
    #  Реалізація-угоди з seg1 йдуть в active_rows (як ліди) + won_rows (як продажі)

    df = date_filter  # порожньо (full) або gteq для inc

    # Сегмент 1: активні 2026+ (result=null)
    seg1 = f"/agreements?q%5Bcreated_at_gteq%5D={CUT_DATE}&q%5Bresult_blank%5D=1{df}"
    if not fetch_segment("Активні 2026+", seg1, 0, 50, 'active'): return

    # Сегмент 2: програні 2026+
    seg3 = f"/agreements?q%5Bresult_eq%5D=failed&q%5Bcreated_at_gteq%5D={CUT_DATE}{df}"
    if not fetch_segment("Програні 2026+", seg3, 50, 49, 'lost'): return

    # ── Inc mode: merge freshly-fetched rows into existing data by ID ────────────
    # For inc sync: load existing, upsert new rows by CRM id, handle status changes
    if mode == 'inc' and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, encoding='utf-8') as f:
                existing = json.load(f)

            # Build mutable ID maps from existing (rows without id kept as-is)
            active_map = {r['id']: r for r in existing.get('active', []) if r.get('id')}
            lost_map   = {r['id']: r for r in existing.get('lost',   []) if r.get('id')}
            won_map    = {r['id']: r for r in existing.get('won',    []) if r.get('id')}
            # Keep legacy rows that have no id
            active_noid = [r for r in existing.get('active', []) if not r.get('id')]
            lost_noid   = [r for r in existing.get('lost',   []) if not r.get('id')]
            won_noid    = [r for r in existing.get('won',    []) if not r.get('id')]

            # Upsert freshly-fetched rows into the maps
            for r in active_rows:
                if r.get('id'):
                    active_map[r['id']] = r
                    lost_map.pop(r['id'], None)   # deal moved back to active
                else:
                    active_noid.append(r)
            for r in lost_rows:
                if r.get('id'):
                    lost_map[r['id']] = r
                    active_map.pop(r['id'], None)  # deal moved to lost
                else:
                    lost_noid.append(r)
            for r in won_rows:
                if r.get('id'):
                    won_map[r['id']] = r
                else:
                    won_noid.append(r)

            active_rows = list(active_map.values()) + active_noid
            lost_rows   = list(lost_map.values())   + lost_noid
            won_rows    = list(won_map.values())     + won_noid
            print(f"  🔀 Merged: active={len(active_rows)}, lost={len(lost_rows)}, won={len(won_rows)}")
        except Exception as e:
            print(f"  ⚠️  ID-merge failed, using fetched data only: {e}")

    # ── Save to disk ──────────────────────────────────────
    payload = {
        'active':     active_rows,
        'lost':       lost_rows,
        'won':        won_rows,
        'total':      len(active_rows) + len(lost_rows) + len(won_rows),
        'synced_at':  datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        print(f"  💾 Збережено в {DATA_FILE} ({payload['total']} угод)")
    except Exception as e:
        print(f"  ⚠️  Не вдалось зберегти crm_data.json: {e}")

    # ── Стрімимо дані через SSE-чанки → клієнт не робить окремий /api/data запит ──
    _CH = 300
    def _chunks(lst):
        for i in range(0, max(1, len(lst)), _CH):
            yield lst[i:i+_CH]
    for c in _chunks(active_rows): send_event({'type': 'data_active', 'rows': c})
    for c in _chunks(lost_rows):   send_event({'type': 'data_lost',   'rows': c})
    for c in _chunks(won_rows):    send_event({'type': 'data_won',    'rows': c})

    send_event({
        'type':         'done_signal',
        'total':        payload['total'],
        'active_count': len(active_rows),
        'lost_count':   len(lost_rows),
        'won_count':    len(won_rows),
    })


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        status = args[1] if len(args) > 1 else '-'
        print(f"  {self.command:4s} {self.path}  [{status}]")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        # ── Dashboard HTML ────────────────────────────────
        if self.path in ('/', '/index.html', '/ads_dashboard.html'):
            self._serve_file()
            return

        # ── SSE sync endpoint ─────────────────────────────
        if self.path.startswith('/api/sync'):
            self._serve_sync()
            return

        # ── Health check ──────────────────────────────────
        if self.path == '/api/ping':
            self._json({'ok': True})
            return

        # ── Stats: what's in crm_data.json ───────────────
        if self.path == '/api/stats':
            self._serve_stats()
            return

        # ── Debug: inspect raw CRM fields ────────────────
        if self.path == '/api/debug':
            self._serve_debug()
            return

        # ── Cached CRM data ───────────────────────────────
        if self.path == '/api/data':
            self._serve_data()
            return

        # ── Open dashboard command file ───────────────────
        if self.path == '/api/run-command':
            self._run_command()
            return

        self.send_response(404)
        self.end_headers()

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

    def _serve_file(self):
        if not os.path.exists(DASHBOARD_FILE):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'ads_dashboard.html not found')
            return
        with open(DASHBOARD_FILE, 'rb') as f:
            data = f.read()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_stats(self):
        """Show breakdown of crm_data.json by year and status"""
        if not os.path.exists(DATA_FILE):
            self._json({'ok': False, 'message': 'crm_data.json не знайдено — потрібна синхронізація'})
            return
        with open(DATA_FILE, encoding='utf-8') as f:
            d = json.load(f)
        def by_year(rows):
            ymap = {}
            for r in rows:
                yr = (r.get('date') or '')[-4:] or '?'
                ymap[yr] = ymap.get(yr, 0) + 1
            return dict(sorted(ymap.items()))
        self._json({
            'ok': True,
            'synced_at': d.get('synced_at'),
            'total': d.get('total'),
            'active': {'count': len(d.get('active',[])), 'by_year': by_year(d.get('active',[]))},
            'lost':   {'count': len(d.get('lost',[])),   'by_year': by_year(d.get('lost',[]))},
            'won':    {'count': len(d.get('won',[])),    'by_year': by_year(d.get('won',[]))},
            'sample_active': d.get('active',[])[:3],
        })

    def _serve_debug(self):
        """Fetch samples: first page + first page of result=success"""
        def extract(item):
            funnel = item.get('funnel') or {}
            source = item.get('source') or {}
            return {
                'id':           item.get('id'),
                'ordered_at':   item.get('ordered_at'),
                'funnel_id':    funnel.get('id'),
                'funnel_title': funnel.get('title'),
                'source_id':    source.get('id'),
                'source_name':  source.get('name'),
                'result':       item.get('result'),
            }
        try:
            # Sample of success/won deals
            success_data = fetch_page('/agreements?q%5Bresult_eq%5D=success', 1)
            success_items = [extract(i) for i in (success_data.get('items') or [])[:5]]

            # Sample of all results to see distinct values
            all_data = fetch_page('/agreements', 1)
            all_items = [extract(i) for i in (all_data.get('items') or [])[:5]]

            self._json({
                'ok': True,
                'all_pagination': all_data.get('pagination'),
                'all_sample': all_items,
                'success_pagination': success_data.get('pagination'),
                'success_sample': success_items,
            })
        except Exception as e:
            self._json({'ok': False, 'message': str(e)})

    def _serve_data(self):
        """Serve cached CRM data from crm_data.json"""
        if not os.path.exists(DATA_FILE):
            self._json({'ok': False, 'message': 'Немає збережених даних. Виконай синхронізацію.'})
            return
        with open(DATA_FILE, 'rb') as f:
            raw = f.read()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._cors()
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _run_command(self):
        """Open 'Відкрити дашборд.command' via macOS open command"""
        import subprocess
        cmd_file = os.path.join(SCRIPT_DIR, 'Відкрити дашборд.command')
        try:
            if not os.path.exists(cmd_file):
                self._json({'ok': False, 'message': f'Файл не знайдено: {cmd_file}'})
                return
            subprocess.Popen(['open', cmd_file])
            self._json({'ok': True, 'message': 'Запущено ✓'})
        except Exception as e:
            self._json({'ok': False, 'message': str(e)})

    def _serve_sync(self):
        # Parse mode from query string
        mode = 'inc'
        if 'mode=full' in self.path:
            mode = 'full'

        self.send_response(200)
        self.send_header('Content-Type',  'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('X-Accel-Buffering', 'no')
        self._cors()
        self.end_headers()

        _done = threading.Event()

        def send_event(obj):
            """Write one SSE event and flush"""
            try:
                line = 'data: ' + json.dumps(obj, ensure_ascii=False) + '\n\n'
                self.wfile.write(line.encode('utf-8'))
                self.wfile.flush()
            except BrokenPipeError:
                pass

        def _keepalive():
            # Кожні 15 сек шлемо SSE-коментар — роутер/NAT не обриває з'єднання
            while not _done.wait(15):
                try:
                    self.wfile.write(b': keepalive\n\n')
                    self.wfile.flush()
                except Exception:
                    break

        threading.Thread(target=_keepalive, daemon=True).start()

        try:
            sync_crm(mode, send_event)
        except Exception as e:
            send_event({'type': 'error', 'message': str(e)})
        finally:
            _done.set()


def main():
    print('=' * 52)
    print('  ADS DASHBOARD — CRM Sync Server')
    print('=' * 52)
    local_ip = _get_local_ip()
    print(f'  Комп\'ютер: http://localhost:{PORT}')
    print(f'  Телефон:   http://{local_ip}:{PORT}')
    print(f'  API ключ:  {API_KEY[:8]}…{API_KEY[-4:]}')
    print(f'  HTML файл: {DASHBOARD_FILE}')
    if not os.path.exists(DASHBOARD_FILE):
        print()
        print('  ⚠️  ads_dashboard.html не знайдено!')
        print('  Переконайся, що crm_proxy.py лежить поруч з дашбордом.')
    print()
    print('  Для зупинки: Ctrl+C')
    print('=' * 52)

    while True:
        try:
            # Слухаємо на всіх інтерфейсах ('') — щоб телефон міг підключитись по локальному IP
            server = _ThreadedServer(('', PORT), Handler)
            server.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            server.serve_forever()
        except KeyboardInterrupt:
            print('\n  Сервер зупинено.')
            break
        except Exception as e:
            print(f'  ⚠️  Краш: {e} — перезапуск через 3 сек...')
            time.sleep(3)


if __name__ == '__main__':
    main()
