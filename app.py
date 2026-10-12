"""
LAGLOG Cloud — Lagonis Logistics Web App
Multi-user, session-based login, Render.com ready
"""
from werkzeug.security import generate_password_hash, check_password_hash
import hmac, time
from flask import (Flask, request, jsonify, send_file, render_template,
                   send_from_directory, session, redirect, url_for, abort)
import json, os, sys, uuid, io, zipfile, smtplib, copy, hashlib, secrets, functools
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, HRFlowable
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ── App Setup ────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH = os.path.join(BASE_DIR, 'static', 'img', 'logo.jpg')

# Render: persistent disk at /var/data — fallback to ./data locally
DATA_DIR = os.environ.get('DATA_DIR', os.path.join(BASE_DIR, 'data'))
os.makedirs(DATA_DIR, exist_ok=True)
print(f'[LAGLOG] DATA_DIR = {DATA_DIR}', flush=True)

DATA_FILE   = os.path.join(DATA_DIR, 'db.json')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.json')
USERS_FILE  = os.path.join(DATA_DIR, 'users.json')

app = Flask(__name__)
def _load_secret_key():
    k = os.environ.get('SECRET_KEY')
    if k: return k
    f = os.path.join(DATA_DIR, 'secret.key')   # dauerhaft speichern -> kein Logout nach Neustart
    try:
        if os.path.exists(f):
            k = open(f).read().strip()
            if k: return k
        k = secrets.token_hex(32)
        with open(f, 'w') as fh: fh.write(k)
        try: os.chmod(f, 0o600)
        except Exception: pass
        return k
    except Exception:
        return secrets.token_hex(32)
app.secret_key = _load_secret_key()
app.permanent_session_lifetime = timedelta(days=7)
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax')

# ── Auth helpers ─────────────────────────────────────────
def _legacy_hash(pw): return hashlib.sha256(pw.encode()).hexdigest()
def hash_pw(pw): return generate_password_hash(pw)          # gesalzen (pbkdf2/scrypt)
def verify_pw(stored, pw):
    stored = stored or ''
    if stored.startswith(('pbkdf2:', 'scrypt:')):
        try: return check_password_hash(stored, pw)
        except Exception: return False
    return hmac.compare_digest(stored, _legacy_hash(pw))     # altes Format (wird beim Login umgestellt)
def is_legacy_hash(stored): return not (stored or '').startswith(('pbkdf2:', 'scrypt:'))

# Login-Sperre: 5 Fehlversuche -> 15 Minuten
_FAILS = {}
def _lock_key(username): return (request.remote_addr or '') + '|' + str(username or '').lower()
def _locked(k):
    f = _FAILS.get(k)
    return bool(f and f[0] >= 5 and time.time() < f[1])
def _fail(k):
    n = (_FAILS.get(k) or (0, 0))[0] + 1
    _FAILS[k] = (n, time.time() + 900)


def load_users():
    if not os.path.exists(USERS_FILE):
        # Create default admin on first run
        default = [{
            'id': str(uuid.uuid4()),
            'username': 'admin',
            'password': hash_pw('laglog2024'),
            'name': 'Administrator',
            'role': 'admin',
            'createdAt': datetime.now().isoformat()
        }]
        with open(USERS_FILE, 'w') as f: json.dump(default, f, indent=2)
        return default
    with open(USERS_FILE, encoding='utf-8') as f: return json.load(f)

def save_users(users):
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=2, ensure_ascii=False)

def current_user():
    uid = session.get('user_id')
    if not uid: return None
    users = load_users()
    return next((u for u in users if u['id'] == uid), None)

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            if request.is_json:
                return jsonify({'error': 'Nicht angemeldet', 'redirect': '/login'}), 401
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        u = current_user()
        if not u or u.get('role') != 'admin':
            return jsonify({'error': 'Keine Berechtigung'}), 403
        return f(*args, **kwargs)
    return decorated

# ── DB helpers ────────────────────────────────────────────
# Railway: data is stored on persistent volume at /app/data
# DATA_DIR is set via RAILWAY_VOLUME_MOUNT_PATH or defaults to ./data

def load_db():
    empty = {'orders':[], 'customers':[], 'inquiries':[], 'order_counter':1}
    if not os.path.exists(DATA_FILE):
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(empty, f)
        return empty
    try:
        with open(DATA_FILE, encoding='utf-8') as f:
            db = json.load(f)
        if 'invoices' in db or 'invoice_counter' in db:   # Altdaten Rechnungen/Mahnwesen/Statistik entfernen
            db.pop('invoices',None); db.pop('invoice_counter',None); save_db(db)
        for k,v in [('customers',[]),('inquiries',[]),('order_counter',len(db.get('orders',[]))+1)]:
            if k not in db: db[k] = v
        return db
    except Exception as e:
        print(f'load_db error: {e}')
        return empty

def save_db(db):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    tmp = DATA_FILE + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DATA_FILE)  # atomic write
    except Exception as e:
        print(f'save_db error: {e}')

def load_config():
    defaults = {
        'smtp_host':'','smtp_port':587,'smtp_user':'','smtp_pass':'',
        'smtp_ssl':False,'from_name':'Lagonis Logistics','from_email':'',
        'company_name':'Lagonis Logistics','company_address':'','company_zip':'',
        'company_city':'','company_phone':'','company_email':'','company_web':'',
        'bank_iban':'','bank_bic':'','bank_name':'',
        'default_author':'',
        'order_prefix':'LL-TA','inquiry_prefix':'ANF','customer_prefix':'KD',
        'order_counter_override':0,
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding='utf-8') as f:
                saved = json.load(f)
            defaults.update(saved)
        except: pass
    return defaults

def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    tmp = CONFIG_FILE + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        os.replace(tmp, CONFIG_FILE)
    except Exception as e:
        print(f'save_config error: {e}')

def sanitize(s): return ''.join(c if c.isalnum() or c in '-_.' else '_' for c in str(s))
def fmt_dt(val, lang='de'):
    if not val: return '-'
    try:
        dt = datetime.fromisoformat(val.replace('Z',''))
        return dt.strftime('%d.%m.%Y %H:%M') if lang=='de' else dt.strftime('%m/%d/%Y %H:%M')
    except: return val
def fmt_date(val, lang='de'):
    if not val: return '-'
    try:
        dt = datetime.strptime(val, '%Y-%m-%d')
        return dt.strftime('%d.%m.%Y') if lang=='de' else dt.strftime('%m/%d/%Y')
    except: return val

# ── Pages ─────────────────────────────────────────────────
@app.route('/login')
def login_page():
    if current_user(): return redirect('/')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear(); return redirect('/login')

@app.route('/')
@login_required
def index(): return render_template('index.html')

@app.route('/static/<path:path>')
def static_files(path): return send_from_directory('static', path)

# ── Auth API ──────────────────────────────────────────────
@app.route('/api/auth/login', methods=['POST'])
def api_login():
    data = request.json or {}
    lk = _lock_key(data.get('username'))
    if _locked(lk):
        return jsonify({'ok': False, 'message': 'Zu viele Fehlversuche. Bitte in 15 Minuten erneut versuchen.'}), 429
    users = load_users()
    user = next((u for u in users if u['username'] == data.get('username')
                 and verify_pw(u['password'], data.get('password',''))), None)
    if not user:
        _fail(lk)
        return jsonify({'ok': False, 'message': 'Benutzername oder Passwort falsch'}), 401
    _FAILS.pop(lk, None)
    if is_legacy_hash(user['password']):                     # automatisch auf gesalzenen Hash umstellen
        user['password'] = hash_pw(data.get('password',''))
        save_users(users)
    session.permanent = True
    session['user_id'] = user['id']
    return jsonify({'ok': True, 'name': user['name'], 'role': user['role']})

@app.route('/api/auth/me')
def api_me():
    u = current_user()
    if not u: return jsonify({'loggedIn': False})
    return jsonify({'loggedIn': True, 'username': u['username'], 'name': u['name'], 'role': u['role']})

@app.route('/api/auth/change-password', methods=['POST'])
@login_required
def change_password():
    data = request.json; u = current_user()
    users = load_users()
    for user in users:
        if user['id'] == u['id']:
            if not verify_pw(user['password'], data.get('old','')):
                return jsonify({'ok': False, 'message': 'Altes Passwort falsch'}), 400
            user['password'] = hash_pw(data.get('new',''))
            save_users(users)
            return jsonify({'ok': True})
    return jsonify({'ok': False}), 404

# ── User Management (admin only) ──────────────────────────
@app.route('/api/users', methods=['GET'])
@login_required
@admin_required
def get_users():
    users = load_users()
    return jsonify([{k:v for k,v in u.items() if k != 'password'} for u in users])

@app.route('/api/users', methods=['POST'])
@login_required
@admin_required
def create_user():
    data = request.json; users = load_users()
    if any(u['username'] == data['username'] for u in users):
        return jsonify({'ok': False, 'message': 'Benutzername bereits vergeben'}), 400
    user = {
        'id': str(uuid.uuid4()), 'username': data['username'],
        'password': hash_pw(data.get('password','laglog2024')),
        'name': data.get('name', data['username']),
        'role': data.get('role','user'),
        'createdAt': datetime.now().isoformat()
    }
    users.append(user); save_users(users)
    return jsonify({k:v for k,v in user.items() if k!='password'}), 201

@app.route('/api/users/<uid>', methods=['PUT'])
@login_required
@admin_required
def update_user(uid):
    data = request.json; users = load_users()
    for i, u in enumerate(users):
        if u['id'] == uid:
            u['name'] = data.get('name', u['name'])
            u['role'] = data.get('role', u['role'])
            if data.get('password'): u['password'] = hash_pw(data['password'])
            save_users(users); return jsonify({k:v for k,v in u.items() if k!='password'})
    return jsonify({'error':'not found'}), 404

@app.route('/api/users/<uid>', methods=['DELETE'])
@login_required
@admin_required
def delete_user(uid):
    u = current_user()
    if u['id'] == uid: return jsonify({'error': 'Kann sich nicht selbst löschen'}), 400
    users = load_users()
    users = [x for x in users if x['id'] != uid]
    save_users(users); return jsonify({'ok': True})

@app.route('/api/users/<uid>/reset-password', methods=['POST'])
@login_required
@admin_required
def reset_password(uid):
    data = request.json; users = load_users()
    for u in users:
        if u['id'] == uid:
            u['password'] = hash_pw(data.get('password','laglog2024'))
            save_users(users); return jsonify({'ok': True})
    return jsonify({'error':'not found'}), 404

# ── Config ────────────────────────────────────────────────
@app.route('/api/config', methods=['GET'])
@login_required
def get_config():
    cfg = load_config(); safe = dict(cfg)
    safe['smtp_pass'] = '***' if cfg.get('smtp_pass') else ''
    return jsonify(safe)

@app.route('/api/config', methods=['POST'])
@login_required
@admin_required
def set_config():
    data = request.json; cfg = load_config()
    for k,v in data.items():
        if k == 'smtp_pass' and v == '***': continue
        cfg[k] = v
    save_config(cfg)
    # Apply counter overrides to DB if set
    oc = int(data.get('order_counter_override') or 0)
    ic = int(data.get('invoice_counter_override') or 0)
    if oc > 0 or ic > 0:
        db = load_db()
        if oc > 0: db['order_counter'] = oc
        if ic > 0: db['invoice_counter'] = ic
        save_db(db)
    return jsonify({'ok': True})

@app.route('/api/config/test-smtp', methods=['POST'])
@login_required
def test_smtp():
    import threading
    cfg = load_config(); body = request.json or {}
    for k in ('smtp_host', 'smtp_port', 'smtp_user', 'smtp_ssl', 'from_email'):     # aktuelle Formularwerte testen, auch ungespeicherte
        if k in body and body[k] not in (None, ''): cfg[k] = body[k]
    if body.get('smtp_pass') not in (None, '', '***'): cfg['smtp_pass'] = body['smtp_pass']
    result = {'ok': False, 'message': 'Timeout — Server nicht erreichbar'}
    def try_connect():
        try:
            srv = _smtp_connect(cfg)
            try: srv.quit()
            except Exception: pass
            result['ok'] = True
            result['message'] = 'Verbindung erfolgreich!'
        except Exception as e:
            result['ok'] = False
            result['message'] = str(e)
    t = threading.Thread(target=try_connect)
    t.daemon = True
    t.start()
    t.join(timeout=40)  # Haupt- + Zweitversuch
    status = 200 if result['ok'] else 400
    return jsonify(result), status

# ── Customers ─────────────────────────────────────────────
def _cust_prefix():
    p = load_config().get('customer_prefix')
    return 'KD' if p is None else str(p).strip()

def _fmt_cust_nr(n):
    p = _cust_prefix()
    return f"{p}-{str(n).zfill(4)}" if p else str(n).zfill(4)

def _next_cust_nr(db, take=True):
    import re as _re
    c = db.get('customer_counter')
    if not c:
        mx = 0
        for x in db.get('customers', []):
            m = _re.search(r'(\d+)\s*$', str(x.get('kdnr') or ''))
            if m: mx = max(mx, int(m.group(1)))
        c = mx + 1
    if take: db['customer_counter'] = c + 1
    return _fmt_cust_nr(c)

def _ensure_cust_nrs(db):
    """Bestandskunden ohne Kundennummer bekommen fortlaufend eine (nach Anlagedatum)."""
    miss = [c for c in db['customers'] if not c.get('kdnr')]
    if not miss: return False
    for c in sorted(miss, key=lambda x: x.get('createdAt') or ''):
        c['kdnr'] = _next_cust_nr(db)
    return True

@app.route('/api/customers', methods=['GET'])
@login_required
def get_customers():
    db = load_db()
    if _ensure_cust_nrs(db): save_db(db)
    return jsonify(db['customers'])

@app.route('/api/customers/next-nr', methods=['GET'])
@login_required
def next_customer_nr():
    db = load_db(); _ensure_cust_nrs(db)
    return jsonify({'nr': _next_cust_nr(db, take=False)})

@app.route('/api/customers/set-counter', methods=['POST'])
@login_required
@admin_required
def set_customer_counter():
    db = load_db()
    try: val = max(1, int((request.json or {}).get('counter', 1)))
    except Exception: return jsonify({'ok': False, 'message': 'Ungültige Zahl'}), 400
    _ensure_cust_nrs(db); db['customer_counter'] = val; save_db(db)
    return jsonify({'ok': True, 'counter': val})

@app.route('/api/customers', methods=['POST'])
@login_required
def create_customer():
    db = load_db(); c = request.json
    _ensure_cust_nrs(db)
    c['id'] = str(uuid.uuid4()); c['createdAt'] = datetime.now().isoformat()
    c['createdBy'] = current_user()['username']
    c['kdnr'] = _next_cust_nr(db)
    db['customers'].append(c); save_db(db); return jsonify(c), 201

@app.route('/api/customers/<cid>', methods=['PUT'])
@login_required
def update_customer(cid):
    db = load_db()
    for i,c in enumerate(db['customers']):
        if c['id'] == cid:
            upd = request.json; upd['id'] = cid; upd['createdAt'] = c.get('createdAt','')
            upd['createdBy'] = c.get('createdBy','')
            upd['kdnr'] = c.get('kdnr') or _next_cust_nr(db)
            upd['updatedBy'] = current_user()['username']
            db['customers'][i] = upd; save_db(db); return jsonify(upd)
    return jsonify({'error':'not found'}), 404

@app.route('/api/customers/<cid>', methods=['DELETE'])
@login_required
def delete_customer(cid):
    db = load_db(); db['customers'] = [c for c in db['customers'] if c['id']!=cid]
    save_db(db); return jsonify({'ok':True})

# ── Orders ────────────────────────────────────────────────
@app.route('/api/orders', methods=['GET'])
@login_required
def get_orders(): return jsonify(load_db()['orders'])

@app.route('/api/orders/next-nr', methods=['GET'])
@login_required
def next_order_nr():
    db = load_db(); cfg = load_config()
    counter = db.get('order_counter', len(db['orders'])+1)
    year = datetime.now().year
    prefix = cfg.get('order_prefix','LL-TA') or 'LL-TA'
    return jsonify({'nr': f'{prefix}-{year}-{str(counter).zfill(4)}'})

@app.route('/api/orders/set-counter', methods=['POST'])
@login_required
@admin_required
def set_order_counter():
    db = load_db()
    val = int(request.json.get('counter', 1))
    db['order_counter'] = val
    save_db(db)
    return jsonify({'ok': True, 'counter': val})

@app.route('/api/orders', methods=['POST'])
@login_required
def create_order():
    db = load_db(); o = request.json
    o['id'] = str(uuid.uuid4()); o['createdAt'] = datetime.now().isoformat()
    o['status'] = 'open'; o['createdBy'] = current_user()['username']
    db['order_counter'] = db.get('order_counter', 1) + 1
    db['orders'].append(o); save_db(db); return jsonify(o), 201

@app.route('/api/orders/<oid>', methods=['PUT'])
@login_required
def update_order(oid):
    db = load_db()
    for i,o in enumerate(db['orders']):
        if o['id'] == oid:
            upd = request.json; upd['id'] = oid; upd['createdAt'] = o.get('createdAt','')
            upd['createdBy'] = o.get('createdBy',''); upd['updatedBy'] = current_user()['username']
            db['orders'][i] = upd; save_db(db); return jsonify(upd)
    return jsonify({'error':'not found'}), 404

@app.route('/api/orders/<oid>', methods=['DELETE'])
@login_required
def delete_order(oid):
    db = load_db(); db['orders'] = [o for o in db['orders'] if o['id']!=oid]
    save_db(db); return jsonify({'ok':True})

@app.route('/api/orders/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_orders():
    ids = set(request.json.get('ids',[])); db = load_db()
    db['orders'] = [o for o in db['orders'] if o['id'] not in ids]
    save_db(db); return jsonify({'ok':True})

@app.route('/api/orders/bulk-duplicate', methods=['POST'])
@login_required
def bulk_duplicate_orders():
    ids = request.json.get('ids',[]); db = load_db(); u = current_user()
    om = {o['id']:o for o in db['orders']}; new_orders = []
    for oid in ids:
        o = om.get(oid)
        if o:
            n = copy.deepcopy(o); n['id'] = str(uuid.uuid4())
            n['createdAt'] = datetime.now().isoformat(); n['createdBy'] = u['username']
            counter = db.get('order_counter', 1)
            n['nr'] = f"LL-TA-{datetime.now().year}-{str(counter).zfill(4)}"
            db['order_counter'] = counter + 1; new_orders.append(n)
    db['orders'].extend(new_orders); save_db(db)
    return jsonify({'ok':True,'count':len(new_orders)})

@app.route('/api/orders/<oid>/pdf')
@login_required
def order_pdf(oid):
    db = load_db(); cfg = load_config(); lang = request.args.get('lang','de')
    o = next((x for x in db['orders'] if x['id']==oid), None)
    if not o: return jsonify({'error':'not found'}), 404
    pdf = generate_order_pdf(o, cfg, lang)
    fname = f"Transportauftrag_{sanitize(o.get('nr',''))}.pdf"
    return send_file(io.BytesIO(pdf), mimetype='application/pdf', as_attachment=True, download_name=fname)

@app.route('/api/orders/<oid>/delivery-pdf')
@login_required
def delivery_pdf(oid):
    db = load_db(); cfg = load_config(); lang = request.args.get('lang','de')
    o = next((x for x in db['orders'] if x['id']==oid), None)
    if not o: return jsonify({'error':'not found'}), 404
    return send_file(io.BytesIO(generate_delivery_pdf(o,cfg,lang)), mimetype='application/pdf',
                     as_attachment=True, download_name=f"Lieferschein_{sanitize(o.get('nr',''))}.pdf")

@app.route('/api/orders/bulk-pdf', methods=['POST'])
@login_required
def orders_bulk_pdf():
    ids = request.json.get('ids',[]); lang = request.json.get('lang','de')
    db = load_db(); cfg = load_config(); om = {o['id']:o for o in db['orders']}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as zf:
        for oid in ids:
            o = om.get(oid)
            if o: zf.writestr(f"Transportauftrag_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang))
    buf.seek(0)
    return send_file(buf, mimetype='application/zip', as_attachment=True, download_name='Transportauftraege.zip')

@app.route('/api/orders/<oid>/send-email', methods=['POST'])
@login_required
def send_order_email(oid):
    data = request.json; db = load_db(); cfg = load_config(); lang = data.get('lang','de')
    o = next((x for x in db['orders'] if x['id']==oid), None)
    if not o: return jsonify({'error':'not found'}), 404
    try:
        atts = [(f"Transportauftrag_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang))]
        if data.get('includeDelivery'):
            atts.append((f"Lieferschein_{sanitize(o.get('nr',''))}.pdf", generate_delivery_pdf(o,cfg,lang)))
        _send_email(cfg, data['to'], data['subject'], data['body'], atts)
        return jsonify({'ok':True})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 500

@app.route('/api/orders/bulk-email', methods=['POST'])
@login_required
def orders_bulk_email():
    data = request.json; db = load_db(); cfg = load_config()
    lang = data.get('lang','de'); om = {o['id']:o for o in db['orders']}; atts = []
    for oid in data.get('ids',[]):
        o = om.get(oid)
        if o: atts.append((f"Transportauftrag_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang)))
    try: _send_email(cfg, data['to'], data['subject'], data['body'], atts); return jsonify({'ok':True})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 500

# ── Invoices ──────────────────────────────────────────────
@app.route('/api/backup', methods=['GET'])
@login_required
@admin_required
def backup():
    """Download full database backup as JSON"""
    db = load_db()
    backup_data = json.dumps(db, indent=2, ensure_ascii=False)
    buf = io.BytesIO(backup_data.encode('utf-8'))
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    return send_file(buf, mimetype='application/json',
                     as_attachment=True, download_name=f'hanseaerp_backup_{ts}.json')

@app.route('/api/restore', methods=['POST'])
@login_required
@admin_required
def restore():
    """Restore database from JSON backup"""
    try:
        data = request.json
        if 'orders' not in data:
            return jsonify({'ok': False, 'message': 'Ungültiges Backup-Format'}), 400
        data.pop('invoices',None); data.pop('invoice_counter',None)
        save_db(data)
        return jsonify({'ok': True, 'orders': len(data.get('orders',[])),
                        'customers': len(data.get('customers',[]))})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/ping')
def ping():
    """Health check + data status — also used as keep-alive"""
    db = load_db()
    return jsonify({
        'ok': True,
        'storage': 'persistent_volume',
        'data_dir': DATA_DIR,
        'data_file_exists': os.path.exists(DATA_FILE),
        'orders': len(db.get('orders', [])),
        'customers': len(db.get('customers', []))
    })

@app.route('/api/keepalive')
def keepalive():
    """Lightweight endpoint for keep-alive pings"""
    return jsonify({'ok': True, 'ts': datetime.now().isoformat()})

# ── Inquiries ────────────────────────────────────────────
@app.route('/api/inquiries', methods=['GET'])
@login_required
def get_inquiries():
    return jsonify(load_db().get('inquiries', []))

@app.route('/api/inquiries', methods=['POST'])
@login_required
def create_inquiry():
    db = load_db(); inq = request.json
    inq['id'] = str(uuid.uuid4())
    inq['createdAt'] = datetime.now().isoformat()
    inq['createdBy'] = current_user()['username']
    if 'inquiries' not in db: db['inquiries'] = []
    # Auto-generate inquiry number
    if not inq.get('nr'):
        counter = db.get('inquiry_counter', len(db['inquiries']) + 1)
        year = datetime.now().year
        cfg = load_config()
        prefix = cfg.get('inquiry_prefix', 'ANF') or 'ANF'
        inq['nr'] = f'{prefix}-{year}-{str(counter).zfill(4)}'
        db['inquiry_counter'] = counter + 1
    db['inquiries'].append(inq)
    save_db(db)
    return jsonify(inq), 201

@app.route('/api/inquiries/next-nr', methods=['GET'])
@login_required
def next_inquiry_nr():
    db = load_db(); cfg = load_config()
    counter = db.get('inquiry_counter', len(db.get('inquiries',[])) + 1)
    year = datetime.now().year
    prefix = cfg.get('inquiry_prefix', 'ANF') or 'ANF'
    return jsonify({'nr': f'{prefix}-{year}-{str(counter).zfill(4)}'})

@app.route('/api/inquiries/<iid>', methods=['PUT'])
@login_required
def update_inquiry(iid):
    db = load_db()
    for i, inq in enumerate(db.get('inquiries', [])):
        if inq['id'] == iid:
            upd = request.json
            upd['id'] = iid
            upd['createdAt'] = inq.get('createdAt', '')
            upd['createdBy'] = inq.get('createdBy', '')
            upd['updatedBy'] = current_user()['username']
            db['inquiries'][i] = upd
            save_db(db)
            return jsonify(upd)
    return jsonify({'error': 'not found'}), 404

@app.route('/api/inquiries/set-counter', methods=['POST'])
@login_required
def set_inquiry_counter():
    db = load_db()
    db['inquiry_counter'] = int(request.json.get('counter', 1))
    save_db(db)
    return jsonify({'ok': True})

@app.route('/api/inquiries/<iid>', methods=['DELETE'])
@login_required
def delete_inquiry(iid):
    db = load_db()
    db['inquiries'] = [x for x in db.get('inquiries', []) if x['id'] != iid]
    save_db(db)
    return jsonify({'ok': True})

# ── Email ─────────────────────────────────────────────────
def _smtp_provider(host):
    h = (host or '').lower()
    if 'gmail' in h or 'googlemail' in h: return 'gmail'
    if 'office365' in h or 'outlook' in h or 'hotmail' in h or 'live.com' in h: return 'microsoft'
    return ''

def _smtp_ehlo_name(cfg):
    """Gültiger EHLO-Name (manche Server trennen bei ungültigem Rechnernamen sofort)"""
    import socket
    try:
        n = socket.getfqdn()
        if n and '.' in n and n.isascii() and ' ' not in n and not n.endswith('.local'): return n
    except Exception: pass
    fe = (cfg.get('from_email') or cfg.get('smtp_user') or '')
    dom = fe.split('@')[-1] if '@' in fe else ''
    return dom if '.' in dom else 'mail.example.com'

def _smtp_open(cfg, host, port, use_ssl, timeout=12):
    """Eine Verbindung aufbauen. Fehler enthalten den Schritt, bei dem es scheiterte."""
    import ssl
    u = (cfg.get('smtp_user') or '').strip(); p = cfg.get('smtp_pass') or ''
    ctx = ssl.create_default_context(); srv = None; step = 'Verbindungsaufbau / Begrüßung des Servers'
    try:
        if use_ssl: srv = smtplib.SMTP_SSL(host, port, timeout=timeout, context=ctx, local_hostname=_smtp_ehlo_name(cfg))
        else: srv = smtplib.SMTP(host, port, timeout=timeout, local_hostname=_smtp_ehlo_name(cfg))
        step = 'EHLO'; srv.ehlo()
        if not use_ssl:
            if srv.has_extn('starttls'):
                step = 'STARTTLS (Verschlüsselung)'; srv.starttls(context=ctx); srv.ehlo()
            elif u and p:
                raise ValueError('Der Mail-Server bietet keine verschlüsselte Verbindung (STARTTLS) an – Zugangsdaten werden nicht unverschlüsselt gesendet.')
        if u and p: step = 'Anmeldung'; srv.login(u, p)
        return srv
    except smtplib.SMTPAuthenticationError:
        try: srv.close()
        except Exception: pass
        raise
    except Exception as e:
        try: srv and srv.close()
        except Exception: pass
        if isinstance(e, ValueError): raise
        raise ConnectionError(f'{type(e).__name__}: {e} (bei: {step})')

def _tcp_probe(host, port, timeout=4):
    import socket
    try:
        s = socket.create_connection((host, port), timeout=timeout); s.close(); return 'TCP erreichbar'
    except Exception as e:
        return f'nicht erreichbar ({type(e).__name__})'

def _smtp_connect(cfg):
    host = (cfg.get('smtp_host') or '').strip()
    if not host: raise ValueError('SMTP-Host nicht konfiguriert. Bitte unter ⚙ Einstellungen → E-Mail / SMTP eintragen.')
    try: port = int(cfg.get('smtp_port') or 587)
    except Exception: port = 587
    use_ssl = bool(cfg.get('smtp_ssl')) or port == 465
    prov = _smtp_provider(host)
    tries = [(port, use_ssl)]
    alt = (465, True) if not use_ssl else (587, False)      # automatischer Zweitversuch mit dem anderen Standard-Port
    if alt[0] != port: tries.append(alt)
    errs = []
    for pt, ssl_mode in tries:
        try: return _smtp_open(cfg, host, pt, ssl_mode)
        except smtplib.SMTPAuthenticationError:
            if prov == 'gmail':
                raise ValueError('Gmail hat die Anmeldung abgelehnt. Bitte ein App-Passwort verwenden (Google-Konto → Sicherheit → 2-Schritt-Verifizierung → App-Passwörter) – das normale Passwort funktioniert nicht. Benutzername = vollständige Gmail-Adresse.')
            if prov == 'microsoft':
                raise ValueError('Microsoft hat die Anmeldung abgelehnt. Bei Microsoft 365 muss „Authentifiziertes SMTP“ für das Postfach aktiviert sein (Admin-Center → Benutzer → E-Mail → E-Mail-Apps verwalten). Bei privaten Outlook.com-Konten ein App-Passwort verwenden und Host smtp-mail.outlook.com eintragen.')
            raise ValueError('Anmeldung fehlgeschlagen. Benutzername (meist die komplette E-Mail-Adresse) und Passwort prüfen.')
        except ValueError: raise
        except ConnectionError as e: errs.append((pt, str(e)))
    probes = [(pt, _tcp_probe(host, pt)) for pt, _ in tries]
    probe = ' · '.join(f'Port {pt}: {r}' for pt, r in probes)
    msg = f'Keine Verbindung zu {host} möglich. ' + ' | '.join(f'Port {pt} → {m}' for pt, m in errs) + f'. Test: {probe}.'
    if any(r == 'TCP erreichbar' for _, r in probes):
        msg += ' Der Server ist erreichbar, beendet die Verbindung aber sofort bzw. antwortet nicht – das passiert, wenn der Hoster/die Firewall ausgehende Mail-Verbindungen filtert. Bitte beim Hoster nachfragen (ausgehender SMTP-Verkehr, Ports 587/465) – oder im Menü ✉ E-Mail die Funktion „In Outlook (Desktop) öffnen“ verwenden.'
    else:
        msg += ' Der Mail-Server ist von diesem Server aus nicht erreichbar (Firewall des Hosters sperrt ausgehende Mail-Ports?). Alternative: im Menü ✉ E-Mail „In Outlook (Desktop) öffnen“ verwenden.'
    raise ValueError(msg)

def _send_email(cfg, to_addr, subject, body, attachments=None):
    msg = MIMEMultipart()
    fe = cfg.get('from_email') or cfg.get('smtp_user','')
    fn = cfg.get('from_name','Lagonis Logistics')
    msg['From'] = f"{fn} <{fe}>"; msg['To'] = to_addr; msg['Subject'] = subject
    msg.attach(MIMEText(body,'plain','utf-8'))
    for fname, pdf_bytes in (attachments or []):
        part = MIMEBase('application','pdf'); part.set_payload(pdf_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition','attachment',filename=fname)
        msg.attach(part)
    srv = _smtp_connect(cfg); srv.sendmail(fe, to_addr, msg.as_string()); srv.quit()

# ── PDF ───────────────────────────────────────────────────
DARK=colors.HexColor('#1a2e4a'); GRAY=colors.HexColor('#4a6080')
LGRAY=colors.HexColor('#f0f4f8'); BORDER=colors.HexColor('#c8d8e8')
ACCENT=colors.HexColor('#1a2e4a'); ACCENT2=colors.HexColor('#2563a8')
ACCENT_LIGHT=colors.HexColor('#dbeafe')
GREEN=colors.HexColor('#166534'); GREEN_LIGHT=colors.HexColor('#dcfce7')

LABELS = {
    'de': {
        'waybill':'Transportauftrag','vehicle':'Fahrzeug Nr.','chassis':'Chassis',
        'station':'Station','times':'Zeiten','address':'Adresse',
        'shipping':'Reederei / Hafen / Schiff','ref':'Ref. / Zoll',
        'pickup':'Abholung','recipient':'Empfänger','depot':'Depot',
        'from':'Von','to_':'Bis','plan':'Plan',
        'carrier':'Reederei','port':'Hafen','vessel':'Schiff','ref2':'Ref.','customs':'Zollnr.',
        'pos':'Pos.','container_nr':'Container-Nr.','qty':'Anz.','type':'Typ',
        'content':'Bemerkung / Inhalt','weight_kg':'Gewicht kg','total_weight':'Gesamtgewicht',
        'notes':'Bemerkungen / Fahreranweisung','ready':'Be-/Entladebereitschaft',
        'end':'Be-/Entladeschluss','receipt':'Empfang des Containers mit unverletzter Plombe',
        'signature':'Ort/Datum                         Stempel + Unterschrift',
        'adsp':'Wir arbeiten ausschließlich auf Grundlage der Allgemeinen Deutschen Spediteurbedingungen – ADSp – jeweils neueste Fassung.',
        'created_by':'Erstellt von','date':'Datum',
        'invoice':'Rechnung','inv_nr':'Rechnungs-Nr.','inv_date':'Rechnungsdatum',
        'due_date':'Fälligkeitsdatum','order_ref':'Auftrags-Ref.',
        'description':'Beschreibung','amount':'Menge','unit_price':'Einzelpreis','total':'Betrag',
        'net':'Netto','vat':'MwSt.','gross':'Gesamtbetrag',
        'payment_terms':'Zahlungsbedingungen','open':'OFFEN','paid':'BEZAHLT',
        'footer':'Alle Angaben ohne Gewähr  |  Es gelten unsere AGB',
        'delivery_note':'Lieferschein','delivery_date':'Lieferdatum','price':'Preis',
        'reminder':'Zahlungserinnerung','dunning':'Mahnung',
        'reminder_text':'wir möchten Sie höflich darauf hinweisen, dass die nachfolgende Rechnung noch nicht beglichen wurde. Wir bitten Sie, den ausstehenden Betrag bis zum angegebenen Datum zu überweisen.',
        'dunning_text':'die nachfolgende Rechnung ist trotz des abgelaufenen Zahlungsziels noch offen. Wir bitten Sie dringend, den ausstehenden Betrag umgehend zu begleichen, um weitere Maßnahmen zu vermeiden.',
        'dear':'Sehr geehrte Damen und Herren,','regards':'Mit freundlichen Grüßen',
        'invoice_ref':'Rechnungs-Referenz','outstanding':'Offener Betrag',
    },
    'en': {
        'waybill':'Transport Order','vehicle':'Vehicle No.','chassis':'Chassis',
        'station':'Station','times':'Times','address':'Address',
        'shipping':'Carrier / Port / Vessel','ref':'Ref. / Customs',
        'pickup':'Pickup','recipient':'Recipient','depot':'Depot',
        'from':'From','to_':'To','plan':'Planned',
        'carrier':'Carrier','port':'Port','vessel':'Vessel','ref2':'Ref.','customs':'Customs No.',
        'pos':'Pos.','container_nr':'Container No.','qty':'Qty','type':'Type',
        'content':'Remark / Content','weight_kg':'Weight kg','total_weight':'Total weight',
        'notes':'Remarks / Driver instructions','ready':'Loading/Unloading ready',
        'end':'Loading/Unloading end','receipt':'Receipt of container with intact seal',
        'signature':'Place/Date                         Stamp + Signature',
        'adsp':'We operate exclusively on the basis of the German Freight Forwarders Standard Terms and Conditions – ADSp – latest version.',
        'created_by':'Created by','date':'Date',
        'invoice':'Invoice','inv_nr':'Invoice No.','inv_date':'Invoice date',
        'due_date':'Due date','order_ref':'Order reference',
        'description':'Description','amount':'Qty','unit_price':'Unit price','total':'Amount',
        'net':'Net','vat':'VAT','gross':'Total amount',
        'payment_terms':'Payment terms','open':'OPEN','paid':'PAID',
        'footer':'All information without guarantee  |  Our terms and conditions apply',
        'delivery_note':'Delivery Note','delivery_date':'Delivery Date','price':'Price',
        'reminder':'Payment Reminder','dunning':'Dunning Notice',
        'reminder_text':'we would like to kindly inform you that the following invoice remains unpaid. Please transfer the outstanding amount by the specified due date.',
        'dunning_text':'the following invoice is still outstanding despite the payment deadline having passed. We urgently request that you settle the outstanding amount immediately to avoid further action.',
        'dear':'Dear Sir or Madam,','regards':'Kind regards',
        'invoice_ref':'Invoice reference','outstanding':'Outstanding amount',
    }
}

def S():
    return {
        'h1':    ParagraphStyle('h1',    fontName='Helvetica-Bold',fontSize=15,textColor=DARK,spaceAfter=1),
        'h3':    ParagraphStyle('h3',    fontName='Helvetica-Bold',fontSize=8, textColor=GRAY,spaceAfter=1),
        'normal':ParagraphStyle('normal',fontName='Helvetica',     fontSize=8, textColor=DARK,leading=11),
        'small': ParagraphStyle('small', fontName='Helvetica',     fontSize=7, textColor=GRAY,leading=10),
        'right': ParagraphStyle('right', fontName='Helvetica',     fontSize=8, textColor=DARK,alignment=TA_RIGHT),
        'bold':  ParagraphStyle('bold',  fontName='Helvetica-Bold',fontSize=8, textColor=DARK),
        'bold_r':ParagraphStyle('bold_r',fontName='Helvetica-Bold',fontSize=9, textColor=DARK,alignment=TA_RIGHT),
        'center':ParagraphStyle('center',fontName='Helvetica',     fontSize=7, textColor=GRAY,alignment=TA_CENTER),
        'label': ParagraphStyle('label', fontName='Helvetica-Bold',fontSize=7, textColor=GRAY,leading=10),
        'value': ParagraphStyle('value', fontName='Helvetica',     fontSize=9, textColor=DARK,leading=12),
        'body':  ParagraphStyle('body',  fontName='Helvetica',     fontSize=10,textColor=DARK,leading=15),
    }

def _logo_hdr(story, s, W, cfg):
    cn = cfg.get('company_name','Lagonis Logistics')
    parts = [f'<b>{cn}</b>']
    for k,lbl in [('company_address',''),('company_zip',''),('company_city',''),
                  ('company_phone','Tel.: '),('company_email',''),('company_web','')]:
        v = cfg.get(k,'')
        if v: parts.append(lbl+v)
    # merge zip + city
    z,c = cfg.get('company_zip',''), cfg.get('company_city','')
    parts2 = [f'<b>{cn}</b>']
    if cfg.get('company_address'): parts2.append(cfg['company_address'])
    if z or c: parts2.append(' '.join(filter(None,[z,c])))
    for k,lbl in [('company_phone','Tel.: '),('company_email',''),('company_web','')]:
        v = cfg.get(k,'')
        if v: parts2.append(lbl+v)
    cell_l = Image(LOGO_PATH,width=35*mm,height=25*mm,kind='proportional') if os.path.exists(LOGO_PATH) else Paragraph('<b>HanseaERP</b>',s['h1'])
    t = Table([[cell_l, Paragraph('<br/>'.join(parts2), s['small'])]], colWidths=[50*mm,W-50*mm])
    t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(1,0),(1,0),'RIGHT')]))
    story.append(t)
    story.append(HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=6))

def eur(v): return f"€ {float(v):,.2f}".replace(',','X').replace('.',',').replace('X','.')

def addr_block(name,addr,zip_,city_):
    return '\n'.join(filter(None,[name,addr,' '.join(filter(None,[zip_,city_]))]))

def generate_order_pdf(order, cfg=None, lang='de'):
    cfg=cfg or {}; L=LABELS[lang]; buf=io.BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=20*mm)
    W=A4[0]-40*mm; s=S(); story=[]
    _logo_hdr(story,s,W,cfg)
    _author = order.get('author','') or ''
    _frachtf = order.get('frachtfuehrer','') or ''
    _info_parts = [f"{L['created_by']}: {_author}" if _author else '']
    if _frachtf: _info_parts.append(f"Frachtführer: {_frachtf}")
    _info_parts.append(f"{L['date']}: {fmt_dt(order.get('createdAt',''),lang)}")
    story+=[Paragraph(L['waybill'],s['h3']),Paragraph(order.get('nr',''),s['h1']),Spacer(1,3),
            Paragraph('   |   '.join(p for p in _info_parts if p),s['small']),
            Spacer(1,6)]
    vt=Table([[Paragraph(L['vehicle'],s['label']),Paragraph(L['chassis'],s['label'])],
              [Paragraph(order.get('vehicle','-'),s['value']),Paragraph(order.get('chassis','-'),s['value'])]],
             colWidths=[W/2,W/2])
    vt.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),6)]))
    story+=[vt,Spacer(1,8)]
    def sr(lbl, zeit, name, addr, zip_, city_, rd, hf, sc, ref, zoll, anm=''):
        # zeit = combined time string already formatted
        return [Paragraph(lbl, s['bold']),
                Paragraph(zeit or '-', s['small']),
                Paragraph(addr_block(name,addr,zip_,city_) or '-', s['small']),
                Paragraph(f"{L['carrier']}: {rd or '-'}\n{L['port']}: {hf or '-'}\n{L['vessel']}: {sc or '-'}", s['small']),
                Paragraph(f"{L['ref2']}: {ref or '-'}", s['small']),
                Paragraph(anm or '-', s['small'])]

    def fmt_zeit(von, plan, zeit=''):
        # If new 'zeit' field exists, use it directly
        if zeit: return str(zeit)
        parts = []
        if von: parts.append(f"Von: {fmt_dt(von,lang)}")
        if plan: parts.append(f"Plan: {fmt_dt(plan,lang)}")
        return '\n'.join(parts) if parts else '-'

    sd=[[Paragraph(h,s['label']) for h in ['Station','Zeiten','Adresse','Reederei / Hafen / Schiff','Referenz','Anmerkungen']],
        sr('1 · Aufnahme / Laden',
           fmt_zeit(order.get('s1von'),order.get('s1plan'),order.get('s1zeit','')),
           order.get('s1name'),order.get('s1addr'),order.get('s1zip'),order.get('s1city'),
           order.get('s1reederei'),order.get('s1hafen'),order.get('s1schiff'),order.get('s1ref'),order.get('s1zoll'),order.get('s1anm','')),
        sr('2 · Gestellung / Lieferung',
           fmt_zeit(order.get('s2von'),order.get('s2plan'),order.get('s2zeit','')),
           order.get('s2name'),order.get('s2addr'),order.get('s2zip'),order.get('s2city'),
           None,order.get('s2hafen'),order.get('s2schiff'),order.get('s2ref'),None,order.get('s2anm','')),
        sr('3 · Leer-Rückgabe',
           fmt_zeit(None,order.get('s3plan'),order.get('s3zeit','')),
           order.get('s3name'),order.get('s3addr'),order.get('s3zip'),order.get('s3city'),
           order.get('s3reederei'),None,None,order.get('s3ref'),order.get('s3zoll'),order.get('s3anm',''))]
    # Station table — 6 cols totalling exactly W (170mm)
    # 26+24+44+38+20+18 = 170mm ✓
    st=Table(sd,colWidths=[26*mm, 24*mm, 44*mm, 38*mm, 20*mm, 18*mm])
    st.setStyle(TableStyle([
        ('BOX',(0,0),(-1,-1),1,ACCENT),
        ('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),ACCENT),
        ('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
        ('FONTSIZE',(0,0),(-1,0),7),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('TOPPADDING',(0,0),(-1,-1),4),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),4),
        ('RIGHTPADDING',(0,0),(-1,-1),4),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f8fafc')]),
    ]))
    story+=[st,Spacer(1,8)]
    show_price=order.get('showPrice',False)
    hdr=[L['pos'],L['container_nr'],L['qty'],L['type'],L['content'],L['weight_kg']]
    if show_price: hdr.append(L['price'])
    cd=[[Paragraph(h,s['label']) for h in hdr]]
    for i,c in enumerate(order.get('containers',[]),1):
        row=[Paragraph(str(i),s['normal']),Paragraph(c.get('nr','-'),s['bold']),
             Paragraph(str(c.get('anz',1)),s['normal']),Paragraph(c.get('type','-'),s['normal']),
             Paragraph(c.get('remark','-'),s['small']),
             Paragraph(f"{float(c.get('weight',0)):,.0f}".replace(',','.'),s['right'])]
        if show_price: row.append(Paragraph(eur(c['price']) if c.get('price') else '-',s['right']))
        cd.append(row)
    tw=sum(float(c.get('weight',0)) for c in order.get('containers',[]))
    foot=['','','','',Paragraph(f"{L['total_weight']}:",s['bold']),Paragraph(f"{tw:,.0f} kg".replace(',','.'),s['bold_r'])]
    if show_price: foot.append(Paragraph('',s['normal']))
    cd.append(foot)
    cw=[12*mm,38*mm,12*mm,26*mm,W-133*mm,28*mm]
    if show_price: cw=[12*mm,33*mm,12*mm,22*mm,W-150*mm,25*mm,23*mm]
    ct=Table(cd,colWidths=cw)
    ct.setStyle(TableStyle([('BOX',(0,0),(-1,-2),0.5,BORDER),('INNERGRID',(0,0),(-1,-2),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('LINEABOVE',(0,-1),(-1,-1),0.5,BORDER),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),5),('ALIGN',(5,0),(5,-1),'RIGHT')]))
    story+=[ct,Spacer(1,8)]
    if order.get('remarks'):
        rt=Table([[Paragraph(L['notes'],s['label'])],[Paragraph(order['remarks'],s['small'])]],colWidths=[W])
        rt.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('BACKGROUND',(0,0),(0,0),LGRAY),
            ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),6)]))
        story+=[rt,Spacer(1,8)]
    sigt=Table([[Paragraph(f"{L['ready']}\nam: _________  um: _________ Uhr",s['small']),
                 Paragraph(f"{L['end']}\nam: _________  um: _________ Uhr",s['small']),
                 Paragraph(f"{L['receipt']}\n\n\n\n{L['signature']}",s['small'])]],
               colWidths=[W/3,W/3,W/3])
    sigt.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('VALIGN',(0,0),(-1,-1),'TOP'),('TOPPADDING',(0,0),(-1,-1),6),
        ('BOTTOMPADDING',(0,0),(-1,-1),30),('LEFTPADDING',(0,0),(-1,-1),6)]))
    story+=[sigt,Spacer(1,6),Paragraph(L['adsp'],s['center'])]
    doc.build(story); return buf.getvalue()

def generate_delivery_pdf(order, cfg=None, lang='de'):
    cfg=cfg or {}; L=LABELS[lang]; buf=io.BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=20*mm)
    W=A4[0]-40*mm; s=S(); story=[]
    _logo_hdr(story,s,W,cfg)
    story+=[Paragraph(L['delivery_note'],s['h3']),Paragraph(order.get('nr',''),s['h1']),Spacer(1,4),
            Paragraph(f"{L['delivery_date']}: {fmt_dt(order.get('s2plan') or order.get('createdAt',''),lang)}   |   {L['created_by']}: {order.get('author','')}",s['small']),
            Spacer(1,10)]
    from_a=addr_block(order.get('s1name'),order.get('s1addr'),order.get('s1zip'),order.get('s1city'))
    to_a=addr_block(order.get('s2name'),order.get('s2addr'),order.get('s2zip'),order.get('s2city'))
    at=Table([[Paragraph(f'<b>{L["pickup"]}:</b><br/>{from_a or "-"}',s['normal']),
               Paragraph(f'<b>{L["recipient"]}:</b><br/>{to_a or "-"}',s['normal'])]],colWidths=[W/2,W/2])
    at.setStyle(TableStyle([('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),8)]))
    story+=[at,HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=8)]
    cd=[[Paragraph(h,s['label']) for h in [L['pos'],L['container_nr'],L['qty'],L['type'],L['content'],L['weight_kg']]]]
    for i,c in enumerate(order.get('containers',[]),1):
        cd.append([Paragraph(str(i),s['normal']),Paragraph(c.get('nr','-'),s['bold']),
                   Paragraph(str(c.get('anz',1)),s['normal']),Paragraph(c.get('type','-'),s['normal']),
                   Paragraph(c.get('remark','-'),s['small']),
                   Paragraph(f"{float(c.get('weight',0)):,.0f}".replace(',','.'),s['right'])])
    tw=sum(float(c.get('weight',0)) for c in order.get('containers',[]))
    cd.append(['','','','',Paragraph(f"{L['total_weight']}:",s['bold']),Paragraph(f"{tw:,.0f} kg".replace(',','.'),s['bold_r'])])
    ct=Table(cd,colWidths=[12*mm,40*mm,12*mm,28*mm,W-130*mm,30*mm])
    ct.setStyle(TableStyle([('BOX',(0,0),(-1,-2),0.5,BORDER),('INNERGRID',(0,0),(-1,-2),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('LINEABOVE',(0,-1),(-1,-1),0.5,BORDER),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),5),('ALIGN',(5,0),(5,-1),'RIGHT')]))
    story+=[ct,Spacer(1,20),HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=8)]
    sig=Table([[Paragraph('_______________________________\n'+L['signature'],s['small']),
                Paragraph('_______________________________\n'+L['signature'],s['small'])]],colWidths=[W/2,W/2])
    story+=[sig]
    doc.build(story); return buf.getvalue()

def generate_invoice_pdf(inv, order, cfg=None, lang='de'):
    cfg=cfg or {}; L=LABELS[lang]; buf=io.BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=20*mm)
    W=A4[0]-40*mm; s=S(); story=[]
    _logo_hdr(story,s,W,cfg)
    info=(f"{L['inv_nr']}: <b>{inv.get('nr','')}</b><br/>{L['inv_date']}: {fmt_date(inv.get('date',''),lang)}"
          f"<br/>{L['due_date']}: {fmt_date(inv.get('due',''),lang)}<br/>{L['order_ref']}: {inv.get('orderNr','-')}")
    ht=Table([[Paragraph(L['invoice'],s['h1']),Paragraph(info,s['right'])]],colWidths=[W*0.5,W*0.5])
    ht.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP')]))
    story+=[ht,Spacer(1,10)]
    # Customer address block (left side, like a letter)
    cname=inv.get('customerName','')
    if cname:
        aparts=list(filter(None,[cname,inv.get('customerAddress',''),
                                  ' '.join(filter(None,[inv.get('customerZip',''),inv.get('customerCity','')] ))]))
        addr_style = ParagraphStyle('addr',fontName='Helvetica',fontSize=9,textColor=DARK,leading=14,spaceAfter=12)
        story+=[Paragraph('<br/>'.join(aparts), addr_style), Spacer(1,6)]
    story.append(HRFlowable(width='100%',thickness=1.5,color=ACCENT,spaceAfter=6))
    # Greeting text — styled nicely after address and separator
    _greet_de = 'Sehr geehrte Damen und Herren,<br/><br/>vielen Dank für Ihren Auftrag und das damit verbundene Vertrauen!<br/>Hiermit stellen wir Ihnen die folgenden Leistungen in Rechnung:'
    _greet_en = 'Dear Sir or Madam,<br/><br/>thank you for your order and the trust you have placed in us!<br/>We hereby invoice you for the following services:'
    _greet_style = ParagraphStyle('greet',fontName='Helvetica',fontSize=9,textColor=DARK,leading=13,spaceAfter=10,leftIndent=0)
    story.append(Paragraph(_greet_de if lang=='de' else _greet_en, _greet_style))
    story.append(Spacer(1,4))
    positions=[p for p in inv.get('positions',[]) if str(p.get('desc','')).strip() or float(p.get('price',0))>0]
    pd2=[[Paragraph(h,s['label']) for h in [L['pos'],L['description'],L['amount'],L['unit_price'],L['total']]]]
    for i,p in enumerate(positions,1):
        pd2.append([Paragraph(str(i),s['normal']),Paragraph(str(p.get('desc','')),s['normal']),
                    Paragraph(str(p.get('qty',1)),s['normal']),
                    Paragraph(eur(p.get('price',0)),s['right']),Paragraph(eur(p.get('line',0)),s['right'])])
    net=float(inv.get('net',0)); vp=int(inv.get('vat',19)); va=float(inv.get('vatAmt',0)); tot=float(inv.get('total',0))
    pd2+=[['','','',Paragraph(f"{L['net']}:",s['right']),Paragraph(eur(net),s['right'])],
          ['','','',Paragraph(f"{L['vat']} {vp}%:",s['right']),Paragraph(eur(va),s['right'])],
          ['','','',Paragraph(f"{L['gross']}:",s['bold_r']),Paragraph(eur(tot),s['bold_r'])]]
    n=len(pd2)
    pt=Table(pd2,colWidths=[12*mm,W-105*mm,20*mm,36*mm,37*mm])
    pt.setStyle(TableStyle([('BOX',(0,0),(-1,n-4),0.5,BORDER),('INNERGRID',(0,0),(-1,n-4),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('LINEABOVE',(3,n-3),(-1,n-3),0.5,BORDER),
        ('LINEABOVE',(3,n-1),(-1,n-1),1.0,DARK),('LINEBELOW',(3,n-1),(-1,n-1),0.5,BORDER),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(2,0),(-1,-1),'RIGHT'),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5)]))
    story+=[pt,Spacer(1,12)]
    pay=[]
    if inv.get('payment'): pay.append(f"<b>{L['payment_terms']}:</b> {inv['payment']}")
    bi='  ·  '.join(filter(None,[f"IBAN: {cfg.get('bank_iban','')}" if cfg.get('bank_iban') else '',
                                  f"BIC: {cfg.get('bank_bic','')}" if cfg.get('bank_bic') else '',
                                  f"Bank: {cfg.get('bank_name','')}" if cfg.get('bank_name') else '']))
    if bi: pay.append(bi)
    if inv.get('notes'): pay.append(inv['notes'])
    if pay: story+=[Paragraph('<br/>'.join(pay),s['small']),Spacer(1,8)]
    st_txt=f"✓  {L['paid']}" if inv.get('status')=='paid' else f"{L['open']} – {'Bitte überweisen Sie den Betrag fristgerecht.' if lang=='de' else 'Please transfer the outstanding amount by the due date.'}"
    story+=[Paragraph(st_txt,s['bold']),Spacer(1,20),
            HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=6),
            Paragraph(f"{cfg.get('company_name','Lagonis Logistics')}  |  {L['footer']}",s['center'])]
    doc.build(story); return buf.getvalue()

def generate_reminder_pdf(inv, cfg=None, doc_type='reminder', lang='de'):
    cfg=cfg or {}; L=LABELS[lang]; buf=io.BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=25*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=20*mm)
    W=A4[0]-45*mm; s=S(); story=[]
    _logo_hdr(story,s,W,cfg)
    title=L['dunning'] if doc_type=='dunning' else L['reminder']
    body_text=L['dunning_text'] if doc_type=='dunning' else L['reminder_text']
    story+=[Spacer(1,10),Paragraph(title,s['h1']),Spacer(1,12)]
    today=datetime.now().strftime('%d.%m.%Y') if lang=='de' else datetime.now().strftime('%m/%d/%Y')
    story+=[Paragraph(f"{L['date']}: {today}",s['small']),Spacer(1,16),
            Paragraph(L['dear'],s['body']),Spacer(1,8),Paragraph(body_text,s['body']),Spacer(1,16)]
    tot=float(inv.get('total',0))
    rows=[[Paragraph(h,s['label']) for h in [L['invoice_ref'],L['inv_date'],L['due_date'],L['outstanding']]],
          [Paragraph(inv.get('nr',''),s['bold']),Paragraph(fmt_date(inv.get('date',''),lang),s['normal']),
           Paragraph(fmt_date(inv.get('due',''),lang),s['normal']),Paragraph(eur(tot),s['bold'])]]
    tbl=Table(rows,colWidths=[50*mm,35*mm,35*mm,W-120*mm])
    tbl.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),6),('ALIGN',(3,0),(3,-1),'RIGHT')]))
    story+=[tbl,Spacer(1,16)]
    if doc_type=='dunning': story+=[Paragraph(f"{L['outstanding']}: <b>{eur(tot)}</b>",s['body']),Spacer(1,8)]
    if inv.get('payment'): story.append(Paragraph(f"<b>{L['payment_terms']}:</b> {inv['payment']}",s['small']))
    bi='  ·  '.join(filter(None,[f"IBAN: {cfg.get('bank_iban','')}" if cfg.get('bank_iban') else '',
                                  f"BIC: {cfg.get('bank_bic','')}" if cfg.get('bank_bic') else '',
                                  f"Bank: {cfg.get('bank_name','')}" if cfg.get('bank_name') else '']))
    if bi: story+=[Spacer(1,4),Paragraph(bi,s['small'])]
    story+=[Spacer(1,24),Paragraph(L['regards'],s['body']),Spacer(1,6),
            Paragraph(cfg.get('company_name','Lagonis Logistics'),s['bold']),
            Spacer(1,20),HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=6),
            Paragraph(f"{cfg.get('company_name','Lagonis Logistics')}  |  {L['footer']}",s['center'])]
    doc.build(story); return buf.getvalue()

# ════════════════════════════════════════════════════════
# DISPO MODULE
# ════════════════════════════════════════════════════════
from reportlab.lib.pagesizes import landscape
from reportlab.platypus import PageBreak

def safe_float(v):
    try: return float(str(v).replace(',', '.')) if v not in ('', None) else 0.0
    except: return 0.0

# key, Label, Gruppe
DISPO_FIELDS = [
    ('nr', 'Dispo-Nr.', 'termine'),
    ('auftragsnr', 'Auftragsnummer', 'termine'),
    ('datum', 'Datum', 'termine'),
    ('lieferdatum', 'Lieferdatum', 'termine'),
    ('kunde', 'Kunde', 'termine'),
    ('fahrzeug_nr', 'Fahrzeug-Nr.', 'termine'),
    ('status', 'Status', 'termine'),
    ('aufnahme', 'Aufnahme', 'stationen'),
    ('aufnahme_adresse', 'Adresse Aufnahme', 'stationen'),
    ('slot_aufnahme', 'Slot Aufnahme', 'stationen'),
    ('tourenplan_aufnahme', 'Tourenplan Aufnahme', 'stationen'),
    ('gestellung', 'Gestellung', 'stationen'),
    ('gestellung_adresse', 'Adresse Gestellung', 'stationen'),
    ('slot_gestellung', 'Slot/Zeit Gestellung', 'stationen'),
    ('ref_gestellung', 'Referenz Gestellung', 'stationen'),
    ('leerdepot', 'Leerdepot', 'stationen'),
    ('leerdepot_adresse', 'Adresse Leerdepot', 'stationen'),
    ('slot_leerdepot', 'Slot Leerdepot', 'stationen'),
    ('tourenplan_leerdepot', 'Tourenplan Leerdepot', 'stationen'),
    ('container_nr', 'Container-Nr.', 'container'),
    ('container_typ', 'Containertyp', 'container'),
    ('container_extra', 'Weitere Container', 'container'),
    ('anzahl_container', 'Anzahl Container', 'container'),
    ('slot_gebucht', 'Slot gebucht', 'container'),
    ('reederei', 'Reederei', 'container'),
    ('einkaufspreis', 'Einkaufspreis', 'finanzen'),
    ('verkaufspreis', 'Verkaufspreis', 'finanzen'),
    ('anmerkung', 'Anmerkung', 'anmerkung'),
]
DISPO_KEYS = [f[0] for f in DISPO_FIELDS]
DISPO_LABEL = {f[0]: f[1] for f in DISPO_FIELDS}
DISPO_PDF_DEFAULT = [k for k in DISPO_KEYS if k not in ('nr', 'einkaufspreis', 'verkaufspreis')]
DISPO_STATUS = {'offen': 'Offen', 'bestaetigt': 'Bestätigt', 'in_fahrt': 'In Fahrt', 'erledigt': 'Erledigt', 'abgelehnt': 'Abgelehnt', 'storniert': 'Storniert'}
DISPO_BULK_FIELDS = [k for k in DISPO_KEYS if k not in ('nr', 'container_extra')]
NEW_FIELDS = {'container_extra': 3, 'gestellung_adresse': 4, 'aufnahme_adresse': 5, 'leerdepot_adresse': 5, 'kunde': 6}   # Feld -> Version, ab der es bekannt ist (ältere Einstellungen bekommen es automatisch)

def _extras(d):
    """Weitere Container als [(nr, typ)] – akzeptiert Liste oder CSV-Text 'NR:TYP | NR:TYP'"""
    v = d.get('container_extra'); out = []
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict) and (x.get('nr') or x.get('typ')): out.append((str(x.get('nr', '')), str(x.get('typ', ''))))
    elif isinstance(v, str) and v.strip():
        for part in v.split('|'):
            nr, _, typ = part.strip().partition(':')
            if nr.strip() or typ.strip(): out.append((nr.strip(), typ.strip()))
    return out
def _extras_str(d): return ' | '.join(f'{n}:{t}' for n, t in _extras(d))

def _dispo_prefix():
    p = str(load_config().get('dispo_prefix') or 'DISPO').strip()
    return p or 'DISPO'

def _next_dispo_nr(db):
    counter = db.get('dispo_counter', len(db.get('dispo', [])) + 1)
    db['dispo_counter'] = counter + 1
    return f'{_dispo_prefix()}-{datetime.now().year}-{str(counter).zfill(4)}'

@app.route('/api/dispo', methods=['GET'])
@login_required
def get_dispo():
    return jsonify(load_db().get('dispo', []))

@app.route('/api/dispo/next-nr', methods=['GET'])
@login_required
def next_dispo_nr():
    db = load_db()
    counter = db.get('dispo_counter', len(db.get('dispo', [])) + 1)
    return jsonify({'nr': f'{_dispo_prefix()}-{datetime.now().year}-{str(counter).zfill(4)}'})

@app.route('/api/dispo/set-counter', methods=['POST'])
@login_required
@admin_required
def set_dispo_counter():
    db = load_db()
    try: val = max(1, int((request.json or {}).get('counter', 1)))
    except Exception: return jsonify({'ok': False, 'message': 'Ungültige Zahl'}), 400
    db['dispo_counter'] = val; save_db(db)
    return jsonify({'ok': True, 'counter': val})

@app.route('/api/dispo', methods=['POST'])
@login_required
def create_dispo():
    db = load_db()
    d = request.json or {}
    d['id'] = str(uuid.uuid4())
    d['createdAt'] = datetime.now().isoformat()
    d['createdBy'] = current_user()['username']
    db.setdefault('dispo', [])
    if not d.get('nr'): d['nr'] = _next_dispo_nr(db)
    db['dispo'].append(d)
    save_db(db)
    return jsonify(d), 201

@app.route('/api/dispo/<did>', methods=['GET'])
@login_required
def get_dispo_one(did):
    d = next((x for x in load_db().get('dispo', []) if x['id'] == did), None)
    return (jsonify(d), 200) if d else (jsonify({'error': 'not found'}), 404)

@app.route('/api/dispo/<did>', methods=['PUT'])
@login_required
def update_dispo(did):
    db = load_db()
    for i, d in enumerate(db.get('dispo', [])):
        if d['id'] == did:
            upd = request.json or {}
            upd.update({'id': did, 'createdAt': d.get('createdAt', ''), 'createdBy': d.get('createdBy', ''),
                        'nr': d.get('nr', ''), 'updatedBy': current_user()['username']})
            db['dispo'][i] = upd
            save_db(db)
            return jsonify(upd)
    return jsonify({'error': 'not found'}), 404

@app.route('/api/dispo/<did>', methods=['DELETE'])
@login_required
def delete_dispo(did):
    db = load_db()
    db['dispo'] = [d for d in db.get('dispo', []) if d['id'] != did]
    save_db(db)
    return jsonify({'ok': True})

@app.route('/api/dispo/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_dispo():
    ids = set((request.json or {}).get('ids', []))
    db = load_db()
    db['dispo'] = [d for d in db.get('dispo', []) if d['id'] not in ids]
    save_db(db)
    return jsonify({'ok': True})

@app.route('/api/dispo/bulk-update', methods=['POST'])
@login_required
def bulk_update_dispo():
    data = request.json or {}
    ids = set(data.get('ids', []))
    fields = {k: v for k, v in (data.get('fields') or {}).items() if k in DISPO_BULK_FIELDS}
    if not ids or not fields: return jsonify({'ok': False, 'message': 'Nichts zu ändern'}), 400
    db = load_db(); n = 0
    for d in db.get('dispo', []):
        if d['id'] in ids:
            d.update(fields); d['updatedBy'] = current_user()['username']; n += 1
    save_db(db)
    return jsonify({'ok': True, 'count': n})

@app.route('/api/dispo/bulk-duplicate', methods=['POST'])
@login_required
def bulk_duplicate_dispo():
    ids = set((request.json or {}).get('ids', []))
    db = load_db(); new = []
    for d in db.get('dispo', []):
        if d['id'] in ids:
            c = {k: v for k, v in d.items() if k not in ('id', 'nr', 'createdAt', 'createdBy', 'updatedBy')}
            c.update({'id': str(uuid.uuid4()), 'createdAt': datetime.now().isoformat(),
                      'createdBy': current_user()['username'], 'status': 'offen'})
            new.append(c)
    for c in new: c['nr'] = _next_dispo_nr(db)
    db.setdefault('dispo', []).extend(new)
    save_db(db)
    return jsonify({'ok': True, 'count': len(new)})

@app.route('/api/dispo/export-csv', methods=['POST'])
@login_required
def export_dispo_csv():
    import csv, io as sio
    data = request.json or {}
    dispos = load_db().get('dispo', [])
    ids = data.get('ids', [])
    fr, to = data.get('from', ''), data.get('to', '')
    if ids:
        dispos = [d for d in dispos if d['id'] in ids]
    elif fr or to:   # Zeitraum richtet sich nach dem Lieferdatum
        dispos = [d for d in dispos if (not fr or (d.get('lieferdatum') or '')[:10] >= fr)
                  and (not to or (d.get('lieferdatum') or '')[:10] <= to)
                  and (d.get('lieferdatum') or '')]
    out = sio.StringIO()
    w = csv.DictWriter(out, fieldnames=DISPO_KEYS, extrasaction='ignore', delimiter=';')
    w.writeheader()
    for d in dispos:
        row = {f: d.get(f, '') for f in DISPO_KEYS}; row['container_extra'] = _extras_str(d); w.writerow(row)
    buf = io.BytesIO(out.getvalue().encode('utf-8-sig'))
    return send_file(buf, mimetype='text/csv', as_attachment=True,
                     download_name=f"dispo_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

@app.route('/api/dispo/import-template', methods=['GET'])
@login_required
def dispo_import_template():
    import csv, io as sio
    keys = [k for k in DISPO_KEYS if k != 'nr']
    ex = {'auftragsnr':'AB-1001','datum':datetime.now().strftime('%Y-%m-%d'),'lieferdatum':datetime.now().strftime('%Y-%m-%d'),
          'aufnahme':'Hamburg CTA','slot_aufnahme':'07:30-08:00','gestellung':'Musterkunde GmbH, Hamburg','slot_gestellung':'13:00',
          'leerdepot':'Depot Nord','container_nr':'MSKU1234567','container_typ':"40' HC",'container_extra':"TEMU7654321:20' DV",
          'anzahl_container':'1','slot_gebucht':'ja','reederei':'MSC','fahrzeug_nr':'LKW1','status':'offen','anmerkung':'Beispielzeile – bitte löschen'}
    out = sio.StringIO(); w = csv.DictWriter(out, fieldnames=keys, extrasaction='ignore', delimiter=';')
    w.writeheader(); w.writerow({k: ex.get(k, '') for k in keys})
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='HG_Dispo_Import_Vorlage.csv')

@app.route('/api/dispo/import-csv', methods=['POST'])
@login_required
def import_dispo_csv():
    import csv, io as sio
    f = request.files.get('file')
    if not f: return jsonify({'ok': False, 'message': 'Keine Datei'}), 400
    try:
        content = f.read().decode('utf-8-sig')
        db = load_db(); db.setdefault('dispo', []); count = 0
        for row in csv.DictReader(sio.StringIO(content), delimiter=';'):
            row = {k: v for k, v in dict(row).items() if k in DISPO_KEYS}
            row['container_extra'] = [{'nr': n, 'typ': t} for n, t in _extras(row)]
            row.update({'id': str(uuid.uuid4()), 'createdAt': datetime.now().isoformat(),
                        'createdBy': current_user()['username']})
            if not row.get('nr') or row['nr'] in {x.get('nr') for x in db['dispo']}: row['nr'] = _next_dispo_nr(db)   # keine doppelten Nummern
            db['dispo'].append(row); count += 1
        save_db(db)
        return jsonify({'ok': True, 'count': count})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── Tagespauschalen je Fahrzeug (Dispo-Statistik) ──
@app.route('/api/dispo/rates', methods=['GET'])
@login_required
def get_dispo_rates():
    return jsonify(load_db().get('dispo_rates', {}))

@app.route('/api/dispo/rates', methods=['POST'])
@login_required
def set_dispo_rates():
    import re as _re
    data = request.json or {}
    veh = str(data.get('vehicle', '')).strip()
    entries = data.get('entries') or {}
    if not veh or not isinstance(entries, dict):
        return jsonify({'ok': False, 'message': 'Fahrzeug fehlt'}), 400
    db = load_db(); rates = db.setdefault('dispo_rates', {}); vr = rates.setdefault(veh, {})
    for day, val in entries.items():
        if not _re.match(r'^\d{4}-\d{2}-\d{2}$', str(day)): continue
        if val is None or str(val).strip() == '':
            vr.pop(day, None)
        else:
            try: vr[day] = round(float(str(val).replace(',', '.')), 2)
            except ValueError: pass
    if not vr: rates.pop(veh, None)
    save_db(db)
    return jsonify({'ok': True, 'rates': rates})


# ── Tagesinfos je Fahrzeug: weitere Umsätze + Notiz (Dispo-Statistik) ──
@app.route('/api/dispo/dayinfo', methods=['GET'])
@login_required
def get_dispo_dayinfo():
    return jsonify(load_db().get('dispo_dayinfo', {}))

@app.route('/api/dispo/dayinfo', methods=['POST'])
@login_required
def set_dispo_dayinfo():
    import re as _re
    data = request.json or {}
    veh = str(data.get('vehicle', '')).strip()
    entries = data.get('entries') or {}
    if not veh or not isinstance(entries, dict):
        return jsonify({'ok': False, 'message': 'Fahrzeug fehlt'}), 400
    db = load_db(); info = db.setdefault('dispo_dayinfo', {}); vi = info.setdefault(veh, {})
    for day, upd in entries.items():
        if not _re.match(r'^\d{4}-\d{2}-\d{2}$', str(day)) or not isinstance(upd, dict): continue
        e = vi.setdefault(day, {})
        if 'extra' in upd:
            v = upd['extra']
            if v is None or str(v).strip() == '': e.pop('extra', None)
            else:
                try: e['extra'] = round(float(str(v).replace(',', '.')), 2)
                except ValueError: pass
        if 'note' in upd:
            t = str(upd['note'] or '').strip()[:500]
            if t: e['note'] = t
            else: e.pop('note', None)
        if not e: vi.pop(day, None)
    if not vi: info.pop(veh, None)
    save_db(db)
    return jsonify({'ok': True, 'info': info})

# ── Dispo PDFs (Querformat, Felder per Einstellung) ───────
from xml.sax.saxutils import escape as _xesc
_DARK = colors.HexColor('#1a2e4a'); _BLUE = colors.HexColor('#2563a8')
_LGRAY = colors.HexColor('#f0f4f8'); _BORDER = colors.HexColor('#c8d8e8'); _WHITE = colors.white
_GC = {'termine': ('#0e7490', '#ecfeff'), 'stationen': ('#7c3aed', '#f5f3ff'),
       'container': ('#d97706', '#fffbeb'), 'finanzen': ('#059669', '#ecfdf5'), 'anmerkung': ('#db2777', '#fdf2f8')}
_GN = {'termine': 'Termine & Fahrzeug', 'stationen': 'Stationen', 'container': 'Container & Reederei', 'finanzen': 'Finanzen', 'anmerkung': 'Anmerkung'}
_WD = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
STATIONS = [('aufnahme', 'Aufnahme', [('tourenplan_aufnahme', 'Tour'), ('slot_aufnahme', 'Slot')]),
            ('gestellung', 'Gestellung', [('ref_gestellung', 'Ref.'), ('slot_gestellung', 'Slot/Zeit')]),
            ('leerdepot', 'Leerdepot', [('tourenplan_leerdepot', 'Tour'), ('slot_leerdepot', 'Slot')])]

def _pdf_sel(cfg):
    s = (cfg or {}).get('dispo_pdf_fields')
    if not isinstance(s, list): return set(DISPO_PDF_DEFAULT)
    s = set(s)
    v = int((cfg or {}).get('dispo_fields_v') or 0)
    s |= {k for k, nv in NEW_FIELDS.items() if v < nv}
    return s

def _e(v): return _xesc(str(v)) if v not in (None, '') else ''
def _fd(v):
    try: return datetime.strptime(str(v)[:10], '%Y-%m-%d').strftime('%d.%m.%Y')
    except: return _e(v) or '-'
def _money(v): return f"{safe_float(v):,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.') if v not in ('', None) else '-'

def _styles():
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    def P(n, **k):
        return ParagraphStyle(n, fontName=k.pop('f', 'Helvetica'), fontSize=k.pop('s', 8), leading=k.pop('l', 10),
                              textColor=k.pop('c', colors.HexColor('#0f172a')), **k)
    return {'h1': P('h1', f='Helvetica-Bold', s=20, l=24, c=_DARK), 'h3': P('h3', f='Helvetica-Bold', s=10, l=13, c=_BLUE),
            'small': P('sm', s=8, l=10, c=colors.HexColor('#475569')), 'cell': P('ce', s=7.5, l=9.2),
            'val': P('va', f='Helvetica-Bold', s=10, l=12.5, c=_DARK), 'lbl': P('lb', f='Helvetica-Bold', s=6.5, l=8, c=colors.HexColor('#64748b')),
            'th': P('th', f='Helvetica-Bold', s=6.5, l=8, c=_WHITE), 'body': P('bo', s=9.5, l=12.5),
            'ctr': P('ct', s=7, l=9, c=colors.HexColor('#94a3b8'), alignment=TA_CENTER),
            'day': P('dy', f='Helvetica-Bold', s=8.5, l=11, c=_DARK),
            'hdr_r': P('hr', s=8, l=10.5, c=colors.HexColor('#334155'), alignment=TA_RIGHT)}

def _hdr(story, s, W, cfg):
    left = ''
    if os.path.exists(LOGO_PATH):
        try: left = Image(LOGO_PATH, width=72*mm, height=28*mm, kind='proportional', hAlign='LEFT')
        except: left = ''
    lines = ['<b>' + _e(cfg.get('company_name', 'Lagonis Logistics')) + '</b>']
    city = ' '.join(filter(None, [cfg.get('company_zip', ''), cfg.get('company_city', '')]))
    addr = ', '.join(x for x in [_e(cfg.get('company_address', '')), _e(city)] if x)
    if addr: lines.append(addr)
    contact = []
    if cfg.get('company_phone'): contact.append('Tel. ' + _e(cfg['company_phone']))
    if cfg.get('company_email'): contact.append('E-Mail: ' + _e(cfg['company_email']))
    if contact: lines.append(' · '.join(contact))
    if cfg.get('company_web'): lines.append(_e(cfg['company_web']))
    t = Table([[left, Paragraph('<br/>'.join(lines), s['hdr_r'])]], colWidths=[W * 0.45, W * 0.55])
    t.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING', (0,0), (-1,-1), 3), ('LEFTPADDING', (0,0), (0,0), 0), ('RIGHTPADDING', (1,0), (1,0), 0)]))
    story += [t, HRFlowable(width='100%', thickness=1.2, color=_BLUE, spaceAfter=6)]

def _foot(cfg, label):
    t = f"{label}: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {_e(cfg.get('company_name', 'Lagonis Logistics'))}"
    bank = ' · '.join(x for x in [_e(cfg.get('bank_name')), ('IBAN ' + _e(cfg['bank_iban'])) if cfg.get('bank_iban') else '', ('BIC ' + _e(cfg['bank_bic'])) if cfg.get('bank_bic') else ''] if x)
    return t + (f'<br/>{bank}' if bank else '')

def _station_html(d, sel, key, extras):
    lines = []
    if key in sel and d.get(key): lines.append('<b>' + _e(d[key]) + '</b>')
    ak = key + '_adresse'
    if ak in sel and d.get(ak): lines.append('<font color="#475569">' + _e(d[ak]) + '</font>')
    for k, pre in extras:
        if k in sel and d.get(k): lines.append(f'<font color="#475569">{pre}:</font> {_e(d[k])}')
    return '<br/>'.join(lines) or '-'

def _container_html(d, sel):
    lines = []
    if 'container_nr' in sel and d.get('container_nr'): lines.append('<b>' + _e(d['container_nr']) + '</b>')
    if 'container_typ' in sel and d.get('container_typ'): lines.append(_e(d['container_typ']))
    if 'anzahl_container' in sel and d.get('anzahl_container'): lines.append('Anz.: ' + _e(d['anzahl_container']))
    if 'container_extra' in sel:
        for n, t in _extras(d): lines.append('+ ' + _e(n) + (f' ({_e(t)})' if t else ''))
    return '<br/>'.join(lines) or '-'

_HEAD = {'auftragsnr': 'Auftrags-Nr.', 'fahrzeug_nr': 'Fahrzeug', 'slot_gebucht': 'Slot gebucht', 'einkaufspreis': 'Einkauf', 'verkaufspreis': 'Verkauf'}

def _plan_columns(sel):
    """(id, Kopf, Gewicht, Gruppe, Funktion) – Reihenfolge der Tagesplan-Spalten"""
    c = []
    def simple(k, w, fn=None):
        if k in sel:
            g = next(f[2] for f in DISPO_FIELDS if f[0] == k)
            c.append((k, _HEAD.get(k, DISPO_LABEL[k]), w, g, fn or (lambda d, k=k: _e(d.get(k)) or '-')))
    simple('auftragsnr', 2.5); simple('nr', 2.5)
    simple('lieferdatum', 2.1, lambda d: _fd(d.get('lieferdatum'))); simple('datum', 1.8, lambda d: _fd(d.get('datum')))
    simple('kunde', 2.4)
    simple('fahrzeug_nr', 2.0)
    if sel & {'container_nr', 'container_typ', 'anzahl_container', 'container_extra'}:
        c.append(('container', 'Container', 3.3, 'container', lambda d: _container_html(d, sel)))
    for key, name, extras in STATIONS:
        if key in sel or any(k in sel for k, _ in extras) or (key + '_adresse' in sel):
            c.append((key, name, 3.6, 'stationen', lambda d, key=key, extras=extras: _station_html(d, sel, key, extras)))
    simple('slot_gebucht', 1.6, lambda d: 'Ja' if str(d.get('slot_gebucht', '')).lower() in ('ja', 'true', '1') else 'Nein')
    simple('reederei', 1.8); simple('status', 1.5, lambda d: _e(DISPO_STATUS.get(d.get('status'), d.get('status'))) or '-')
    simple('einkaufspreis', 1.5, lambda d: _money(d.get('einkaufspreis'))); simple('verkaufspreis', 1.5, lambda d: _money(d.get('verkaufspreis')))
    simple('anmerkung', 3.0)
    return c

def _plan_sort(d):
    return ((d.get('lieferdatum') or '')[:10], d.get('fahrzeug_nr') or '', d.get('slot_aufnahme') or d.get('tourenplan_aufnahme') or '', d.get('nr') or '')

def _gen_tagesplan_pdf(dispos, cfg, title, subtitle, multi_day):
    buf = io.BytesIO()
    PW, PH = landscape(A4)
    doc = SimpleDocTemplate(buf, pagesize=(PW, PH), leftMargin=9*mm, rightMargin=9*mm, topMargin=9*mm, bottomMargin=9*mm)
    W = PW - 18*mm; s = _styles(); sel = _pdf_sel(cfg); story = []
    _hdr(story, s, W, cfg)
    story += [Paragraph(title, s['h3']), Paragraph(subtitle, s['h1']),
              Paragraph(f'{len(dispos)} Aufträge', s['small']), Spacer(1, 6)]
    cols = _plan_columns(sel)
    if not dispos: story.append(Paragraph('Keine Aufträge für diesen Zeitraum (nach Lieferdatum).', s['body']))
    elif not cols: story.append(Paragraph('In den Einstellungen sind keine PDF-Felder ausgewählt.', s['body']))
    else:
        tot = sum(c[2] for c in cols); cw = [W * c[2] / tot for c in cols]
        rows = [[Paragraph(c[1].upper(), s['th']) for c in cols]]; spans = []; days = []
        last = None
        for d in sorted(dispos, key=_plan_sort):
            day = (d.get('lieferdatum') or '')[:10]
            if multi_day and day != last:
                try: lbl = f"{_WD[datetime.strptime(day, '%Y-%m-%d').weekday()]}, {_fd(day)}"
                except: lbl = day
                rows.append([Paragraph(lbl, s['day'])] + [''] * (len(cols) - 1)); spans.append(len(rows) - 1); days.append(len(rows) - 1); last = day
            rows.append([Paragraph(c[4](d), s['cell']) for c in cols])
        st = [('BOX', (0,0), (-1,-1), 0.8, _DARK), ('INNERGRID', (0,0), (-1,-1), 0.4, _BORDER), ('VALIGN', (0,0), (-1,-1), 'TOP'),
              ('TOPPADDING', (0,0), (-1,-1), 3), ('BOTTOMPADDING', (0,0), (-1,-1), 3), ('LEFTPADDING', (0,0), (-1,-1), 3), ('RIGHTPADDING', (0,0), (-1,-1), 3)]
        for i, c in enumerate(cols):
            st += [('BACKGROUND', (i,0), (i,0), colors.HexColor(_GC[c[3]][0]))]
            st += [('BACKGROUND', (i,1), (i,-1), colors.HexColor(_GC[c[3]][1]))] if False else []
        body_rows = [r for r in range(1, len(rows)) if r not in days]
        for r in body_rows:
            st.append(('BACKGROUND', (0,r), (-1,r), _WHITE if body_rows.index(r) % 2 == 0 else colors.HexColor('#f8fafc')))
        for r in days:
            st += [('SPAN', (0,r), (-1,r)), ('BACKGROUND', (0,r), (-1,r), colors.HexColor('#dbeafe')), ('LINEABOVE', (0,r), (-1,r), 0.8, _BLUE)]
        t = Table(rows, colWidths=cw, repeatRows=1); t.setStyle(TableStyle(st)); story.append(t)
    story += [Spacer(1, 6), Paragraph(_foot(cfg, 'Gedruckt'), s['ctr'])]
    doc.build(story)
    return buf.getvalue()

def _section(name, g, W, s):
    t = Table([[Paragraph(name.upper(), s['th'])]], colWidths=[W])
    t.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,-1), colors.HexColor(_GC[g][0])), ('TOPPADDING', (0,0), (-1,-1), 2.5),
                           ('BOTTOMPADDING', (0,0), (-1,-1), 2.5), ('LEFTPADDING', (0,0), (-1,-1), 6)]))
    return t

def _grid(cells, W, g, s, ncols=6):
    """cells: [(Label, html)] – Label über Wert, ncols je Zeile"""
    if not cells: return None
    ncols = min(ncols, len(cells)); rows = []; st = [('BOX', (0,0), (-1,-1), 0.5, _BORDER), ('INNERGRID', (0,0), (-1,-1), 0.4, _BORDER),
          ('LEFTPADDING', (0,0), (-1,-1), 6), ('TOPPADDING', (0,0), (-1,-1), 3), ('BOTTOMPADDING', (0,0), (-1,-1), 4)]
    for i in range(0, len(cells), ncols):
        chunk = cells[i:i + ncols]; chunk += [('', '')] * (ncols - len(chunk))
        rows.append([Paragraph(c[0].upper(), s['lbl']) for c in chunk]); rows.append([Paragraph((c[1] or '-') if c[0] else '', s['val']) for c in chunk])
        r = len(rows) - 2; st += [('BACKGROUND', (0,r), (-1,r), colors.HexColor(_GC[g][1]))]
    t = Table(rows, colWidths=[W / ncols] * ncols); t.setStyle(TableStyle(st)); return t

def _order_story(d, cfg, s, W):
    sel = _pdf_sel(cfg); story = []
    _hdr(story, s, W, cfg)
    an = d.get('auftragsnr') if 'auftragsnr' in sel else ''
    story += [Paragraph('Transportauftrag / Frachtbrief', s['h3']), Paragraph(_e(an or d.get('nr')), s['h1'])]
    if an: story.append(Paragraph(f"Dispo-Nr.: <b>{_e(d.get('nr'))}</b>", s['body']))
    story.append(Spacer(1, 6))
    def add(g, cells, ncols=3):
        if cells: story.extend([_section(_GN[g], g, W, s), _grid(cells, W, g, s, ncols), Spacer(1, 6)])
    F = lambda k, v=None: (DISPO_LABEL[k], v if v is not None else (_e(d.get(k)) or '-'))
    yn = lambda v: 'Ja' if str(v).lower() in ('ja', 'true', '1') else 'Nein'
    termine = []
    for k in ('auftragsnr', 'nr', 'lieferdatum', 'datum', 'kunde', 'fahrzeug_nr', 'status'):
        if k in sel:
            v = _fd(d.get(k)) if k in ('lieferdatum', 'datum') else (_e(DISPO_STATUS.get(d.get('status'), d.get('status'))) or '-') if k == 'status' else None
            termine.append(F(k, v))
    add('termine', termine)
    blocks = [(name, _station_html(d, sel, key, extras)) for key, name, extras in STATIONS if key in sel or any(k in sel for k, _ in extras) or (key + '_adresse' in sel)]
    add('stationen', blocks)
    cont = [F(k) for k in ('container_nr', 'container_typ', 'anzahl_container', 'reederei') if k in sel]
    if 'slot_gebucht' in sel: cont.append(F('slot_gebucht', yn(d.get('slot_gebucht', ''))))
    if 'container_extra' in sel and _extras(d):
        cont.append(('Weitere Container', '<br/>'.join(_e(n) + (f' ({_e(t)})' if t else '') for n, t in _extras(d))))
    add('container', cont)
    add('finanzen', [F(k, _money(d.get(k))) for k in ('einkaufspreis', 'verkaufspreis') if k in sel])
    if 'anmerkung' in sel: add('anmerkung', [F('anmerkung')], 1)
    sig = Table([[Paragraph('Fahrer / Unterschrift / Datum:<br/><br/><br/>______________________________', s['small']),
                  Paragraph('Empfangsbestätigung / Stempel:<br/><br/><br/>______________________________', s['small'])]], colWidths=[W / 2, W / 2])
    sig.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 0.5, _BORDER), ('INNERGRID', (0,0), (-1,-1), 0.5, _BORDER), ('TOPPADDING', (0,0), (-1,-1), 6), ('BOTTOMPADDING', (0,0), (-1,-1), 8), ('LEFTPADDING', (0,0), (-1,-1), 8)]))
    story += [Spacer(1, 6), sig, Spacer(1, 6), Paragraph(_foot(cfg, 'Erstellt'), s['ctr'])]
    return story

def _gen_orders_pdf(dispos, cfg):
    buf = io.BytesIO(); PW, PH = A4
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=12*mm, rightMargin=12*mm, topMargin=10*mm, bottomMargin=10*mm)
    W = PW - 24*mm; s = _styles(); story = []
    for i, d in enumerate(dispos):
        if i: story.append(PageBreak())
        story += _order_story(d, cfg, s, W)
    doc.build(story); return buf.getvalue()

def _pdf_resp(pdf, name):
    return send_file(io.BytesIO(pdf), mimetype='application/pdf', as_attachment=True, download_name='HG_' + name)

@app.route('/api/dispo/<did>/pdf')
@login_required
def dispo_pdf(did):
    d = next((x for x in load_db().get('dispo', []) if x['id'] == did), None)
    if not d: return jsonify({'error': 'not found'}), 404
    return _pdf_resp(_gen_orders_pdf([d], load_config()), f"Transportauftrag_{sanitize(d.get('auftragsnr') or d.get('nr', ''))}.pdf")

@app.route('/api/dispo/bulk-pdf', methods=['POST'])
@login_required
def dispo_bulk_pdf():
    ids = set((request.json or {}).get('ids', []))
    sel = sorted([d for d in load_db().get('dispo', []) if d['id'] in ids], key=_plan_sort)
    if not sel: return jsonify({'error': 'keine Auswahl'}), 400
    return _pdf_resp(_gen_orders_pdf(sel, load_config()), f"Transportauftraege_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf")

def _plan_build(data):
    """Tagesplan nach Datenlage bauen -> (pdf_bytes, dateiname, anzahl_auftraege)"""
    dispos = load_db().get('dispo', []); cfg = load_config()
    ids = data.get('ids') or []
    if ids:
        sel = [d for d in dispos if d['id'] in set(ids)]
        days = sorted({(d.get('lieferdatum') or '')[:10] for d in sel if d.get('lieferdatum')})
        sub = 'Auswahl' + (f' · {_fd(days[0])}' + (f' – {_fd(days[-1])}' if len(days) > 1 else '') if days else '')
        return _gen_tagesplan_pdf(sel, cfg, 'Tagesplan', sub, len(days) > 1), f"Tagesplan_Auswahl_{datetime.now().strftime('%Y%m%d')}.pdf", len(sel)
    fr = data.get('from') or data.get('date') or datetime.now().strftime('%Y-%m-%d')
    to = data.get('to') or data.get('date') or fr
    if to < fr: fr, to = to, fr
    veh = (data.get('fahrzeug') or '').strip()
    sel = [d for d in dispos if fr <= (d.get('lieferdatum') or '')[:10] <= to and d.get('lieferdatum')   # strikt nach Lieferdatum
           and (not veh or (d.get('fahrzeug_nr') or '') == veh)]
    vs = f' · Fahrzeug {veh}' if veh else ''; vf = f'_{sanitize(veh)}' if veh else ''
    if fr != to:
        return _gen_tagesplan_pdf(sel, cfg, 'Tagesplan Zeitraum', f'{_fd(fr)} – {_fd(to)}{vs}', True), f'Tagesplan_{fr}_bis_{to}{vf}.pdf', len(sel)
    try: sub = f"{_WD[datetime.strptime(fr, '%Y-%m-%d').weekday()]}, {_fd(fr)}"
    except: sub = _fd(fr)
    return _gen_tagesplan_pdf(sel, cfg, 'Tagesplan', sub + vs, False), f'Tagesplan_{fr}{vf}.pdf', len(sel)

@app.route('/api/dispo/tagesplan-pdf', methods=['POST'])
@login_required
def tagesplan_pdf():
    pdf, name, _n = _plan_build(request.json or {})
    return _pdf_resp(pdf, name)

# ── E-Mail-Versand aus der Dispo (Tagesplan / Aufträge) ───
import re as _re_mail
from email.message import EmailMessage as _EmailMessage
from email import policy as _mail_policy
from email.utils import formatdate as _fdate, make_msgid as _mkid, formataddr as _fmtaddr

def _mail_files(data):
    """Anhänge je nach Art bauen -> ([(dateiname, bytes)], anzahl_auftraege)"""
    cfg = load_config(); kind = data.get('kind') or 'plan'
    if kind == 'plan':
        pdf, name, n = _plan_build(data)
        if n == 0: raise ValueError('Für diese Auswahl gibt es keine Aufträge (Tagesplan richtet sich nach dem Lieferdatum).')
        return [('HG_' + name, pdf)], n
    ids = set(data.get('ids') or [])
    sel = sorted([d for d in load_db().get('dispo', []) if d['id'] in ids], key=_plan_sort)
    if not sel: raise ValueError('Bitte mindestens einen Auftrag auswählen.')
    if data.get('merge') and len(sel) > 1:
        return [(f"HG_Transportauftraege_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf", _gen_orders_pdf(sel, cfg))], len(sel)
    files, used = [], set()
    for d in sel:
        nm = f"HG_Transportauftrag_{sanitize(d.get('auftragsnr') or d.get('nr', ''))}.pdf"
        if nm in used: nm = nm[:-4] + '_' + sanitize(d.get('nr', '')) + '.pdf'
        used.add(nm); files.append((nm, _gen_orders_pdf([d], cfg)))
    return files, len(sel)

def _split_addrs(txt):
    out = []
    for a in _re_mail.split(r'[;,\s]+', txt or ''):
        a = a.strip().strip('<>')
        if a: out.append(a)
    return out

_MAIL_RE = _re_mail.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$')

def _mail_build(data, files, unsent=False):
    cfg = load_config()
    to = _split_addrs(data.get('recipients')); cc = _split_addrs(data.get('cc'))
    bad = [a for a in to + cc if not _MAIL_RE.match(a)]
    if bad: raise ValueError('Ungültige E-Mail-Adresse: ' + ', '.join(bad))
    if not to and not unsent: raise ValueError('Bitte mindestens einen Empfänger angeben.')
    fe = (cfg.get('from_email') or cfg.get('smtp_user') or '').strip()
    if _smtp_provider(cfg.get('smtp_host')) and '@' in (cfg.get('smtp_user') or ''):
        fe = cfg['smtp_user'].strip()      # Gmail/Microsoft erlauben nur die angemeldete Adresse als Absender
    fn = cfg.get('from_name') or cfg.get('company_name') or ''
    m = _EmailMessage()
    if fe: m['From'] = _fmtaddr((fn, fe))
    if to: m['To'] = ', '.join(to)
    if cc: m['Cc'] = ', '.join(cc)
    m['Subject'] = (data.get('subject') or '').strip() or 'Dispo'
    m['Date'] = _fdate(localtime=True)
    m['Message-ID'] = _mkid(domain=(fe.split('@')[-1] if '@' in fe else None))
    if unsent: m['X-Unsent'] = '1'          # Outlook öffnet die .eml als bearbeitbaren Entwurf
    m.set_content((data.get('body') or '').replace('\r\n', '\n'))
    for nm, pdf in files: m.add_attachment(pdf, maintype='application', subtype='pdf', filename=nm)
    return m, fe, to + cc

@app.route('/api/dispo/email/prepare', methods=['POST'])
@login_required
def dispo_mail_prepare():
    try:
        files, n = _mail_files(request.json or {})
        return jsonify({'ok': True, 'count': n, 'files': [{'name': nm, 'kb': round(len(b) / 1024)} for nm, b in files]})
    except Exception as e: return jsonify({'ok': False, 'message': str(e)}), 400

@app.route('/api/dispo/email/send', methods=['POST'])
@login_required
def dispo_mail_send():
    data = request.json or {}
    try:
        files, n = _mail_files(data)
        msg, fe, rcpts = _mail_build(data, files)
        cfg = load_config()
        if not fe: raise ValueError('Absender-E-Mail fehlt (⚙ Einstellungen → E-Mail / SMTP).')
        srv = _smtp_connect(cfg)
        try: srv.send_message(msg, from_addr=fe, to_addrs=rcpts)
        except smtplib.SMTPSenderRefused: raise ValueError('Der Server hat den Absender abgelehnt. Absender-E-Mail muss zur SMTP-Anmeldung passen.')
        except smtplib.SMTPRecipientsRefused: raise ValueError('Der Server hat den/die Empfänger abgelehnt.')
        finally:
            try: srv.quit()
            except Exception: pass
        db = load_db(); log = db.setdefault('dispo_mail_log', [])
        log.insert(0, {'at': datetime.now().isoformat(timespec='seconds'), 'by': current_user()['username'], 'to': ', '.join(_split_addrs(data.get('recipients'))),
                       'subject': msg['Subject'], 'files': [nm for nm, _ in files], 'kind': data.get('kind') or 'plan'})
        del log[50:]; save_db(db)
        return jsonify({'ok': True, 'files': [nm for nm, _ in files]})
    except Exception as e: return jsonify({'ok': False, 'message': str(e)}), 400

@app.route('/api/dispo/email/eml', methods=['POST'])
@login_required
def dispo_mail_eml():
    data = request.json or {}
    try:
        files, n = _mail_files(data); msg, _fe, _r = _mail_build(data, files, unsent=True)
        return send_file(io.BytesIO(msg.as_bytes(policy=_mail_policy.SMTP)), mimetype='message/rfc822', as_attachment=True,
                         download_name=f"HG_E-Mail_Entwurf_{datetime.now().strftime('%Y%m%d_%H%M')}.eml")
    except Exception as e: return jsonify({'ok': False, 'message': str(e)}), 400

@app.route('/api/dispo/email/attachments', methods=['POST'])
@login_required
def dispo_mail_attachments():
    """Anhänge zum Herunterladen (1 Datei = PDF, mehrere = ZIP) – z. B. für Gmail/Outlook im Browser"""
    try:
        files, n = _mail_files(request.json or {})
        if len(files) == 1:
            return send_file(io.BytesIO(files[0][1]), mimetype='application/pdf', as_attachment=True, download_name=files[0][0])
        z = io.BytesIO()
        with zipfile.ZipFile(z, 'w', zipfile.ZIP_DEFLATED) as zf:
            for nm, b in files: zf.writestr(nm, b)
        z.seek(0)
        return send_file(z, mimetype='application/zip', as_attachment=True, download_name=f"HG_Anhaenge_{datetime.now().strftime('%Y%m%d_%H%M')}.zip")
    except Exception as e: return jsonify({'ok': False, 'message': str(e)}), 400

@app.route('/api/dispo/email/log', methods=['GET'])
@login_required
def dispo_mail_log():
    return jsonify(load_db().get('dispo_mail_log', [])[:15])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=False)
