"""
LAGLOG Cloud — Lagonis Logistics Web App
Multi-user, session-based login, Render.com ready
"""
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
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH   = os.path.join(BASE_DIR, 'static', 'img', 'logo.jpg')

# Railway: use mounted volume at /data, fallback to ./data locally
DATA_DIR = os.environ.get('DATA_DIR', '/data')
if not os.path.exists(DATA_DIR):
    DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE   = os.path.join(DATA_DIR, 'db.json')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.json')
USERS_FILE  = os.path.join(DATA_DIR, 'users.json')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.permanent_session_lifetime = timedelta(days=7)

# ── Auth helpers ─────────────────────────────────────────
def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

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
    empty = {'orders':[], 'invoices':[], 'customers':[], 'order_counter':1}
    if not os.path.exists(DATA_FILE):
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(empty, f)
        return empty
    try:
        with open(DATA_FILE, encoding='utf-8') as f:
            db = json.load(f)
        for k,v in [('customers',[]),('order_counter',len(db.get('orders',[]))+1)]:
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
        'reminder_days':14,'dunning_days':30,
        'default_payment_days':14,'default_payment_text':'Zahlbar innerhalb 14 Tagen netto',
        'default_author':'','default_vat':19,
        'order_prefix':'LL-TA','invoice_prefix':'RE',
        'show_price_default':False,'auto_invoice_from_order':True,
        'order_counter_override':0,'invoice_counter_override':0,
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
    data = request.json
    users = load_users()
    user = next((u for u in users if u['username'] == data.get('username')
                 and u['password'] == hash_pw(data.get('password',''))), None)
    if not user:
        return jsonify({'ok': False, 'message': 'Benutzername oder Passwort falsch'}), 401
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
            if user['password'] != hash_pw(data.get('old','')):
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
    try: _smtp_connect(load_config()); return jsonify({'ok':True,'message':'Verbindung erfolgreich!'})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 400

# ── Customers ─────────────────────────────────────────────
@app.route('/api/customers', methods=['GET'])
@login_required
def get_customers(): return jsonify(load_db()['customers'])

@app.route('/api/customers', methods=['POST'])
@login_required
def create_customer():
    db = load_db(); c = request.json
    c['id'] = str(uuid.uuid4()); c['createdAt'] = datetime.now().isoformat()
    c['createdBy'] = current_user()['username']
    db['customers'].append(c); save_db(db); return jsonify(c), 201

@app.route('/api/customers/<cid>', methods=['PUT'])
@login_required
def update_customer(cid):
    db = load_db()
    for i,c in enumerate(db['customers']):
        if c['id'] == cid:
            upd = request.json; upd['id'] = cid; upd['createdAt'] = c.get('createdAt','')
            upd['createdBy'] = c.get('createdBy','')
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

@app.route('/api/invoices/set-counter', methods=['POST'])
@login_required
@admin_required
def set_invoice_counter():
    db = load_db()
    val = int(request.json.get('counter', 1))
    db['invoice_counter'] = val
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
    return send_file(io.BytesIO(generate_order_pdf(o,cfg,lang)), mimetype='application/pdf',
                     as_attachment=True, download_name=f"Frachtbrief_{sanitize(o.get('nr',''))}.pdf")

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
            if o: zf.writestr(f"Frachtbrief_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang))
    buf.seek(0)
    return send_file(buf, mimetype='application/zip', as_attachment=True, download_name='Frachtbriefe.zip')

@app.route('/api/orders/<oid>/send-email', methods=['POST'])
@login_required
def send_order_email(oid):
    data = request.json; db = load_db(); cfg = load_config(); lang = data.get('lang','de')
    o = next((x for x in db['orders'] if x['id']==oid), None)
    if not o: return jsonify({'error':'not found'}), 404
    try:
        atts = [(f"Frachtbrief_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang))]
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
        if o: atts.append((f"Frachtbrief_{sanitize(o.get('nr',''))}.pdf", generate_order_pdf(o,cfg,lang)))
    try: _send_email(cfg, data['to'], data['subject'], data['body'], atts); return jsonify({'ok':True})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 500

# ── Invoices ──────────────────────────────────────────────
@app.route('/api/invoices', methods=['GET'])
@login_required
def get_invoices(): return jsonify(load_db()['invoices'])

@app.route('/api/invoices', methods=['POST'])
@login_required
def create_invoice():
    db = load_db(); inv = request.json
    inv['id'] = str(uuid.uuid4()); inv['createdAt'] = datetime.now().isoformat()
    inv['createdBy'] = current_user()['username']
    db['invoices'].append(inv); save_db(db); return jsonify(inv), 201

@app.route('/api/invoices/<iid>', methods=['PUT'])
@login_required
def update_invoice(iid):
    db = load_db()
    for i,inv in enumerate(db['invoices']):
        if inv['id'] == iid:
            upd = request.json; upd['id'] = iid; upd['createdAt'] = inv.get('createdAt','')
            upd['createdBy'] = inv.get('createdBy',''); upd['updatedBy'] = current_user()['username']
            db['invoices'][i] = upd; save_db(db); return jsonify(upd)
    return jsonify({'error':'not found'}), 404

@app.route('/api/invoices/<iid>/paid', methods=['POST'])
@login_required
def mark_paid(iid):
    db = load_db()
    for inv in db['invoices']:
        if inv['id']==iid:
            inv['status']='paid'; inv['paidAt']=datetime.now().isoformat()
            inv['paidBy']=current_user()['username']
            save_db(db); return jsonify(inv)
    return jsonify({'error':'not found'}), 404

@app.route('/api/invoices/<iid>/unpaid', methods=['POST'])
@login_required
def mark_unpaid(iid):
    db = load_db()
    for inv in db['invoices']:
        if inv['id']==iid:
            inv['status']='unpaid'; inv.pop('paidAt',None); inv.pop('paidBy',None)
            save_db(db); return jsonify(inv)
    return jsonify({'error':'not found'}), 404

@app.route('/api/invoices/<iid>', methods=['DELETE'])
@login_required
def delete_invoice(iid):
    db = load_db(); db['invoices'] = [i for i in db['invoices'] if i['id']!=iid]
    save_db(db); return jsonify({'ok':True})

@app.route('/api/invoices/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_invoices():
    ids = set(request.json.get('ids',[])); db = load_db()
    db['invoices'] = [i for i in db['invoices'] if i['id'] not in ids]
    save_db(db); return jsonify({'ok':True})

@app.route('/api/invoices/bulk-paid', methods=['POST'])
@login_required
def bulk_paid_invoices():
    ids = set(request.json.get('ids',[])); db = load_db(); u = current_user()
    for inv in db['invoices']:
        if inv['id'] in ids:
            inv['status']='paid'; inv['paidAt']=datetime.now().isoformat(); inv['paidBy']=u['username']
    save_db(db); return jsonify({'ok':True})

@app.route('/api/invoices/bulk-duplicate', methods=['POST'])
@login_required
def bulk_duplicate_invoices():
    ids = request.json.get('ids',[]); db = load_db(); u = current_user()
    im = {i['id']:i for i in db['invoices']}; new_invs = []
    for iid in ids:
        inv = im.get(iid)
        if inv:
            n = copy.deepcopy(inv); n['id'] = str(uuid.uuid4())
            n['createdAt'] = datetime.now().isoformat(); n['status'] = 'unpaid'
            n['createdBy'] = u['username']; n.pop('paidAt',None); n.pop('paidBy',None)
            year = datetime.now().year
            cnt = len([x for x in db['invoices'] if x.get('nr','').startswith(f'RE-{year}')]) + len(new_invs) + 1
            n['nr'] = f"RE-{year}-{str(cnt).zfill(3)}"
            new_invs.append(n)
    db['invoices'].extend(new_invs); save_db(db)
    return jsonify({'ok':True,'count':len(new_invs)})

@app.route('/api/invoices/<iid>/pdf')
@login_required
def invoice_pdf(iid):
    db = load_db(); cfg = load_config(); lang = request.args.get('lang','de')
    inv = next((i for i in db['invoices'] if i['id']==iid), None)
    if not inv: return jsonify({'error':'not found'}), 404
    order = next((o for o in db['orders'] if o['id']==inv.get('orderId')), None)
    return send_file(io.BytesIO(generate_invoice_pdf(inv,order,cfg,lang)), mimetype='application/pdf',
                     as_attachment=True, download_name=f"Rechnung_{sanitize(inv.get('nr',''))}.pdf")

@app.route('/api/invoices/<iid>/reminder-pdf')
@login_required
def reminder_pdf(iid):
    db = load_db(); cfg = load_config()
    lang = request.args.get('lang','de'); doc_type = request.args.get('type','reminder')
    inv = next((i for i in db['invoices'] if i['id']==iid), None)
    if not inv: return jsonify({'error':'not found'}), 404
    pdf = generate_reminder_pdf(inv, cfg, doc_type, lang)
    prefix = {'de':{'reminder':'Zahlungserinnerung','dunning':'Mahnung'},
               'en':{'reminder':'Payment_Reminder','dunning':'Dunning_Notice'}}[lang][doc_type]
    return send_file(io.BytesIO(pdf), mimetype='application/pdf',
                     as_attachment=True, download_name=f"{prefix}_{sanitize(inv.get('nr',''))}.pdf")

@app.route('/api/invoices/bulk-pdf', methods=['POST'])
@login_required
def invoices_bulk_pdf():
    ids = request.json.get('ids',[]); lang = request.json.get('lang','de')
    db = load_db(); cfg = load_config()
    im = {i['id']:i for i in db['invoices']}; om = {o['id']:o for o in db['orders']}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as zf:
        for iid in ids:
            inv = im.get(iid)
            if inv:
                order = om.get(inv.get('orderId'))
                zf.writestr(f"Rechnung_{sanitize(inv.get('nr',''))}.pdf", generate_invoice_pdf(inv,order,cfg,lang))
    buf.seek(0)
    return send_file(buf, mimetype='application/zip', as_attachment=True, download_name='Rechnungen.zip')

@app.route('/api/invoices/<iid>/send-email', methods=['POST'])
@login_required
def send_invoice_email(iid):
    data = request.json; db = load_db(); cfg = load_config(); lang = data.get('lang','de')
    inv = next((i for i in db['invoices'] if i['id']==iid), None)
    if not inv: return jsonify({'error':'not found'}), 404
    order = next((o for o in db['orders'] if o['id']==inv.get('orderId')), None)
    try:
        _send_email(cfg, data['to'], data['subject'], data['body'],
                    [(f"Rechnung_{sanitize(inv.get('nr',''))}.pdf", generate_invoice_pdf(inv,order,cfg,lang))])
        return jsonify({'ok':True})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 500

@app.route('/api/invoices/bulk-email', methods=['POST'])
@login_required
def invoices_bulk_email():
    data = request.json; db = load_db(); cfg = load_config(); lang = data.get('lang','de')
    im = {i['id']:i for i in db['invoices']}; om = {o['id']:o for o in db['orders']}; atts = []
    for iid in data.get('ids',[]):
        inv = im.get(iid)
        if inv:
            order = om.get(inv.get('orderId'))
            atts.append((f"Rechnung_{sanitize(inv.get('nr',''))}.pdf", generate_invoice_pdf(inv,order,cfg,lang)))
    try: _send_email(cfg, data['to'], data['subject'], data['body'], atts); return jsonify({'ok':True})
    except Exception as e: return jsonify({'ok':False,'message':str(e)}), 500

@app.route('/api/stats', methods=['GET'])
@login_required
def get_stats():
    db = load_db()
    return jsonify([{'date':i.get('date',''),'net':float(i.get('net',0)),'total':float(i.get('total',0)),
                     'status':i.get('status','unpaid'),'orderNr':i.get('orderNr',''),
                     'customerName':i.get('customerName','')} for i in db['invoices']])

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
                     as_attachment=True, download_name=f'laglog_backup_{ts}.json')

@app.route('/api/restore', methods=['POST'])
@login_required
@admin_required
def restore():
    """Restore database from JSON backup"""
    try:
        data = request.json
        if 'orders' not in data or 'invoices' not in data:
            return jsonify({'ok': False, 'message': 'Ungültiges Backup-Format'}), 400
        save_db(data)
        return jsonify({'ok': True, 'orders': len(data.get('orders',[])),
                        'invoices': len(data.get('invoices',[])),
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
        'invoices': len(db.get('invoices', [])),
        'customers': len(db.get('customers', []))
    })

@app.route('/api/keepalive')
def keepalive():
    """Lightweight endpoint for keep-alive pings"""
    return jsonify({'ok': True, 'ts': datetime.now().isoformat()})

# ── Email ─────────────────────────────────────────────────
def _smtp_connect(cfg):
    host = cfg.get('smtp_host','')
    if not host: raise ValueError('SMTP-Host nicht konfiguriert. Bitte Einstellungen prüfen.')
    port = int(cfg.get('smtp_port', 587))
    use_ssl = cfg.get('smtp_ssl', False)
    try:
        if use_ssl or port == 465:
            srv = smtplib.SMTP_SSL(host, port, timeout=15)
            srv.ehlo()
        else:
            srv = smtplib.SMTP(host, port, timeout=15)
            srv.ehlo()
            try:
                srv.starttls()
                srv.ehlo()
            except Exception:
                pass  # Some servers don't support STARTTLS
        u = cfg.get('smtp_user','')
        p = cfg.get('smtp_pass','')
        if u and p:
            srv.login(u, p)
        return srv
    except smtplib.SMTPAuthenticationError:
        raise ValueError('Authentifizierung fehlgeschlagen. Benutzername/Passwort prüfen.')
    except smtplib.SMTPConnectError:
        raise ValueError(f'Verbindung zu {host}:{port} fehlgeschlagen.')
    except Exception as e:
        raise ValueError(f'SMTP-Fehler: {str(e)}')

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
DARK=colors.HexColor('#1a1a1a'); GRAY=colors.HexColor('#666666')
LGRAY=colors.HexColor('#f5f5f5'); BORDER=colors.HexColor('#cccccc')

LABELS = {
    'de': {
        'waybill':'Hausfrachtbrief','vehicle':'Fahrzeug Nr.','chassis':'Chassis',
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
        'waybill':'Bill of Lading','vehicle':'Vehicle No.','chassis':'Chassis',
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
        'h1':    ParagraphStyle('h1',    fontName='Helvetica-Bold',fontSize=18,textColor=DARK,spaceAfter=2),
        'h3':    ParagraphStyle('h3',    fontName='Helvetica-Bold',fontSize=9, textColor=GRAY,spaceAfter=2),
        'normal':ParagraphStyle('normal',fontName='Helvetica',     fontSize=9, textColor=DARK,leading=13),
        'small': ParagraphStyle('small', fontName='Helvetica',     fontSize=8, textColor=GRAY,leading=11),
        'right': ParagraphStyle('right', fontName='Helvetica',     fontSize=9, textColor=DARK,alignment=TA_RIGHT),
        'bold':  ParagraphStyle('bold',  fontName='Helvetica-Bold',fontSize=9, textColor=DARK),
        'bold_r':ParagraphStyle('bold_r',fontName='Helvetica-Bold',fontSize=10,textColor=DARK,alignment=TA_RIGHT),
        'center':ParagraphStyle('center',fontName='Helvetica',     fontSize=8, textColor=GRAY,alignment=TA_CENTER),
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
    cell_l = Image(LOGO_PATH,width=35*mm,height=25*mm,kind='proportional') if os.path.exists(LOGO_PATH) else Paragraph('<b>LAGLOG</b>',s['h1'])
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
    story+=[Paragraph(L['waybill'],s['h3']),Paragraph(order.get('nr',''),s['h1']),Spacer(1,4),
            Paragraph(f"{L['created_by']}: {order.get('author','')}   |   {L['date']}: {fmt_dt(order.get('createdAt',''),lang)}",s['small']),
            Spacer(1,8)]
    vt=Table([[Paragraph(L['vehicle'],s['label']),Paragraph(L['chassis'],s['label'])],
              [Paragraph(order.get('vehicle','-'),s['value']),Paragraph(order.get('chassis','-'),s['value'])]],
             colWidths=[W/2,W/2])
    vt.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),6)]))
    story+=[vt,Spacer(1,8)]
    def sr(lbl,v,b,pl,name,addr,zip_,city_,rd,hf,sc,ref,zoll):
        return [Paragraph(lbl,s['bold']),
                Paragraph(f"{L['from']}: {fmt_dt(v,lang)}\n{L['to_']}: {fmt_dt(b,lang)}\n{L['plan']}: {fmt_dt(pl,lang)}",s['small']),
                Paragraph(addr_block(name,addr,zip_,city_) or '-',s['small']),
                Paragraph(f"{L['carrier']}: {rd or '-'}\n{L['port']}: {hf or '-'}\n{L['vessel']}: {sc or '-'}",s['small']),
                Paragraph(f"{L['ref2']}: {ref or '-'}\n{L['customs']}: {zoll or '-'}",s['small'])]
    sd=[[Paragraph(h,s['label']) for h in [L['station'],L['times'],L['address'],L['shipping'],L['ref']]],
        sr(f"1·{L['pickup']}",order.get('s1von'),order.get('s1bis'),order.get('s1plan'),order.get('s1name'),order.get('s1addr'),order.get('s1zip'),order.get('s1city'),order.get('s1reederei'),order.get('s1hafen'),order.get('s1schiff'),order.get('s1ref'),order.get('s1zoll')),
        sr(f"2·{L['recipient']}",order.get('s2von'),order.get('s2bis'),order.get('s2plan'),order.get('s2name'),order.get('s2addr'),order.get('s2zip'),order.get('s2city'),None,order.get('s2hafen'),order.get('s2schiff'),order.get('s2ref'),None),
        sr(f"3·{L['depot']}",None,None,order.get('s3plan'),order.get('s3name'),order.get('s3addr'),order.get('s3zip'),order.get('s3city'),order.get('s3reederei'),None,None,order.get('s3ref'),order.get('s3zoll'))]
    st=Table(sd,colWidths=[24*mm,33*mm,W-142*mm,45*mm,40*mm])
    st.setStyle(TableStyle([('BOX',(0,0),(-1,-1),0.5,BORDER),('INNERGRID',(0,0),(-1,-1),0.5,BORDER),
        ('BACKGROUND',(0,0),(-1,0),LGRAY),('VALIGN',(0,0),(-1,-1),'TOP'),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),5)]))
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
    story+=[ht,Spacer(1,8)]
    cname=inv.get('customerName','')
    if cname:
        aparts=list(filter(None,[cname,inv.get('customerAddress',''),
                                  ' '.join(filter(None,[inv.get('customerZip',''),inv.get('customerCity','')] ))]))
        story+=[Paragraph('<br/>'.join(aparts),s['normal']),Spacer(1,8)]
    story.append(HRFlowable(width='100%',thickness=0.5,color=BORDER,spaceAfter=8))
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=False)
