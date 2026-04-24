#!/bin/bash
# LAGLOG Setup Script für IONOS VPS (Ubuntu 22.04)
# Einfach Schritt für Schritt ausführen

echo "=== LAGLOG IONOS Setup ==="

# 1. System aktualisieren
apt-get update -y && apt-get upgrade -y

# 2. Python und pip installieren
apt-get install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx

# 3. App-Verzeichnis erstellen
mkdir -p /opt/laglog/data
cd /opt/laglog

# 4. Python-Umgebung erstellen
python3 -m venv venv
source venv/bin/activate

# 5. Pakete installieren
pip install flask reportlab pillow gunicorn

# 6. Systemd-Service erstellen (App läuft dauerhaft)
cat > /etc/systemd/system/laglog.service << 'SYSTEMD'
[Unit]
Description=LAGLOG Logistics App
After=network.target

[Service]
User=root
WorkingDirectory=/opt/laglog
Environment="PATH=/opt/laglog/venv/bin"
Environment="DATA_DIR=/opt/laglog/data"
Environment="SECRET_KEY=BITTE_AENDERN_laglog2024geheim"
ExecStart=/opt/laglog/venv/bin/gunicorn app:app --bind 0.0.0.0:5000 --workers 2 --timeout 120
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SYSTEMD

# 7. Nginx als Reverse-Proxy konfigurieren
cat > /etc/nginx/sites-available/laglog << 'NGINX'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120;
        client_max_body_size 50M;
    }
}
NGINX

ln -sf /etc/nginx/sites-available/laglog /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

echo ""
echo "=== Setup abgeschlossen! ==="
echo "Jetzt app.py, templates/ und static/ nach /opt/laglog/ hochladen"
echo "Dann: systemctl enable laglog && systemctl start laglog"
