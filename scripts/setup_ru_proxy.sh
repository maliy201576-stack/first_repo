#!/usr/bin/env bash
# Deploy a Squid forward-proxy on the German server (171.22.120.138) that
# chains through a Russian upstream proxy so that worker_web can reach
# zakupki.gov.ru and profi.ru with a Russian IP.
#
# Architecture:
#   worker_web → Squid (:3129) → Russian upstream proxy → profi.ru / zakupki.gov.ru
#
# The Russian upstream can be:
#   - A SOCKS5/HTTP proxy on a Russian VPS
#   - A WireGuard/SSH tunnel endpoint
#   - A commercial Russian proxy service
#
# Usage (run on the German server 171.22.120.138):
#   export RU_UPSTREAM_PROXY="http://ru-vps-ip:port"   # required
#   export RU_PROXY_PASSWORD="your-password"            # optional, auto-generated if empty
#   chmod +x setup_ru_proxy.sh
#   sudo -E ./setup_ru_proxy.sh
#
# After running, the proxy will be available at:
#   http://glukhov:<PASSWORD>@171.22.120.138:3129

set -euo pipefail

PROXY_PORT=3129
PROXY_USER="glukhov"
PROXY_PASS="${RU_PROXY_PASSWORD:-$(openssl rand -base64 18)}"
RU_UPSTREAM="${RU_UPSTREAM_PROXY:-}"

if [ -z "$RU_UPSTREAM" ]; then
    echo "============================================"
    echo "  WARNING: RU_UPSTREAM_PROXY is not set!"
    echo "============================================"
    echo "  Without a Russian upstream proxy, Squid will"
    echo "  exit to the internet with a German IP."
    echo "  profi.ru and zakupki.gov.ru may block or"
    echo "  throttle requests from non-Russian IPs."
    echo ""
    echo "  To fix, set RU_UPSTREAM_PROXY before running:"
    echo "    export RU_UPSTREAM_PROXY=http://ru-vps:port"
    echo ""
    echo "  Continuing without upstream (direct access)..."
    echo "============================================"
fi

echo "=== Installing Squid ==="
apt-get update -qq
apt-get install -y squid apache2-utils

# --- Detect basic_ncsa_auth path (varies across Ubuntu versions) ---
NCSA_AUTH=""
for candidate in \
    /usr/lib/squid/basic_ncsa_auth \
    /usr/lib/squid3/basic_ncsa_auth \
    /usr/lib64/squid/basic_ncsa_auth; do
    if [ -x "$candidate" ]; then
        NCSA_AUTH="$candidate"
        break
    fi
done

if [ -z "$NCSA_AUTH" ]; then
    echo "ERROR: basic_ncsa_auth not found. Is squid installed correctly?"
    echo "Searched: /usr/lib/squid/, /usr/lib/squid3/, /usr/lib64/squid/"
    exit 1
fi
echo "Found ncsa_auth at: $NCSA_AUTH"

echo "=== Creating password file ==="
htpasswd -cb /etc/squid/proxy_users "$PROXY_USER" "$PROXY_PASS"
chmod 640 /etc/squid/proxy_users
chown proxy:proxy /etc/squid/proxy_users

# --- Build upstream (cache_peer) block if Russian proxy is provided ---
UPSTREAM_BLOCK=""
UPSTREAM_ACCESS=""
if [ -n "$RU_UPSTREAM" ]; then
    # Parse host:port from URL like http://host:port
    RU_HOST=$(echo "$RU_UPSTREAM" | sed -E 's|https?://||; s|/.*||; s|:.*||')
    RU_PORT=$(echo "$RU_UPSTREAM" | sed -E 's|https?://||; s|/.*||; s|.*:||')
    RU_PORT="${RU_PORT:-3128}"

    UPSTREAM_BLOCK="
# Chain through Russian upstream proxy
cache_peer ${RU_HOST} parent ${RU_PORT} 0 no-query default
never_direct allow ru_sites"
    UPSTREAM_ACCESS=""
    echo "Upstream proxy: ${RU_HOST}:${RU_PORT}"
else
    echo "No upstream proxy — direct access to Russian sites"
fi

echo "=== Writing Squid config ==="
cat > /etc/squid/squid.conf << SQUID_CONF
# Forward proxy for Glukhov Sales Engine (German server)
# Chains through Russian upstream for .profi.ru and .zakupki.gov.ru

http_port ${PROXY_PORT}

# Authentication
auth_param basic program ${NCSA_AUTH} /etc/squid/proxy_users
auth_param basic realm GlukhovProxy
auth_param basic credentialsttl 24 hours
auth_param basic children 5 startup=2 idle=1

acl authenticated proxy_auth REQUIRED

# Restrict to target Russian domains only
acl ru_sites dstdomain .zakupki.gov.ru .profi.ru

# Ports
acl SSL_ports port 443
acl Safe_ports port 80
acl Safe_ports port 443
acl CONNECT method CONNECT
${UPSTREAM_BLOCK}

# Access rules — only authenticated AND targeting allowed domains
http_access deny !Safe_ports
http_access deny CONNECT !SSL_ports
http_access allow authenticated ru_sites
http_access deny all

# Performance
cache deny all

# Privacy — don't leak proxy headers
via off
forwarded_for delete
request_header_access X-Forwarded-For deny all

# Timeouts (generous — Russian gov sites can be slow)
connect_timeout 60 seconds
read_timeout 120 seconds
request_timeout 90 seconds

# Logging (minimal)
access_log daemon:/var/log/squid/access.log squid
cache_log /var/log/squid/cache.log
SQUID_CONF

echo "=== Validating config ==="
squid -k parse 2>&1 || { echo "ERROR: Squid config validation failed"; exit 1; }

echo "=== Opening firewall port ==="
if command -v ufw &> /dev/null; then
    ufw allow "$PROXY_PORT"/tcp
elif command -v firewall-cmd &> /dev/null; then
    firewall-cmd --permanent --add-port="$PROXY_PORT"/tcp
    firewall-cmd --reload
fi

echo "=== Restarting Squid ==="
systemctl enable squid
systemctl restart squid

# Verify Squid is running
sleep 2
if systemctl is_active --quiet squid; then
    echo ""
    echo "============================================"
    echo "  Proxy deployed successfully!"
    echo "============================================"
    echo "  Host:     171.22.120.138"
    echo "  Port:     ${PROXY_PORT}"
    echo "  Username: ${PROXY_USER}"
    echo "  Password: ${PROXY_PASS}"
    if [ -n "$RU_UPSTREAM" ]; then
    echo "  Upstream: ${RU_UPSTREAM} (Russian IP)"
    else
    echo "  Upstream: NONE (direct, German IP)"
    fi
    echo ""
    echo "  Add to your .env:"
    echo "  SCRAPER_DIRECT_PROXY_URL=http://171.22.120.138:${PROXY_PORT}"
    echo "  SCRAPER_DIRECT_PROXY_USER=${PROXY_USER}"
    echo "  SCRAPER_DIRECT_PROXY_PASS=${PROXY_PASS}"
    echo "============================================"
else
    echo "ERROR: Squid failed to start. Check: journalctl -u squid"
    exit 1
fi
