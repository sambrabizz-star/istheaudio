from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import re
import subprocess
import os
import sys
import psycopg2
import requests
import yt_dlp
import jwt
from jwt import PyJWKClient
from datetime import datetime, timezone

app = Flask(__name__)
app.logger.setLevel("INFO")

# -----------------------------
# CORS
# -----------------------------
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response

# -----------------------------
# CONFIG (Fly.io secrets)
# -----------------------------
DB_URL = os.environ.get("SUPABASE_DB_URL")
JWKS_URL = os.environ.get("SUPABASE_JWKS_URL")
JWT_ISSUER = os.environ.get("SUPABASE_JWT_ISSUER")
JWT_AUDIENCE = os.environ.get("SUPABASE_JWT_AUDIENCE", "authenticated")
QUOTA_PER_HOUR = 30

# -----------------------------
# GLOBALS (lazy init)
# -----------------------------
db = None
jwk_client = None

# -----------------------------
# INIT HELPERS
# -----------------------------
def get_db():
    global db
    if db is None or db.closed != 0:
        app.logger.info("🔌 Connecting to Supabase DB")
        db = psycopg2.connect(DB_URL)
        db.autocommit = True
    return db

def get_jwk_client():
    global jwk_client
    if jwk_client is None:
        app.logger.info("🔑 Initializing PyJWKClient")
        jwk_client = PyJWKClient(JWKS_URL)
    return jwk_client

# -----------------------------
# AUTH
# -----------------------------
def verify_jwt_and_get_user():
    auth = request.headers.get("Authorization", "")
    app.logger.info(f"🔐 Authorization header present={bool(auth)}")

    if not auth.startswith("Bearer "):
        app.logger.warning("❌ Missing Bearer token")
        return None

    token = auth.split(" ", 1)[1]

    try:
        signing_key = get_jwk_client().get_signing_key_from_jwt(token)

        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],   # Supabase utilise ES256
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )

        user_id = payload.get("sub")
        app.logger.info(f"✅ JWT verified user_id={user_id}")
        return user_id

    except Exception as e:
        app.logger.error(f"❌ JWT verification failed: {e}")
        return None

# -----------------------------
# QUOTA
# -----------------------------
def increment_usage(user_id):
    app.logger.info(f"📊 Increment usage for user={user_id}")
    with get_db().cursor() as cur:
        cur.execute("""
            insert into api_usage (user_id, hour_bucket, count)
            values (%s, date_trunc('hour', now()), 1)
            on conflict (user_id, hour_bucket)
            do update set count = api_usage.count + 1
            returning count;
        """, (user_id,))
        count = cur.fetchone()[0]
        app.logger.info(f"📈 Current usage count={count}")
        return count

# -----------------------------
# UTILS
# -----------------------------
def is_valid_tiktok_url(url: str) -> bool:
    return bool(re.search(r"(vm\.tiktok\.com|tiktok\.com)", url))

def extract_info_and_filesize(url: str):
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "nocheckcertificate": True,
        "user_agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
            "Mobile/15E148 Safari/604.1"
        ),
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    filesize = info.get("filesize") or info.get("filesize_approx")
    return info, filesize

# -----------------------------
# STREAM ENDPOINT
# -----------------------------
@app.route("/tiktok/stream", methods=["POST", "OPTIONS"])
def tiktok_stream():
    app.logger.info("➡️ /tiktok/stream called")

    if request.method == "OPTIONS":
        return "", 200

    user_id = verify_jwt_and_get_user()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    count = increment_usage(user_id)
    if count > QUOTA_PER_HOUR:
        reset_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        app.logger.warning("⛔ Quota exceeded")
        return jsonify({
            "error": "Quota exceeded",
            "limit": QUOTA_PER_HOUR,
            "reset_at": reset_at.isoformat()
        }), 429

    data = request.get_json(silent=True)
    app.logger.info(f"📦 Payload={data}")

    if not data or "url" not in data:
        return jsonify({"error": "Missing url"}), 400

    url = data["url"]
    if not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid TikTok URL"}), 400

    # Récupération taille pour progression UI
    try:
        info, filesize = extract_info_and_filesize(url)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not filesize:
        return jsonify({"error": "Unable to determine file size"}), 500

    def generate():
        app.logger.info("🎬 Starting yt-dlp streaming")
        cmd = [
            sys.executable,
            "-m", "yt_dlp",
            "-f", "bv*[ext=mp4][watermark!=true]/b[ext=mp4]",
            "-o", "-",
            "--merge-output-format", "mp4",
            "--no-part",
            "--quiet",
            url,
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1024 * 1024,
        )

        try:
            received_bytes = 0
            while True:
                chunk = process.stdout.read(8192)
                if not chunk:
                    break
                received_bytes += len(chunk)
                yield chunk
        finally:
            stderr = process.stderr.read().decode()
            process.stdout.close()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                app.logger.error(f"❌ yt-dlp error: {stderr}")
            else:
                app.logger.info("✅ Stream finished")

    return Response(
        stream_with_context(generate()),
        content_type="video/mp4",
        headers={
            "Content-Disposition": "attachment; filename=tiktok.mp4",
            "Content-Length": str(filesize),  # ← indispensable pour Flutter progression
            "Cache-Control": "no-store",
            "Accept-Ranges": "none",
        },
    )

# -----------------------------
# HEALTH
# -----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)














