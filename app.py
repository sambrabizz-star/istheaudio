from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import subprocess
import tempfile
import os
import sys
import re
import shutil
import psycopg2
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
            algorithms=["ES256"],   # Supabase utilise ES256 pour EC keys
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

# -----------------------------
# AUDIO STREAM ENDPOINT → MP3
# -----------------------------
@app.route("/tiktok/mp3", methods=["POST", "OPTIONS"])
def tiktok_mp3():
    app.logger.info("➡️ /tiktok/mp3 called")

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

    url = data["url"].strip()
    if not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid TikTok URL"}), 400

    temp_dir = tempfile.mkdtemp(prefix="tiktok_mp3_")
    video_path = os.path.join(temp_dir, "video.mp4")
    audio_path = os.path.join(temp_dir, "audio.mp3")

    try:
        # 1️⃣ Télécharger la vidéo
        subprocess.run(
            [
                sys.executable, "-m", "yt_dlp",
                "-f", "bv*+ba/b",
                "--merge-output-format", "mp4",
                "--no-part",
                "--no-playlist",
                "--quiet",
                "-o", video_path,
                url,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )

        if not os.path.exists(video_path) or os.path.getsize(video_path) < 1024:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({"error": "Downloaded video is empty"}), 500

        # 2️⃣ Extraire en MP3
        subprocess.run(
            [
                "ffmpeg", "-y", "-i", video_path,
                "-vn", "-acodec", "libmp3lame", "-ab", "192k",
                audio_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )

        if not os.path.exists(audio_path) or os.path.getsize(audio_path) < 1024:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({"error": "MP3 extraction failed"}), 409

        mp3_size = os.path.getsize(audio_path)

        def generate():
            try:
                with open(audio_path, "rb") as f:
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        yield chunk
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        return Response(
            stream_with_context(generate()),
            content_type="audio/mpeg",
            headers={
                "Content-Disposition": "attachment; filename=tiktok_audio.mp3",
                "Content-Length": str(mp3_size),
                "Cache-Control": "no-store",
                "Accept-Ranges": "none",
            },
        )

    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return jsonify({
            "error": "Video download or MP3 encoding failed",
            "details": e.stderr.decode(errors="ignore"),
        }), 500

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
















