from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import subprocess
import tempfile
import os
import sys
import re
import shutil
from psycopg2 import pool
import jwt
from jwt import PyJWKClient
from datetime import datetime, timezone

# --------------------------------------------------
# APP
# --------------------------------------------------
app = Flask(__name__)
app.logger.setLevel("INFO")

CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response

# --------------------------------------------------
# CONFIG (Fly.io secrets)
# --------------------------------------------------
DB_URL = os.environ["SUPABASE_DB_URL"]
JWKS_URL = os.environ["SUPABASE_JWKS_URL"]
JWT_ISSUER = os.environ["SUPABASE_JWT_ISSUER"]
JWT_AUDIENCE = os.environ.get("SUPABASE_JWT_AUDIENCE", "authenticated")

QUOTA_PER_HOUR = 30

# --------------------------------------------------
# GLOBALS (lazy init)
# --------------------------------------------------
db_pool: pool.SimpleConnectionPool | None = None
jwk_client: PyJWKClient | None = None

# --------------------------------------------------
# DB POOL
# --------------------------------------------------
def init_db_pool():
    global db_pool
    if db_pool is None:
        app.logger.info("🔌 Initializing PostgreSQL connection pool")
        db_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=20,  # SAFE: Supabase Free (60) / 2 Fly machines / streaming
            dsn=DB_URL,
        )
    return db_pool

def get_conn():
    return init_db_pool().getconn()

def release_conn(conn):
    init_db_pool().putconn(conn)

# --------------------------------------------------
# JWKS
# --------------------------------------------------
def get_jwk_client():
    global jwk_client
    if jwk_client is None:
        app.logger.info("🔑 Initializing PyJWKClient")
        jwk_client = PyJWKClient(JWKS_URL)
    return jwk_client

# --------------------------------------------------
# AUTH
# --------------------------------------------------
def verify_jwt_and_get_user():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None

    token = auth.split(" ", 1)[1]

    try:
        signing_key = get_jwk_client().get_signing_key_from_jwt(token)

        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )

        user_id = payload["sub"]
        app.logger.info(f"✅ JWT verified user_id={user_id}")
        return user_id

    except Exception as e:
        app.logger.error(f"❌ JWT verification failed: {e}")
        return None

# --------------------------------------------------
# QUOTA
# --------------------------------------------------
def increment_usage(user_id: str) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO api_usage (user_id, hour_bucket, count)
                VALUES (%s, date_trunc('hour', now()), 1)
                ON CONFLICT (user_id, hour_bucket)
                DO UPDATE SET count = api_usage.count + 1
                RETURNING count;
            """, (user_id,))
            count = cur.fetchone()[0]
            app.logger.info(f"📈 Usage user={user_id} count={count}")
            return count
    finally:
        release_conn(conn)

# --------------------------------------------------
# UTILS
# --------------------------------------------------
def is_valid_tiktok_url(url: str) -> bool:
    return bool(re.search(r"(vm\.tiktok\.com|tiktok\.com)", url))

# --------------------------------------------------
# STREAM MP3
# --------------------------------------------------
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
        reset_at = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        return jsonify({
            "error": "Quota exceeded",
            "limit": QUOTA_PER_HOUR,
            "reset_at": reset_at.isoformat(),
        }), 429

    data = request.get_json(silent=True)
    url = data.get("url").strip() if data else None

    if not url or not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid or missing URL"}), 400

    temp_dir = tempfile.mkdtemp(prefix="tiktok_mp3_")
    video_path = os.path.join(temp_dir, "video.mp4")
    audio_path = os.path.join(temp_dir, "audio.mp3")

    try:
        # Download video
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

        # Convert to MP3
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", video_path,
                "-vn",
                "-acodec", "libmp3lame",
                "-ab", "192k",
                audio_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )

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
                "Content-Length": str(mp3_size),  # 🔑 progression Flutter
                "Cache-Control": "no-store",
                "Accept-Ranges": "none",
            },
        )

    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        app.logger.error("❌ MP3 processing failed")
        return jsonify({
            "error": "Video download or MP3 encoding failed",
            "details": e.stderr.decode(errors="ignore"),
        }), 500

# --------------------------------------------------
# HEALTH
# --------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# --------------------------------------------------
# RUN
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)




















