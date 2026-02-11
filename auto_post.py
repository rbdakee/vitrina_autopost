"""
Автопостинг видео из Google Drive в соцсети (upload-post.com).
Читает посты из Google Sheets, скачивает видео с Drive, уникализирует по профилю и публикует.

Сценарий:
- 1 строка (пост) -> 1 видео из Drive
- N users (profiles) -> для каждого user создаётся 1 уникальная версия
- один и тот же user может публиковать эту версию в Instagram и TikTok
- уникализация делается 1 раз на user, затем переиспользуется на все платформы user-а
"""
import os
import re
import json
import hashlib
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Важно: импортируем file-to-file функцию
from unique import uniquify_video_file

load_dotenv()

UPLOAD_POST_API_KEY = os.getenv("UPLOAD_POST_API_KEY")
UPLOAD_POST_ENDPOINT = os.getenv("UPLOAD_POST_ENDPOINT", "https://api.upload-post.com/api/upload")

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

SHEET_POSTS = os.getenv("SHEET_POSTS", "posts")
SHEET_SETUP = os.getenv("SHEET_SETUP", "setup")
SHEET_HISTORY = os.getenv("SHEET_HISTORY", "history_posts")

_max = os.getenv("MAX_POSTS_PER_RUN")
MAX_POSTS_PER_RUN = int(_max) if (_max and _max.strip().isdigit()) else None

TEST_RUN = str(os.getenv("TEST_RUN", "FALSE")).strip().upper() == "TRUE"
ENABLE_UNIQUE = str(os.getenv("ENABLE_UNIQUE", "TRUE")).strip().upper() == "TRUE"
UNIQUE_LOGO_PATH = os.getenv("UNIQUE_LOGO_PATH", "")
UNIQUE_OVERLAY_TEXT = os.getenv("UNIQUE_OVERLAY_TEXT", "")

_stale = os.getenv("PROCESSING_STALE_MINUTES", "10")
PROCESSING_STALE_MINUTES = int(_stale) if (_stale and _stale.strip().isdigit()) else 10

_upload_to = os.getenv("UPLOAD_TIMEOUT", "600")
UPLOAD_TIMEOUT = int(_upload_to) if (_upload_to and _upload_to.strip().isdigit()) else 600

VIDEO_MIMETYPES = ("video/", "application/octet-stream")  # octet-stream — общий fallback

# ===== POSTS columns =====
COL_CAPTION = "caption"
COL_DRIVE_FILE_LINK = "drive_file_link"
COL_TO_POST = "to_post"
COL_STATUS = "status"
COL_RESULT = "result"

# ===== SETUP columns =====
COL_USERS = "users"
PLATFORM_COLUMNS = ["instagram", "tiktok"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def is_true(v) -> bool:
    return (v is True) or (isinstance(v, str) and v.strip().upper() == "TRUE")


def normalize_headers(d: dict) -> dict:
    return {str(k).strip().lower(): v for k, v in d.items()}


def safe_filename(s: str) -> str:
    """Безопасное имя файла: убирает пробелы, слэши, эмодзи и т.п."""
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", (s or "").strip())
    return (s[:80] or "user")


def drive_url_to_file_id(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    if "drive.google.com" not in value and re.fullmatch(r"[A-Za-z0-9_-]{10,}", value):
        return value
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", value)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", value)
    if m:
        return m.group(1)
    return ""


def load_creds():
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    if os.path.exists(SERVICE_ACCOUNT_JSON):
        return Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

    return Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES)


def ensure_history_header(ws_history):
    try:
        first_row = ws_history.row_values(1)
        if first_row and any(cell.strip() for cell in first_row):
            return
    except Exception:
        pass

    ws_history.update(
        range_name="A1:H1",
        values=[[
            "ts", "post_row", "user", "platform", "caption", "drive_file_id", "status", "result",
        ]],
    )


def read_setup_destinations(ws_setup) -> List[dict]:
    """Возвращает список {user, platform}. Группировка по user — в main."""
    records = ws_setup.get_all_records()
    dests: List[dict] = []
    for r in records:
        rn = normalize_headers(r)
        user = str(rn.get(COL_USERS) or "").strip()
        if not user:
            continue
        for pcol in PLATFORM_COLUMNS:
            if is_true(rn.get(pcol)):
                dests.append({"user": user, "platform": pcol})
    return dests


def group_destinations_by_user(dests: List[dict]) -> Dict[str, List[str]]:
    """Группирует назначения по user: {user: [platform1, platform2, ...]}."""
    by_user: Dict[str, List[str]] = {}
    for d in dests:
        user, platform = d["user"], d["platform"]
        if user not in by_user:
            by_user[user] = []
        if platform not in by_user[user]:
            by_user[user].append(platform)
    return by_user


def is_processing_stale(result_val: str) -> bool:
    """True если status=processing и «processing since» старше PROCESSING_STALE_MINUTES."""
    prefix = "processing since "
    if not result_val or prefix not in result_val:
        return False
    try:
        ts_str = result_val.split(prefix, 1)[1].strip().split("\n")[0].strip()
        parsed = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta_min = (now - parsed).total_seconds() / 60
        return delta_min >= PROCESSING_STALE_MINUTES
    except (ValueError, IndexError):
        return False


def make_unique_seed(file_id: str, user: str) -> int:
    """Детерминированный seed: стабилен по file_id+user, не зависит от номера строки."""
    s = f"{file_id}:{user}"
    return int(hashlib.sha256(s.encode()).hexdigest()[:12], 16)


def download_drive_file_to_path(drive_service, file_id: str, dst_path: Path) -> str:
    """
    Скачивает файл из Drive сразу на диск (dst_path). Возвращает filename из метадаты.
    Проверяет mimeType на "похоже на видео".
    """
    meta = drive_service.files().get(fileId=file_id, fields="name,mimeType,size").execute()
    filename = meta.get("name", "video.mp4")
    mime = (meta.get("mimeType") or "").lower()
    if mime and not any(mime.startswith(p) for p in VIDEO_MIMETYPES):
        raise ValueError(f"Drive file is not a video (mimeType={mime}). Expected video/* or application/octet-stream.")

    request = drive_service.files().get_media(fileId=file_id)

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dst_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return filename


def upload_post_video_path(video_path: Path, filename: str, user: str, caption: str, platform: str) -> dict:
    """
    Загружает видео как file-stream (не читая весь файл в RAM).
    """
    headers = {"Authorization": f"Apikey {UPLOAD_POST_API_KEY}"}
    title = caption.strip() if caption else os.path.splitext(filename)[0] or "Untitled"

    multipart_data = [
        ("user", user),
        ("platform[]", platform.strip().lower()),
        ("title", title),
    ]
    if caption:
        multipart_data.append(("description", caption))

    with open(video_path, "rb") as f:
        files = {"video": (filename, f, "video/mp4")}
        resp = requests.post(
            UPLOAD_POST_ENDPOINT,
            headers=headers,
            data=multipart_data,
            files=files,
            timeout=UPLOAD_TIMEOUT,
        )

    resp.raise_for_status()
    ct = resp.headers.get("content-type", "")
    return resp.json() if ct.startswith("application/json") else {"raw": resp.text}


def main():
    # print("STEP 1: entering main()", flush=True)

    if not UPLOAD_POST_API_KEY and not TEST_RUN:
        raise RuntimeError("UPLOAD_POST_API_KEY is not set")
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")

    # print("STEP 2: load_creds()", flush=True)
    creds = load_creds()

    # print("STEP 3: gspread.authorize()", flush=True)
    gc = gspread.authorize(creds)

    # print("STEP 4: open spreadsheet", flush=True)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        print("Sheets title:", sh.title, flush=True)
    except Exception:
        pass

    # print("STEP 5: open worksheets", flush=True)
    ws_posts = sh.worksheet(SHEET_POSTS)
    ws_setup = sh.worksheet(SHEET_SETUP)
    ws_history = sh.worksheet(SHEET_HISTORY)

    # print("STEP 6: ensure history header", flush=True)
    ensure_history_header(ws_history)

    # print("STEP 7: read setup destinations", flush=True)
    dests = read_setup_destinations(ws_setup)
    if not dests:
        print("SETUP ERROR: no destinations. Check setup sheet: users + platform checkboxes.", flush=True)
        return

    by_user = group_destinations_by_user(dests)
    total_targets = sum(len(v) for v in by_user.values())
    print(f"Destinations users={len(by_user)} total_targets={total_targets}", flush=True)

    # print("STEP 8: build drive service", flush=True)
    drive_service = build("drive", "v3", credentials=creds)

    # print("STEP 9: read posts rows", flush=True)
    posts = ws_posts.get_all_records()
    if not posts:
        print("No posts rows.", flush=True)
        return

    headers = ws_posts.row_values(1)
    header_to_col = {str(h).strip().lower(): i + 1 for i, h in enumerate(headers)}

    def set_cell(row_idx_1based: int, col_name: str, value):
        col = header_to_col.get(str(col_name).strip().lower())
        if col:
            ws_posts.update_cell(row_idx_1based, col, value)

    posts_done = 0
    logo_path = Path(UNIQUE_LOGO_PATH) if UNIQUE_LOGO_PATH and Path(UNIQUE_LOGO_PATH).is_file() else None

    for row_idx, row in enumerate(posts, start=2):
        if MAX_POSTS_PER_RUN is not None and posts_done >= MAX_POSTS_PER_RUN:
            # print(f"STEP 10: MAX_POSTS_PER_RUN reached ({MAX_POSTS_PER_RUN}), stopping", flush=True)
            break

        rn = normalize_headers(row)
        if not is_true(rn.get(COL_TO_POST)):
            continue

        status_val = str(rn.get(COL_STATUS) or "").strip().lower()
        result_val = str(rn.get(COL_RESULT) or "")

        if status_val == "posted":
            continue
        if status_val == "processing" and not is_processing_stale(result_val):
            continue

        caption = str(rn.get(COL_CAPTION) or "").strip()
        drive_link = str(rn.get(COL_DRIVE_FILE_LINK) or "").strip()
        file_id = drive_url_to_file_id(drive_link)

        # print(f"\nSTEP 11: processing row={row_idx} file_id={file_id or 'EMPTY'}", flush=True)

        if not file_id:
            set_cell(row_idx, COL_STATUS, "failed")
            set_cell(row_idx, COL_RESULT, "Could not extract Drive file_id from drive_file_link")
            set_cell(row_idx, COL_TO_POST, "FALSE")
            ws_history.append_row([now_iso(), row_idx, "", "", caption, "", "failed",
                                   "Could not extract Drive file_id from drive_file_link"])
            posts_done += 1
            continue

        set_cell(row_idx, COL_STATUS, "processing")
        set_cell(row_idx, COL_RESULT, f"processing since {now_iso()}")

        ok = 0
        fail = 0
        results_compact: List[str] = []

        with tempfile.TemporaryDirectory(prefix="auto_post_") as td:
            td_path = Path(td)

            if TEST_RUN:
                input_path = td_path / "TEST_RUN.mp4"
                input_path.write_bytes(b"")
                filename = "TEST_RUN.mp4"
                # print("STEP 12: TEST_RUN enabled (no Drive download)", flush=True)
            else:
                input_path = td_path / "input.mp4"
                # print(f"STEP 12: download Drive -> {input_path}", flush=True)
                try:
                    filename = download_drive_file_to_path(drive_service, file_id, input_path)
                    print(f"Downloaded: {filename}", flush=True)
                except Exception as e:
                    err = f"Drive download failed: {e}"
                    set_cell(row_idx, COL_STATUS, "failed")
                    set_cell(row_idx, COL_RESULT, err[:45000])
                    set_cell(row_idx, COL_TO_POST, "FALSE")
                    ws_history.append_row([now_iso(), row_idx, "", "", caption, file_id, "failed", err[:45000]])
                    posts_done += 1
                    continue

            base = os.path.splitext(filename)[0]

            for user, platforms in by_user.items():
                user_safe = safe_filename(user)
                out_filename = f"{base}_{user_safe}.mp4"

                try:
                    if TEST_RUN or not ENABLE_UNIQUE:
                        out_path = input_path
                        # if not ENABLE_UNIQUE:
                        #     print(f"STEP 13: unique disabled -> user={user}", flush=True)
                    else:
                        out_path = td_path / f"unique_{user_safe}.mp4"
                        seed = make_unique_seed(file_id, user)
                        # print(f"STEP 13: uniquify user={user} seed={seed}", flush=True)
                        uniquify_video_file(
                            input_path=input_path,
                            output_path=out_path,
                            seed=seed,
                            skip_if_exists=False,
                            logo_path=logo_path,
                            overlay_text=UNIQUE_OVERLAY_TEXT or None,
                        )

                    for platform in platforms:
                        try:
                            if TEST_RUN:
                                result = {"test_run": True, "user": user, "platform": platform}
                            else:
                                # print(f"STEP 14: upload user={user} platform={platform}", flush=True)
                                result = upload_post_video_path(
                                    video_path=out_path,
                                    filename=out_filename,
                                    user=user,
                                    caption=caption,
                                    platform=platform,
                                )
                            ok += 1
                            rtxt = json.dumps(result, ensure_ascii=False)[:45000]
                            ws_history.append_row([now_iso(), row_idx, user, platform, caption, file_id, "posted", rtxt])
                            results_compact.append(f"{platform}:{user}=ok")
                        except Exception as e:
                            fail += 1
                            err = str(e)[:45000]
                            ws_history.append_row([now_iso(), row_idx, user, platform, caption, file_id, "failed", err])
                            results_compact.append(f"{platform}:{user}=fail")

                except Exception as e:
                    err = str(e)[:45000]
                    for platform in platforms:
                        fail += 1
                        ws_history.append_row([now_iso(), row_idx, user, platform, caption, file_id, "failed", err])
                        results_compact.append(f"{platform}:{user}=fail")

        final_status = "posted" if (ok > 0 and fail == 0) else ("partial" if ok > 0 else "failed")
        set_cell(row_idx, COL_STATUS, final_status)
        set_cell(row_idx, COL_RESULT, f"ok={ok} fail={fail} | " + ", ".join(results_compact))
        set_cell(row_idx, COL_TO_POST, "FALSE")

        posts_done += 1
        print(f"Row {row_idx} done: {final_status} (ok={ok}, fail={fail})", flush=True)

    if posts_done == 0:
        print("No rows to post (no to_post=TRUE or already processed).", flush=True)



if __name__ == "__main__":
    print("=== RUN START ===", now_iso())
    main()
    print("=== RUN END ===", now_iso())
