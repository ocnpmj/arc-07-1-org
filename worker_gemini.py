import os
import re
import unicodedata
import time
import json
import requests
import threading
from google import genai  # SDK baru: google-genai

# ============================
# KONFIG
# ============================
JOBS_API_URL = "https://leamarie-yoga.de/jobs_api.php"  # GANTI ke URL jobs_api.php kamu

MIN_SECONDS_PER_REQUEST = 10
MAX_RETRIES_PER_TITLE = 3
DEFAULT_QUOTA_SLEEP_SECONDS = 120

# Batas maksimal request ke Gemini per API key / thread
MAX_REQUESTS_PER_API = 250  # <<< BATAS REQUEST PER API KEY

# Berapa thread per worker (matrix). Ubahlah jika perlu.
THREADS_PER_WORKER = 3

# ============================
# FUNGSI BANTUAN
# ============================
def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text)
    text = text.strip("-")
    text = text.lower()
    return text or "article"


def parse_retry_delay_seconds(err_str: str) -> float:
    m = re.search(r"retry in ([0-9.]+)s", err_str)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return DEFAULT_QUOTA_SLEEP_SECONDS


def build_prompt(judul: str) -> str:
    judul_safe = judul.replace('"', "'")
    prompt = f"""
ABSOLUTELY NO <h1> TAG ALLOWED. START WITH <p> OR OUTPUT IS USELESS.

You are a professional SEO content writer.. 
Your articles regularly hit position #1–3 on Google because they are helpful, authoritative, and feel genuinely human.

Main title to write about: "{judul_safe}"

Your task:
Write one complete, high-quality SEO article in English that perfectly satisfies Google’s E-E-A-T guidelines.

Do these steps internally (never show them in the output):
1. Create 10 alternative, more clickable title variations (for your reference only).
2. Build a logical, value-packed outline with at least 7–9 H2 sections before FAQ & Conclusion.
3. Research/recall the most recent 2024–2025 data, statistics, tools, or trends related to the topic.

STRICT WRITING RULES YOU MUST FOLLOW:
- Write in a warm, conversational yet authoritative tone — like a trusted expert talking directly to the reader.
- Use “you” frequently to make it personal and engaging.
- Naturally weave in real-world experience or observations.
- Use smooth transitions (however, here’s the thing, the good news is, interestingly, for example, etc.).
- Keep passive voice under 8%.
- Avoid keyword stuffing — use the main keyword and related terms naturally.
- Every section must deliver real value; no fluff.
- When using lists, make them numbered H3s (1., 2., 3…) and explain each item in depth.
- Include up-to-date facts, statistics, tools, or case studies where relevant.
- Opening paragraph: instantly engaging, data-rich or insight-rich, no rhetorical questions.

REQUIRED STRUCTURE:
- Strong introduction
- Logical H2 sections
- Use numbered <h3> for lists inside sections
- End with exactly these two sections:
  <h2>FAQ</h2>
  <h2>Conclusion</h2>

OUTPUT FORMAT:
1. ONLY the clean article HTML (no <html>, <head>, or <body>).
2. After the HTML, add one blank line, lalu:
   META_DESC: your compelling meta description (145–160 characters, plain text, no quotes)

Now write the best possible article for this title:
"{judul_safe}"
"""
    return prompt


# ============================
# LOAD API KEY DARI ENV + WORKER_INDEX
# ============================
worker_index_str = os.getenv("WORKER_INDEX", "0")
try:
    WORKER_INDEX = int(worker_index_str)
except ValueError:
    raise ValueError(f"WORKER_INDEX bukan integer valid: {worker_index_str}")

# Ambil raw secret: boleh berisi 1 atau banyak API key (dipisah newline)
raw_api = os.getenv("GEMINI_API_KEY", "").strip()
if not raw_api:
    raise ValueError(
        "Environment variable GEMINI_API_KEY tidak ditemukan / kosong. "
        "Pastikan sudah diset di GitHub Secrets dan dipasang di YAML."
    )

# Pecah per baris → jadi list API key
api_keys = [line.strip() for line in raw_api.splitlines() if line.strip()]

if not api_keys:
    raise ValueError(
        "GEMINI_API_KEY ter-set tapi tidak ada API key valid (semua kosong?)."
    )

# Pastikan ada cukup API key untuk worker ini
start_idx = WORKER_INDEX * THREADS_PER_WORKER
end_idx = start_idx + THREADS_PER_WORKER
if start_idx < 0 or end_idx > len(api_keys):
    raise IndexError(
        f"WORKER_INDEX={WORKER_INDEX} dengan THREADS_PER_WORKER={THREADS_PER_WORKER} butuh "
        f"API keys dari index {start_idx} sampai {end_idx-1}. Namun hanya ada {len(api_keys)} key."
    )

worker_api_keys = api_keys[start_idx:end_idx]

print(
    f"🔑 Worker index {WORKER_INDEX} akan pakai {len(worker_api_keys)} key (thread per worker = {THREADS_PER_WORKER})."
)

# ============================
# AMBIL JOB DARI SERVER (shared)
# ============================
def get_next_job(max_retries: int = 5):
    """
    Ambil 1 job dari server.
    - Return dict job     → kalau sukses
    - Return None         → kalau server bilang 'no_job' (benar-benar habis)
    - Return "RETRY"      → kalau error sementara (500, network, dll)
    """
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(JOBS_API_URL, params={"action": "next"}, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[JOB] ❌ Error ambil job (attempt {attempt}): {e}")
            # Error koneksi / HTTP 500 / timeout → tunggu sebentar lalu coba lagi
            time.sleep(5)
            continue

        # Kalau server balas ok = False, lihat reason
        if not data.get("ok"):
            reason = data.get("reason")
            if reason == "no_job":
                print("[JOB] ✅ Server bilang: tidak ada job pending.")
                return None  # benar-benar habis
            else:
                print("[JOB] ⚠ Response tidak OK:", data)
                # Anggap error sementara, coba lagi
                time.sleep(5)
                continue

        # Sukses ambil job
        return data.get("job")

    # Sudah coba beberapa kali tapi tetap gagal → suruh caller RETRY nanti
    print("[JOB] ❌ Gagal ambil job setelah beberapa attempt. Tidur 60 detik lalu coba lagi.")
    time.sleep(60)
    return "RETRY"


# ============================
# KIRIM HASIL KE SERVER (shared)
# ============================
def submit_result(job_id, status, judul=None, slug=None, metadesc=None, artikel=None):
    payload = {"job_id": job_id, "status": status}

    if status == "done":
        payload.update({
            "judul": judul,
            "slug": slug,
            "metadesc": metadesc,
            "artikel": artikel,
        })

    try:
        r = requests.post(
            JOBS_API_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        r.raise_for_status()
        print(f"[JOB {job_id}] 📤 POST sukses:", r.json())
    except Exception as e:
        print(f"[JOB {job_id}] ❌ Error submit: {e}")


# ============================
# THREAD WORKER
# ============================
print_lock = threading.Lock()
total_sukses = 0
total_sukses_lock = threading.Lock()

class ThreadWorker(threading.Thread):
    def __init__(self, api_key: str, thread_idx: int):
        super().__init__(daemon=True)
        self.api_key = api_key
        self.thread_idx = thread_idx
        self.client = genai.Client(api_key=api_key)
        self.request_count = 0
        self.local_success = 0

    def log(self, *args, **kwargs):
        with print_lock:
            print(f"[W{WORKER_INDEX}-T{self.thread_idx}]", *args, **kwargs)

    def run(self):
        last_call = 0.0

        while True:
            # Cek batas request untuk API/key/thread ini
            if self.request_count >= MAX_REQUESTS_PER_API:
                self.log(f"⏹️ Batas {MAX_REQUESTS_PER_API} request tercapai untuk key ini. Thread berhenti.")
                break

            job = get_next_job()

            if job is None:
                self.log("🎉 Tidak ada job lagi (no_job). Thread selesai.")
                break

            if job == "RETRY":
                # dapat error ambil job → coba lagi (sedikit jeda untuk tidak spam)
                time.sleep(5)
                continue

            job_id = job["id"]
            judul = job["keyword"]
            self.log(f"🎯 Ambil job {job_id} | '{judul}'")

            success = False
            for attempt in range(1, MAX_RETRIES_PER_TITLE + 1):
                try:
                    elapsed = time.time() - last_call
                    if elapsed < MIN_SECONDS_PER_REQUEST:
                        to_sleep = MIN_SECONDS_PER_REQUEST - elapsed
                        self.log(f"Menunggu {to_sleep:.1f}s untuk respect rate limit per thread.")
                        time.sleep(to_sleep)

                    # Catat request akan dilakukan
                    self.request_count += 1
                    self.log(f"🔄 Gemini request attempt {attempt} (total_request={self.request_count}/{MAX_REQUESTS_PER_API})")

                    prompt = build_prompt(judul)
                    res = self.client.models.generate_content(
                        model="gemini-2.5-flash",
                        contents=prompt,
                    )
                    last_call = time.time()

                    raw = (res.text or "").strip()
                    if not raw:
                        self.log(f"⚠ Output kosong dari Gemini.")
                        break

                    # META_DESC parsing
                    m = re.search(r"META_DESC\s*:(.*)$", raw, re.IGNORECASE | re.DOTALL)
                    if m:
                        metadesc = m.group(1).strip()
                        artikel_html = raw[: m.start()].strip()
                    else:
                        self.log(f"⚠ META_DESC tidak ditemukan, generate dari artikel.")
                        artikel_html = raw
                        txt = re.sub(r"<.*?>", " ", artikel_html)
                        txt = re.sub(r"\s+", " ", txt).strip()
                        metadesc = txt[:155]

                    if not artikel_html:
                        self.log(f"⚠ Artikel kosong setelah parsing.")
                        break

                    slug = slugify(judul)

                    submit_result(
                        job_id=job_id,
                        status="done",
                        judul=judul,
                        slug=slug,
                        metadesc=metadesc,
                        artikel=artikel_html,
                    )

                    self.local_success += 1
                    with total_sukses_lock:
                        global total_sukses
                        total_sukses += 1

                    self.log(f"✅ DONE job {job_id}. Local sukses: {self.local_success} | Total sukses: {total_sukses}")
                    success = True
                    break

                except Exception as e:
                    err_str = str(e)
                    low = err_str.lower()
                    self.log(f"❌ Error Gemini: {err_str}")

                    # Kalau key ketahuan leaked / permission denied → jangan retry terus
                    if "reported as leaked" in low or "permission_denied" in low:
                        self.log("⛔ API key bermasalah (leaked/permission). Thread stop.")
                        submit_result(job_id=job_id, status="failed")
                        return

                    if "quota" in low or "limit" in low or "exceeded" in low:
                        delay = parse_retry_delay_seconds(err_str)
                        self.log(f"🚫 Quota/limit → tidur {delay:.1f}s lalu coba lagi.")
                        time.sleep(delay)
                        continue

                    self.log("⚠ Error lain → sleep 10 detik lalu retry.")
                    time.sleep(10)

            if not success:
                self.log(f"❌ Gagal permanen job {job_id} setelah {MAX_RETRIES_PER_TITLE} attempt.")
                submit_result(job_id=job_id, status="failed")

        self.log(f"Thread selesai. Local sukses: {self.local_success}")


# ============================
# START THREADS
# ============================
def main():
    threads = []
    for idx, key in enumerate(worker_api_keys):
        t = ThreadWorker(api_key=key, thread_idx=idx)
        threads.append(t)
        t.start()
        time.sleep(0.2)  # small stagger supaya tidak langsung spike semua thread

    # Tunggu semua selesai
    for t in threads:
        t.join()

    print(f"\n🎉 Semua thread worker (WORKER_INDEX={WORKER_INDEX}) selesai. Total artikel sukses: {total_sukses}")


if __name__ == "__main__":
    main()
