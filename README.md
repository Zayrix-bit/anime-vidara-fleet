# 🚀 Anime Vidara Fleet

High-speed cloud pipeline running **10 parallel GitHub Actions matrix workers** to remux HLS anime streams and direct upload MP4 streams to Vidara, updating the master database in real time.

---

## ⚡ How It Works

```
[Master MySQL DB] ──▶ (Fetches HLS Episode by Shard: id % 10)
        │
        ▼
[Cloud Worker] ──▶ (FFmpeg Remux HLS to MP4: ~10-15s, no re-encoding)
        │
        ▼
[Vidara API] ──▶ (Direct File Upload via HTTP POST)
        │
        ▼
[Master MySQL DB] ──▶ (Updates embed_url, stream_type='MP4', filecode)
```

- **10 Cloud Machines concurrently**: Fast gigabit speeds.
- **Self-Draining Database Queue**: Workers query `WHERE stream_type = 'HLS' AND (id % 10) = shard_index`.
- **Zero Duplication**: Each shard has a mathematically disjoint set of episodes.
- **Auto Cleanup**: Temporary files are deleted immediately after upload.

---

## 🔑 Required GitHub Secrets

Go to your repository:
**Settings ➔ Secrets and variables ➔ Actions ➔ New repository secret**

Add the following 6 secrets:

| Secret Name | Value | Description |
|---|---|---|
| `DB_HOST` | `37.27.232.161` | MySQL Database Host |
| `DB_PORT` | `3306` | MySQL Port |
| `DB_USER` | `jeevanka_user` | MySQL User |
| `DB_PASSWORD` | *(Your Database Password)* | Database Password |
| `DB_NAME` | `jeevanka_anime` | Database Name |
| `VIDARA_API_KEY` | *(Your Vidara API Key)* | Vidara Account API Key |

---

## 🚀 How to Run

1. Open the **Actions** tab in this GitHub repository: [Actions](https://github.com/Zayrix-bit/anime-vidara-fleet/actions)
2. Select **"Anime Vidara Fleet Pipeline"** from the left sidebar.
3. Click **"Run workflow"**.
   - Optional: Check `Run Test Mode` first if you want each worker to only process 1 episode as a test verification.
   - Or leave default (unlimited) and click the green **Run workflow** button!
4. Watch 10 workers churn through the catalog simultaneously!

---

## 💻 Local Testing (Optional)

To test a single episode locally:
```bash
npm install
node worker.js --test
```
