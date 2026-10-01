import fs from 'fs';
import path from 'path';
import { spawn } from 'child_process';
import mysql from 'mysql2/promise';

// Load .env if present locally (for testing without export)
if (fs.existsSync('.env')) {
  const envText = fs.readFileSync('.env', 'utf8');
  envText.split('\n').forEach(l => {
    const [k, ...v] = l.trim().split('=');
    if (k && !k.startsWith('#') && !process.env[k.trim()]) {
      process.env[k.trim()] = v.join('=').trim().replace(/^["']|["']$/g, '');
    }
  });
}

const DB_HOST = process.env.DB_HOST || '37.27.232.161';
const DB_PORT = parseInt(process.env.DB_PORT || '3306');
const DB_USER = process.env.DB_USER || 'jeevanka_user';
const DB_PASSWORD = process.env.DB_PASSWORD || '';
const DB_NAME = process.env.DB_NAME || 'jeevanka_anime';
// ==============================================================================
// 👥 Multi-Account Failover & Rotation Pool
// ==============================================================================
const ACCOUNT_POOL = [
  {
    id: 1,
    name: 'Account 1 (elfen0909)',
    apiKey: process.env.VIDARA_API_KEY_1 || '487cfda837c6b45562f3ecdcdda3d62dd6622c5ecb12887fa9ff59ec023585a7',
    folderSeries: parseInt(process.env.FOLDER_SERIES_ID_1 || '32797', 10),
    folderMovie: parseInt(process.env.FOLDER_MOVIE_ID_1 || '32791', 10),
    isExhausted: false
  },
  {
    id: 2,
    name: 'Account 2 (alex09op)',
    apiKey: process.env.VIDARA_API_KEY_2 || process.env.VIDARA_API_KEY || 'a3dd10efa43ce85c2f04ebd2b3977754f86ff27160178280e786c151784e5d47',
    folderSeries: parseInt(process.env.FOLDER_SERIES_ID_2 || process.env.FOLDER_SERIES_ID || '33143', 10),
    folderMovie: parseInt(process.env.FOLDER_MOVIE_ID_2 || process.env.FOLDER_MOVIE_ID || '33142', 10),
    isExhausted: false
  }
];

const SHARD_INDEX = parseInt(process.env.SHARD_INDEX || '0', 10);
const TOTAL_SHARDS = parseInt(process.env.TOTAL_SHARDS || '1', 10);
const MAX_EPISODES = parseInt(process.env.MAX_EPISODES || '0', 10); // 0 = unlimited
const TARGET_TMDB_ID = process.env.TARGET_TMDB_ID ? parseInt(process.env.TARGET_TMDB_ID, 10) : null;
const IS_TEST = process.argv.includes('--test') || process.env.TEST_MODE === 'true';

const QUALITY_SPECS = [
  { label: '1080p', code: 'haa' },
  { label: '720p',  code: 'gaa' },
  { label: '480p',  code: 'caa' },
  { label: '360p',  code: 'baa' },
  { label: '240p',  code: 'oaa' }
];

function parseRanges(rangesStr) {
  const map = {};
  if (!rangesStr) return map;
  const lines = rangesStr.split('\n');
  for (const line of lines) {
    const m = line.trim().match(/^(\d+-\d+)\s*\(([^)]+)\)/);
    if (m) {
      map[m[2].trim().toLowerCase()] = m[1].trim();
    }
  }
  return map;
}

async function checkStreamReachable(streamUrl) {
  try {
    const res = await fetch(streamUrl, {
      method: 'GET',
      headers: {
        'Origin': 'https://blakiteapi.xyz',
        'Referer': 'https://blakiteapi.xyz/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });
    return res.status === 200;
  } catch {
    return false;
  }
}

const uploadServerCache = new Map();

async function getVidaraUploadServer(apiKey) {
  const now = Date.now();
  const cached = uploadServerCache.get(apiKey);
  if (cached && (now - cached.time < 300000)) { // 5 min cache
    return cached.server;
  }
  const res = await fetch(`https://api.vidara.so/v1/upload/server?api_key=${apiKey}`);
  if (!res.ok) throw new Error(`Vidara server API error: HTTP ${res.status}`);
  const json = await res.json();
  const server = json.result?.upload_server || json.data?.upload_server;
  if (!server) throw new Error(`Failed to get Vidara upload server: ${JSON.stringify(json)}`);
  uploadServerCache.set(apiKey, { server, time: now });
  return server;
}

function remuxStreamWithFfmpeg(streamUrl, outputPath) {
  return new Promise((resolve, reject) => {
    const headers = 'Origin: https://blakiteapi.xyz\r\nReferer: https://blakiteapi.xyz/\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n';
    const args = [
      '-headers', headers,
      '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
      '-allowed_extensions', 'ALL',
      '-allowed_segment_extensions', 'ALL',
      '-extension_picky', '0',
      '-reconnect', '1',
      '-reconnect_streamed', '1',
      '-reconnect_delay_max', '5',
      '-i', streamUrl,
      '-c', 'copy',
      '-movflags', '+faststart',
      outputPath,
      '-y'
    ];

    const proc = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';

    // 8-minute watchdog to prevent stalling forever
    const timer = setTimeout(() => {
      try { proc.kill('SIGKILL'); } catch {}
      reject(new Error('FFmpeg remux timed out after 8 minutes'));
    }, 480000);

    proc.stderr.on('data', (d) => {
      stderr += d.toString();
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0 && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 1000) {
        resolve();
      } else {
        reject(new Error(`FFmpeg exited with code ${code}. Stderr: ${stderr.slice(-500)}`));
      }
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

async function uploadToSingleAccount(account, filePath, filename, isMovie, maxRetries = 4) {
  const targetFolderId = isMovie ? account.folderMovie : account.folderSeries;
  const folderLabel = isMovie ? `Movies (Folder #${account.folderMovie})` : `Series (Folder #${account.folderSeries})`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const uploadServer = await getVidaraUploadServer(account.apiKey);
      const fileBuffer = fs.readFileSync(filePath);
      const blob = new Blob([fileBuffer], { type: 'video/mp4' });

      const formData = new FormData();
      formData.append('api_key', account.apiKey);
      formData.append('file', blob, filename);
      if (targetFolderId) {
        formData.append('fld_id', String(targetFolderId));
        formData.append('folder_id', String(targetFolderId));
      }

      const res = await fetch(uploadServer, {
        method: 'POST',
        body: formData
      });

      const resText = await res.text();
      let json = null;
      try { json = JSON.parse(resText); } catch {}

      if (resText.includes('Daily upload limit reached') || json?.error?.includes('Daily upload limit reached')) {
        const msg = json?.error || resText;
        uploadServerCache.delete(account.apiKey);
        account.isExhausted = true;
        const limitErr = new Error(`DAILY_LIMIT [${account.name}]: ${msg}`);
        limitErr.isDailyLimit = true;
        limitErr.accountName = account.name;
        throw limitErr;
      }

      if (res.status === 429 || res.status === 502 || res.status === 503) {
        uploadServerCache.delete(account.apiKey);
        const waitSec = attempt * 10;
        console.warn(`   ⏳ [${account.name}] upload HTTP ${res.status}. Pausing ${waitSec}s (Attempt ${attempt}/${maxRetries})...`);
        await new Promise(r => setTimeout(r, waitSec * 1000));
        continue;
      }

      if (!res.ok) {
        uploadServerCache.delete(account.apiKey);
        throw new Error(`Vidara upload HTTP ${res.status}: ${resText.slice(0, 100)}`);
      }

      let filecode = json?.filecode || json?.result?.filecode || json?.data?.filecode;
      if (!filecode) {
        const raw = resText;
        if (raw.includes('Rate limit') || raw.includes('429')) {
          uploadServerCache.delete(account.apiKey);
          const waitSec = attempt * 12;
          console.warn(`   ⏳ [${account.name}] API rate limit in body. Pausing ${waitSec}s...`);
          await new Promise(r => setTimeout(r, waitSec * 1000));
          continue;
        }
        throw new Error(`Vidara response missing filecode: ${raw}`);
      }

      // Clean filecode if returned as full URL
      filecode = filecode.replace(/^https?:\/\/vidara\.[^/]+\/e\//, '').trim();

      // Call official move endpoint to guarantee file is in the target folder
      if (targetFolderId) {
        try {
          const moveRes = await fetch(`https://api.vidara.so/v1/video/move?api_key=${account.apiKey}&filecode=${filecode}&fld_id=${targetFolderId}`);
          if (moveRes.ok) {
            console.log(`   📁 Confirmed in ${account.name} (Folder #${targetFolderId})`);
          }
        } catch (moveErr) {
          console.warn(`   ⚠️ Warning: Folder move error: ${moveErr.message}`);
        }
      }

      return {
        filecode,
        embedUrl: `https://vidara.to/e/${filecode}`,
        watchUrl: `https://vidara.to/${filecode}`,
        accountName: account.name,
        folderLabel
      };
    } catch (err) {
      if (err.isDailyLimit) throw err;
      if (attempt === maxRetries) throw err;
      const waitSec = attempt * 5;
      console.warn(`   ⚠️ [${account.name}] Upload attempt ${attempt} error: ${err.message}. Retrying in ${waitSec}s...`);
      await new Promise(r => setTimeout(r, waitSec * 1000));
    }
  }
  throw new Error(`Failed to upload to ${account.name} after ${maxRetries} attempts`);
}

async function uploadWithFailover(filePath, filename, isMovie) {
  // Check if any accounts available
  const available = ACCOUNT_POOL.filter(a => !a.isExhausted);
  if (available.length === 0) {
    const allErr = new Error('ALL_ACCOUNTS_DAILY_LIMIT_EXHAUSTED');
    allErr.allAccountsExhausted = true;
    throw allErr;
  }

  for (const account of available) {
    try {
      console.log(`   📤 Trying upload to ${account.name}...`);
      const result = await uploadToSingleAccount(account, filePath, filename, isMovie);
      return result;
    } catch (err) {
      if (err.isDailyLimit) {
        console.warn(`\n⚠️  >>> ${account.name} DAILY LIMIT (200/day) REACHED! <<<`);
        console.warn(`   🔄 Auto-Failover: Switching to next available account in pool...\n`);
        continue; // Try next account in pool immediately with same remuxed file!
      }
      throw err;
    }
  }

  const allErr = new Error('ALL_ACCOUNTS_DAILY_LIMIT_EXHAUSTED');
  allErr.allAccountsExhausted = true;
  throw allErr;
}

async function main() {
  console.log(`=============================================================`);
  console.log(`🚀 ANIME VIDARA FLEET WORKER`);
  console.log(`⚙️  Shard: [${SHARD_INDEX + 1} / ${TOTAL_SHARDS}] (Mod: id % ${TOTAL_SHARDS} = ${SHARD_INDEX})`);
  if (IS_TEST) console.log(`🧪 Running in TEST mode (1 episode only)`);
  if (TARGET_TMDB_ID) console.log(`🎯 Target TMDB ID: ${TARGET_TMDB_ID}`);
  console.log(`=============================================================\n`);

  const activeKeys = ACCOUNT_POOL.filter(a => !!a.apiKey);
  if (activeKeys.length === 0) {
    console.error('❌ No Vidara API keys configured in ACCOUNT_POOL!');
    process.exit(1);
  }

  const pool = mysql.createPool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 3,
    enableKeepAlive: true,
    keepAliveInitialDelay: 10000
  });

  console.log(`👥 MULTI-ACCOUNT FAILOVER POOL:`);
  for (const acc of ACCOUNT_POOL) {
    try {
      const uRes = await fetch(`https://api.vidara.so/v1/user/info?api_key=${acc.apiKey}`);
      if (uRes.ok) {
        const u = await uRes.json();
        console.log(`   * ${acc.name}: User=${u.result?.username} | Total Videos=${u.result?.videos_total} | Folders: Series #${acc.folderSeries}, Movies #${acc.folderMovie}`);
      }
    } catch (e) {
      console.warn(`   * ${acc.name}: Probe warning - ${e.message}`);
    }
  }
  console.log(``);

  let processedCount = 0;
  const tempDir = path.resolve('./temp');
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

  try {
    while (true) {
      if (IS_TEST && processedCount >= 1) break;
      if (MAX_EPISODES > 0 && processedCount >= MAX_EPISODES) {
        console.log(`🎯 Reached MAX_EPISODES limit (${MAX_EPISODES}). Stopping.`);
        break;
      }

      // Fetch next HLS episode assigned to this shard
      let query = `
        SELECT id, tmdb_id, anime_title, format, season, episode, quality, filecode, qualities_json
        FROM anime_episodes
        WHERE stream_type = 'HLS'
      `;
      const params = [];

      if (TARGET_TMDB_ID) {
        query += ` AND tmdb_id = ?`;
        params.push(TARGET_TMDB_ID);
      }

      if (TOTAL_SHARDS > 1) {
        query += ` AND (id % ?) = ?`;
        params.push(TOTAL_SHARDS, SHARD_INDEX);
      }

      query += ` ORDER BY id ASC LIMIT 1`;

      const [rows] = await pool.query(query, params);
      if (!rows || rows.length === 0) {
        console.log(`✨ No remaining HLS episodes found for this shard. Worker finished!`);
        break;
      }

      const ep = rows[0];
      const epLabel = `[#${ep.id}] ${ep.anime_title} S${String(ep.season).padStart(2, '0')}E${String(ep.episode).padStart(2, '0')}`;
      console.log(`\n▶️  Processing ${epLabel}...`);

      let qData = ep.qualities_json;
      if (typeof qData === 'string') {
        try { qData = JSON.parse(qData); } catch {}
      }

      const dataId = qData?.dataId || ep.filecode;
      const ranges = parseRanges(qData?.ranges || '');

      if (!dataId) {
        console.error(`❌ Missing dataId for episode #${ep.id}. Skipping.`);
        // Mark as ERROR or update to prevent infinite loop
        await pool.query(`UPDATE anime_episodes SET stream_type = 'ERROR' WHERE id = ?`, [ep.id]);
        continue;
      }

      // Cascading Fallback: Try highest available quality first (1080p -> 720p -> 480p -> 360p -> 240p)
      let successfulRemux = false;
      let finalSpec = null;
      let finalTempFile = null;
      let finalUploadFilename = null;

      for (const spec of QUALITY_SPECS) {
        const range = ranges[spec.label.toLowerCase()];
        // If ranges are provided, make sure this quality has a range
        if (Object.keys(ranges).length > 0 && !range) continue;

        const rangeParam = range ? `&r_range=${encodeURIComponent(range)}` : '';
        const streamUrl = `https://hugh.cdn.rumble.cloud/video/${dataId}.${spec.code}.tar?r_file=chunklist.m3u8&r_type=application%2Fvnd.apple.mpegurl${rangeParam}`;

        console.log(`   🔍 Checking quality candidate: ${spec.label}...`);
        const isReachable = await checkStreamReachable(streamUrl);
        if (!isReachable) {
          console.log(`   ⏩ [${spec.label}] not accessible or forbidden, checking next lower quality...`);
          continue;
        }

        console.log(`   ⏳ Remuxing HLS (${spec.label}) via FFmpeg...`);
        const cleanTitle = (ep.anime_title || 'Anime').replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 30);
        const tempFile = path.join(tempDir, `ep_${ep.id}_${cleanTitle}_s${ep.season}e${ep.episode}_${spec.label}.mp4`);
        const startTime = Date.now();

        try {
          await remuxStreamWithFfmpeg(streamUrl, tempFile);
          const remuxSec = ((Date.now() - startTime) / 1000).toFixed(1);
          const fileSizeMB = (fs.statSync(tempFile).size / (1024 * 1024)).toFixed(1);
          console.log(`   ✅ Remuxed ${spec.label} (${fileSizeMB} MB in ${remuxSec}s)`);

          successfulRemux = true;
          finalSpec = spec;
          finalTempFile = tempFile;
          finalUploadFilename = `${cleanTitle}_S${ep.season}E${ep.episode}_${spec.label}.mp4`;
          break; // Successfully got highest working quality!
        } catch (ffmpegErr) {
          console.log(`   ⚠️ FFmpeg failed on ${spec.label}: ${ffmpegErr.message}. Trying next lower quality...`);
          if (fs.existsSync(tempFile)) {
            try { fs.unlinkSync(tempFile); } catch {}
          }
        }
      }

      if (!successfulRemux || !finalTempFile) {
        console.error(`   ❌ All qualities failed for episode #${ep.id}`);
        await pool.query(`UPDATE anime_episodes SET stream_type = 'HLS_ERROR' WHERE id = ?`, [ep.id]);
        continue;
      }

      try {
        const isMovie = (ep.format && String(ep.format).toLowerCase().trim() === 'movie');
        const upStart = Date.now();
        const vidaraResult = await uploadWithFailover(finalTempFile, finalUploadFilename, isMovie);
        const upSec = ((Date.now() - upStart) / 1000).toFixed(1);
        console.log(`   ✅ Uploaded in ${upSec}s via ${vidaraResult.accountName}! Filecode: ${vidaraResult.filecode} (${vidaraResult.folderLabel})`);

        // Update Database
        await pool.query(`
          UPDATE anime_episodes
          SET
            stream_type = 'MP4',
            filecode = ?,
            embed_url = ?,
            watch_url = ?,
            quality = ?
          WHERE id = ?
        `, [
          vidaraResult.filecode,
          vidaraResult.embedUrl,
          vidaraResult.watchUrl,
          finalSpec.label,
          ep.id
        ]);

        console.log(`   💾 Database updated: MP4 stream linked (${finalSpec.label})!`);
        processedCount++;

        // Check if entire series is now completed
        const [remainingInSeries] = await pool.query(`
          SELECT COUNT(*) as count 
          FROM anime_episodes 
          WHERE tmdb_id = ? AND stream_type = 'HLS'
        `, [ep.tmdb_id]);

        if (remainingInSeries[0].count === 0) {
          await pool.query(`UPDATE anime_series SET format = 'MP4' WHERE tmdb_id = ?`, [ep.tmdb_id]);
          console.log(`   🏆 Series TMDB ID ${ep.tmdb_id} completely mirrored to Vidara MP4!`);
        }

      } catch (err) {
        if (err.allAccountsExhausted) {
          console.error(`\n🛑 All accounts in the pool (Account 1 & Account 2) have exhausted their 200/day daily limits!`);
          console.error(`   Combined 400 uploads/day capacity filled.`);
          console.error(`   Free/Expired accounts reset at 00:00 UTC (05:30 AM IST).`);
          console.error(`   Shard #${SHARD_INDEX + 1} shutting down gracefully.\n`);
          process.exit(0);
        }
        console.error(`   ❌ Failed uploading episode #${ep.id}:`, err.message);
        // Do NOT mark as permanent HLS_ERROR on upload network/rate-limit failure.
        // It stays as 'HLS' to be retried safely on the next pass.
      } finally {
        // Always clean up temp file
        if (finalTempFile && fs.existsSync(finalTempFile)) {
          try { fs.unlinkSync(finalTempFile); } catch {}
        }
      }
    }

    console.log(`\n=============================================================`);
    console.log(`🎉 Fleet Worker Summary: Processed ${processedCount} episodes.`);
    console.log(`=============================================================`);

  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error('Fatal worker error:', err);
  process.exit(1);
});
