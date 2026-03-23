const { PutObjectCommand, S3Client } = require("@aws-sdk/client-s3");
const mongoose = require("mongoose");
const Fuse = require("fuse.js");
const fs = require("fs");
const path = require("path");
const Patient = require("../models/patients");
const Scan = require("../models/scans");
const dotenv = require("dotenv");

dotenv.config();

// ─── Configuration ───────────────────────────────────────────────────────────

const ROOT_DIR = String.raw`D:\report\PET-CT Reports\2021\April 2021\01 April 2021`;
const PROGRESS_FILE = path.join(__dirname, "..", "progress.json");
const BUCKET = process.env.S3_BUCKET_NAME;
const REGION = process.env.AWS_REGION;
const FUSE_THRESHOLD = 0.3;

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// ─── Directory Walker ────────────────────────────────────────────────────────

const EXCLUDED_DIRS = new Set(["images", "ravi"]);

function isDirExcluded(dirName) {
  const lower = dirName.toLowerCase();
  if (EXCLUDED_DIRS.has(lower)) return true;
  if (lower.includes("form")) return true;
  return false;
}

function walkDocxFiles(dir) {
  const results = [];
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    console.warn(`Cannot read directory: ${dir} — ${err.message}`);
    return results;
  }

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!isDirExcluded(entry.name)) {
        results.push(...walkDocxFiles(fullPath));
      }
    } else if (
      entry.isFile() &&
      path.extname(entry.name).toLowerCase() === ".docx"
    ) {
      results.push(fullPath);
    }
  }
  return results;
}

// ─── Date Parser ─────────────────────────────────────────────────────────────

const MONTHS = {
  january: 0, february: 1, march: 2, april: 3,
  may: 4, june: 5, july: 6, august: 7,
  september: 8, october: 9, november: 10, december: 11,
  jan: 0, feb: 1, mar: 2, apr: 3,
  aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

const DATE_PATTERN_LONG = /^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/;
const DATE_PATTERN_DOT = /^(\d{1,2})\.(\d{1,2})\.(\d{4})$/;

function parseDateFolder(name) {
  let match = name.match(DATE_PATTERN_LONG);
  if (match) {
    const day = parseInt(match[1], 10);
    const monthIdx = MONTHS[match[2].toLowerCase()];
    const year = parseInt(match[3], 10);
    if (monthIdx !== undefined) {
      return new Date(year, monthIdx, day);
    }
  }

  match = name.match(DATE_PATTERN_DOT);
  if (match) {
    const day = parseInt(match[1], 10);
    const month = parseInt(match[2], 10) - 1;
    const year = parseInt(match[3], 10);
    return new Date(year, month, day);
  }

  return null;
}

function extractStudyDateAndYear(filePath) {
  const relative = path.relative(ROOT_DIR, filePath);
  const parts = relative.split(path.sep);

  let studyDate = null;
  let year = null;

  for (const part of parts) {
    if (/^\d{4}$/.test(part)) {
      year = part;
    }
    const parsed = parseDateFolder(part);
    if (parsed && !isNaN(parsed.getTime())) {
      studyDate = parsed;
      if (!year) year = String(parsed.getFullYear());
    }
  }

  return { studyDate, year };
}

// ─── Name Cleaner ────────────────────────────────────────────────────────────

const PREFIX_PATTERN = /^(mrs|dr|review)\s+/i;
const SUFFIX_PATTERN = /\s+(comparison|comarison)$/i;
const TRAILING_NUM_PATTERN = /\s+\d+$/;

function cleanPatientName(filename) {
  let name = path.basename(filename, ".docx");
  name = name.replace(TRAILING_NUM_PATTERN, "");
  name = name.replace(PREFIX_PATTERN, "");
  name = name.replace(SUFFIX_PATTERN, "");
  return name.trim();
}

// ─── Fuzzy Matcher ───────────────────────────────────────────────────────────

function buildFuse(patients) {
  return new Fuse(patients, {
    keys: ["name"],
    threshold: FUSE_THRESHOLD,
    includeScore: true,
  });
}

async function findOrCreatePatient(name, patientsCache, fuse) {
  const exactMatch = patientsCache.find(
    (p) => p.name.toLowerCase() === name.toLowerCase(),
  );
  if (exactMatch) return exactMatch._id;

  const results = fuse.search(name);
  if (results.length > 0 && results[0].score <= FUSE_THRESHOLD) {
    return results[0].item._id;
  }

  const doc = await Patient.create({ name });
  const entry = { _id: doc._id, name: doc.name };
  patientsCache.push(entry);
  fuse.setCollection(patientsCache);
  return doc._id;
}

// ─── S3 Upload ───────────────────────────────────────────────────────────────

async function uploadToS3(filePath, year, originalFilename) {
  const key = `${year}/${originalFilename}`;
  const body = fs.readFileSync(filePath);

  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: body,
      ContentType:
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }),
  );

  return `https://${BUCKET}.s3.${REGION}.amazonaws.com/${encodeURIComponent(key).replace(/%2F/g, "/")}`;
}

// ─── Progress Tracking ──────────────────────────────────────────────────────

function loadProgress() {
  try {
    const data = fs.readFileSync(PROGRESS_FILE, "utf-8");
    return new Set(JSON.parse(data));
  } catch {
    return new Set();
  }
}

function saveProgress(processed) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processed], null, 2));
}

// ─── MongoDB Connection (SRV fallback) ───────────────────────────────────────

const MONGO_STANDARD_URI =
  "mongodb://pulselabs13_db_user:tDZwThFhIber30yY@" +
  "ac-mmcizf1-shard-00-00.hcdmbzo.mongodb.net:27017," +
  "ac-mmcizf1-shard-00-01.hcdmbzo.mongodb.net:27017," +
  "ac-mmcizf1-shard-00-02.hcdmbzo.mongodb.net:27017/pulse" +
  "?ssl=true&replicaSet=atlas-z2o3xu-shard-0&authSource=admin&appName=Cluster0";

async function connectMongo() {
  try {
    await mongoose.connect(process.env.MONGO_URI);
  } catch (err) {
    if (err.code === "ECONNREFUSED" || err.message.includes("querySrv")) {
      console.warn("SRV lookup failed — retrying with standard connection string …");
      await mongoose.connect(MONGO_STANDARD_URI);
    } else {
      throw err;
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  await connectMongo();
  console.log("MongoDB connected!");

  const existingPatients = await Patient.find({}, { name: 1 }).lean();
  const patientsCache = existingPatients.map((p) => ({
    _id: p._id,
    name: p.name,
  }));
  const fuse = buildFuse(patientsCache);
  console.log(`Loaded ${patientsCache.length} existing patients for matching`);

  const processed = loadProgress();
  console.log(`Resuming — ${processed.size} files already processed`);

  console.log(`Scanning ${ROOT_DIR} ...`);
  const allFiles = walkDocxFiles(ROOT_DIR);
  console.log(`Found ${allFiles.length} .docx files total`);

  const toProcess = allFiles.filter((f) => !processed.has(f));
  console.log(`${toProcess.length} files remaining to process\n`);

  let success = 0;
  let errors = 0;

  for (let i = 0; i < toProcess.length; i++) {
    const filePath = toProcess[i];
    const filename = path.basename(filePath);

    try {
      const { studyDate, year } = extractStudyDateAndYear(filePath);

      if (!year) {
        console.warn(`  [SKIP] No year found for: ${filePath}`);
        errors++;
        continue;
      }

      const patientName = cleanPatientName(filename);
      if (!patientName) {
        console.warn(`  [SKIP] Empty name after cleaning: ${filename}`);
        errors++;
        continue;
      }

      const patientId = await findOrCreatePatient(
        patientName,
        patientsCache,
        fuse,
      );

      const reportUrl = await uploadToS3(filePath, year, filename);

      await Scan.create({
        patient: patientId,
        studyDate: studyDate || undefined,
        report_url: reportUrl,
      });

      processed.add(filePath);
      saveProgress(processed);
      success++;

      console.log(
        `[${i + 1}/${toProcess.length}] ✓ ${patientName} | ${year} | ${studyDate ? studyDate.toDateString() : "no date"}`,
      );
    } catch (err) {
      errors++;
      console.error(`[${i + 1}/${toProcess.length}] ✗ ${filename} — ${err.message}`);
    }
  }

  console.log(
    `\nDone! Processed: ${success}, Errors: ${errors}, Total: ${toProcess.length}`,
  );
  await mongoose.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
