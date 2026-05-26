#!/usr/bin/env node
/**
 * SageMaker load test CLI for the Deepgram JS SDK + sagemaker transport.
 *
 * Mirrors the Java/Python reference harness flag-for-flag:
 *
 *   node loadtest/dg-sdk-loadtest.mjs <endpoint-name> \
 *       --file ./english.wav \
 *       --reference ./english.txt \
 *       --connections 400 \
 *       --no-loop \
 *       --region us-east-2 \
 *       --transcripts-dir /tmp/proc-01-transcripts
 *
 * Opens N concurrent streaming connections through the Deepgram SDK + the
 * SageMaker transport, captures final transcripts per connection, and
 * reports per-connection WER against a reference transcript.
 */

import { readFile, mkdir, writeFile } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";

import { SdkStreamingConnection, buildConnectOptions } from "./connection.mjs";
import { computeWer } from "./wer.mjs";
import { readWavInfo } from "./wav.mjs";

const require_ = createRequire(import.meta.url);

function gitHead(repoRoot) {
  const headFile = path.join(repoRoot, ".git", "HEAD");
  if (!existsSync(headFile)) return "";
  try {
    const head = readFileSync(headFile, "utf8").trim();
    if (head.startsWith("ref: ")) {
      const ref = head.slice(5);
      const branch = ref.split("/").pop();
      const shaFile = path.join(repoRoot, ".git", ref);
      if (existsSync(shaFile)) {
        const sha = readFileSync(shaFile, "utf8").trim().slice(0, 7);
        return `${branch} @ ${sha}`;
      }
      return branch;
    }
    return head.slice(0, 7);
  } catch {
    return "";
  }
}

function repoRootFor(packageMainPath) {
  // Walk up from the resolved main until we find a directory containing .git
  let dir = path.dirname(packageMainPath);
  while (dir && dir !== path.dirname(dir)) {
    if (existsSync(path.join(dir, ".git"))) return dir;
    dir = path.dirname(dir);
  }
  return null;
}

function printResolutionBanner() {
  try {
    const sdkPath = require_.resolve("@deepgram/sdk");
    const trPath = require_.resolve("@deepgram/sagemaker");
    process.stderr.write(`SDK:        ${sdkPath}\n`);
    const sdkRoot = repoRootFor(sdkPath);
    if (sdkRoot) {
      process.stderr.write(`            branch: ${gitHead(sdkRoot)}\n`);
    }
    process.stderr.write(`Transport:  ${trPath}\n`);
    const trRoot = repoRootFor(trPath);
    if (trRoot) {
      process.stderr.write(`            branch: ${gitHead(trRoot)}\n`);
    }
  } catch (err) {
    process.stderr.write(`(resolution banner skipped: ${err?.message ?? err})\n`);
  }
}

const PARSE_ARGS_OPTIONS = {
  file: { type: "string", short: "f" },
  reference: { type: "string" },
  connections: { type: "string", short: "c", default: "1" },
  "batch-size": { type: "string", default: "0" },
  "batch-delay": { type: "string", default: "0" },
  loop: { type: "boolean", default: false },
  "no-loop": { type: "boolean", default: false },
  duration: { type: "string", default: "0" },
  region: { type: "string", default: "us-west-2" },
  "max-retries": { type: "string", default: "10" },
  "await-final-results": { type: "string", default: "15" },
  model: { type: "string", default: "nova-3" },
  service: { type: "string", default: "listen.v1" },
  "interim-results": { type: "boolean", default: false },
  "log-level": { type: "string", default: "INFO" },
  "transcripts-dir": { type: "string" },
  "write-reference": { type: "string" },
  help: { type: "boolean", short: "h", default: false },
};

const HELP_TEXT = `Deepgram SDK SageMaker load test (JavaScript)

Usage:
  node loadtest/dg-sdk-loadtest.mjs <endpoint-name> --file FILE [options]

Required:
  <endpoint-name>             SageMaker endpoint name (positional)
  -f, --file FILE             Path to a 16-bit PCM WAV file

Options:
  --reference PATH            Path to a plain-text reference transcript used to compute WER
                              per connection. Defaults to <wav-stem>.txt next to the WAV.
                              Pass '' to disable.
  -c, --connections N         Total simultaneous streaming connections (default: 1)
  --batch-size N              Connections to open per batch (0 = all at once, default: 0)
  --batch-delay S             Seconds to wait between batches (default: 0)
  --loop                      Loop audio file continuously until --duration elapses.
  --no-loop                   Play the file once per stream (default; matches the Salesforce
                              production usage pattern).
  --duration S                Stop after N seconds (0 = run until audio ends or Ctrl+C, default: 0)
  --region REGION             AWS region (default: us-west-2)
  --max-retries N             Max retries per connection on retryable errors (default: 10)
  --await-final-results S     After sending CloseStream, wait up to N seconds for the model
                              to flush remaining transcripts and close. Bump when retries
                              cause large model backlogs (default: 15)
  --model MODEL               Deepgram model (default: nova-3; use flux-general-en for Flux)
  --service NAME              listen.v1 (default, Nova/Nova-3 STT) or listen.v2 (Flux turn-based)
  --interim-results           Enable interim/partial results (listen.v1 only)
  --log-level LEVEL           DEBUG, INFO, WARN, ERROR (default: INFO)
  --transcripts-dir DIR       Directory to write per-connection final transcripts
                              (conn-NNNN.txt) plus summary.csv with WER per connection.
                              Pass '-' to disable.
  --write-reference PATH      After a --connections 1 run, write the captured final
                              transcripts as the reference file at the given path.
  -h, --help                  Show this help and exit
`;

function fail(message, code = 1) {
  process.stderr.write(`ERROR: ${message}\n`);
  process.exit(code);
}

function parseArgvIntoOptions(argv) {
  // node:util.parseArgs doesn't handle a positional alongside option flags
  // gracefully when the positional comes first. Pull it out manually, then
  // hand the rest to parseArgs.
  const positionals = [];
  const flags = [];
  for (const arg of argv) {
    if (arg.startsWith("-")) {
      flags.push(arg);
    } else if (flags.length > 0 && !flags[flags.length - 1].startsWith("--no-") && !["--loop", "--interim-results", "--help", "-h"].includes(flags[flags.length - 1])) {
      // Likely the value for the previous flag — push along.
      flags.push(arg);
    } else {
      positionals.push(arg);
    }
  }
  const { values } = parseArgs({ options: PARSE_ARGS_OPTIONS, args: flags, allowPositionals: false });
  return { values, positionals };
}

function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function printDashboard(connections, startTime) {
  if (connections.length === 0) return;
  let active = 0;
  let errored = 0;
  let transcripts = 0;
  let chunks = 0;
  let retries = 0;
  for (const c of connections) {
    if (c.stats.active) active += 1;
    if (c.stats.errored) errored += 1;
    transcripts += c.stats.transcriptCount;
    chunks += c.stats.chunkCount;
    retries += c.stats.retryCount;
  }
  const elapsed = (performance.now() - startTime) / 1000;
  process.stderr.write(
    `\r[${formatDuration(elapsed)}] Active: ${active}/${connections.length} | ` +
      `Errored: ${errored} | Retries: ${retries} | Transcripts: ${transcripts} | ` +
      `Chunks: ${chunks}    `,
  );
}

async function loadSdkAndTransport() {
  try {
    const [{ DeepgramClient }, transportModule] = await Promise.all([
      import("@deepgram/sdk"),
      import("@deepgram/sagemaker"),
    ]);
    return {
      DeepgramClient,
      createSageMakerTransportFactory: transportModule.createSageMakerTransportFactory,
      SageMakerConfig: transportModule.SageMakerConfig,
    };
  } catch (err) {
    process.stderr.write(
      "ERROR: Could not load @deepgram/sdk or @deepgram/sagemaker.\n" +
        "  Install or link them into this workspace:\n" +
        "    npm install @deepgram/sdk @deepgram/sagemaker\n" +
        "  Or, for local testing against the PR branch:\n" +
        "    npm link @deepgram/sdk\n" +
        "    npm link @deepgram/sagemaker\n",
    );
    process.stderr.write(`  Underlying error: ${err.message}\n`);
    process.exit(1);
  }
}

async function writeReferenceIfRequested(connections, refPath) {
  if (!refPath) return;
  const parts = [];
  for (const c of connections) {
    for (const t of c.stats.finalTranscripts) {
      if (t) parts.push(t);
    }
  }
  const body = parts.join(" ");
  const parent = path.dirname(refPath);
  if (parent && !existsSync(parent)) await mkdir(parent, { recursive: true });
  await writeFile(refPath, body);
  process.stderr.write(
    `\nWrote reference transcript to ${path.resolve(refPath)} (${body.length} chars)\n`,
  );
}

async function writeTranscriptsIfRequested(connections, dir) {
  if (!dir || dir === "-") return null;
  await mkdir(dir, { recursive: true });
  for (const c of connections) {
    const body = c.stats.finalTranscripts.join(" ");
    const name = `conn-${String(c.connectionId).padStart(4, "0")}.txt`;
    await writeFile(path.join(dir, name), body);
  }
  process.stderr.write(
    `\nWrote ${connections.length} per-connection transcript file(s) to ${path.resolve(dir)}\n`,
  );
  return dir;
}

async function loadReferenceText(referenceArg, wavPath) {
  if (referenceArg != null) {
    if (referenceArg === "") return null; // explicit disable
    try {
      return await readFile(referenceArg, "utf8");
    } catch (err) {
      process.stderr.write(
        `WARN: Reference transcript not readable at ${referenceArg}: ${err.message}; skipping WER\n`,
      );
      return null;
    }
  }
  const stem = wavPath.replace(/\.[^.]+$/, "");
  const defaultPath = `${stem}.txt`;
  if (!existsSync(defaultPath)) return null;
  try {
    return await readFile(defaultPath, "utf8");
  } catch {
    return null;
  }
}

function printWerReport(connections, reference, dumpDir) {
  process.stderr.write("\nComputing WER...\n");
  const results = connections.map((c) => {
    const hypothesis = c.stats.finalTranscripts.join(" ");
    if (hypothesis.trim() === "") {
      return { connectionId: c.connectionId, wer: null, note: c.stats.errored ? "errored" : "no transcript" };
    }
    const wer = computeWer(reference, hypothesis);
    if (wer == null) {
      return { connectionId: c.connectionId, wer: null, note: c.stats.errored ? "errored" : "no transcript" };
    }
    return { connectionId: c.connectionId, wer, note: "" };
  });
  results.sort((a, b) => a.connectionId - b.connectionId);

  process.stderr.write("\nWER per connection (vs. reference transcript):\n");
  for (const r of results) {
    if (r.wer == null) {
      process.stderr.write(`  [Conn ${String(r.connectionId).padStart(4, " ")}] -    (${r.note})\n`);
    } else {
      process.stderr.write(`  [Conn ${String(r.connectionId).padStart(4, " ")}] ${(r.wer * 100).toFixed(2)}%\n`);
    }
  }

  const values = results.map((r) => r.wer).filter((v) => v != null).sort((a, b) => a - b);
  if (values.length > 0) {
    const mean = values.reduce((acc, v) => acc + v, 0) / values.length;
    const mid = Math.floor(values.length / 2);
    const median = values.length % 2 === 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
    const p95Idx = Math.max(0, Math.floor(values.length * 0.95) - 1);
    process.stderr.write(
      `\n  Mean WER:   ${(mean * 100).toFixed(2)}%\n` +
        `  Median WER: ${(median * 100).toFixed(2)}%\n` +
        `  Min WER:    ${(values[0] * 100).toFixed(2)}%\n` +
        `  P95 WER:    ${(values[p95Idx] * 100).toFixed(2)}%\n` +
        `  Max WER:    ${(values[values.length - 1] * 100).toFixed(2)}%\n`,
    );
  }

  if (dumpDir) {
    const byId = new Map(results.map((r) => [r.connectionId, r]));
    const lines = ["conn_id,errored,transcripts,chunks,retries,duration_s,wer_pct,note,err_msg"];
    for (const c of connections) {
      const r = byId.get(c.connectionId);
      const wer = r?.wer == null ? "" : (r.wer * 100).toFixed(4);
      const note = (r?.note ?? "").replace(/,/g, ";");
      const errMsg = (c.stats.errorMessages.join(" | ") ?? "")
        .replace(/[\r\n]+/g, " ")
        .replace(/,/g, ";")
        .slice(0, 300);
      lines.push(
        [
          c.connectionId,
          c.stats.errored,
          c.stats.transcriptCount,
          c.stats.chunkCount,
          c.stats.retryCount,
          c.durationSeconds().toFixed(2),
          wer,
          note,
          errMsg,
        ].join(","),
      );
    }
    const csvPath = path.join(dumpDir, "summary.csv");
    return writeFile(csvPath, lines.join("\n") + "\n").then(() => {
      process.stderr.write(`\nWrote per-connection summary to ${path.resolve(csvPath)}\n`);
    });
  }
  return Promise.resolve();
}

function printSummary(connections, wallTime) {
  process.stderr.write("\n\n=== STREAM SUMMARY ===\n");
  let successful = 0;
  let errored = 0;
  let totalTranscripts = 0;
  let totalChunks = 0;
  let totalRetries = 0;
  for (const c of connections) {
    if (c.stats.errored) errored += 1;
    else successful += 1;
    totalTranscripts += c.stats.transcriptCount;
    totalChunks += c.stats.chunkCount;
    totalRetries += c.stats.retryCount;
  }
  process.stderr.write(`Total connections:  ${connections.length}\n`);
  process.stderr.write(`Successful:         ${successful}\n`);
  process.stderr.write(`Errored:            ${errored}\n`);
  process.stderr.write(`Total retries:      ${totalRetries}\n`);
  process.stderr.write(`Total transcripts:  ${totalTranscripts}\n`);
  process.stderr.write(`Total chunks sent:  ${totalChunks}\n`);
  process.stderr.write(`Wall time:          ${wallTime.toFixed(2)}s\n\n`);

  const durations = connections.filter((c) => !c.stats.errored).map((c) => c.durationSeconds()).sort((a, b) => a - b);
  if (durations.length > 0) {
    const mean = durations.reduce((a, v) => a + v, 0) / durations.length;
    process.stderr.write("--- Connection Durations (successful) ---\n");
    process.stderr.write(`Min:    ${durations[0].toFixed(2)}s\n`);
    process.stderr.write(`Median: ${durations[Math.floor(durations.length / 2)].toFixed(2)}s\n`);
    process.stderr.write(`Max:    ${durations[durations.length - 1].toFixed(2)}s\n`);
    process.stderr.write(`Mean:   ${mean.toFixed(2)}s\n`);
  }

  if (errored > 0) {
    process.stderr.write("\n--- Errors ---\n");
    const counts = new Map();
    for (const c of connections) {
      if (!c.stats.errored) continue;
      for (const m of c.stats.errorMessages) {
        const key = (m ?? "").slice(0, 120);
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
    }
    const sorted = [...counts.entries()].sort((a, b) => b[1] - a[1]);
    for (const [key, count] of sorted) {
      process.stderr.write(`  [${count}x] ${key}\n`);
    }
  }
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("-h") || argv.includes("--help") || argv.length === 0) {
    process.stderr.write(HELP_TEXT);
    process.exit(argv.length === 0 ? 2 : 0);
  }

  let parsed;
  try {
    parsed = parseArgvIntoOptions(argv);
  } catch (err) {
    fail(err.message ?? String(err));
    return;
  }
  const { values, positionals } = parsed;
  if (positionals.length === 0) fail("missing positional <endpoint-name>");
  if (positionals.length > 1) fail(`unexpected positionals: ${positionals.slice(1).join(", ")}`);
  if (!values.file) fail("--file is required");
  const endpointName = positionals[0];
  const wavPath = values.file;
  const connections_n = Number(values.connections);
  const batchSize = Number(values["batch-size"]);
  const batchDelay = Number(values["batch-delay"]);
  const duration = Number(values.duration);
  const maxRetries = Number(values["max-retries"]);
  const awaitFinalResultsMs = Number(values["await-final-results"]) * 1000;
  const useLoop = values.loop && !values["no-loop"];
  if (!Number.isFinite(connections_n) || connections_n < 1) fail("--connections must be >= 1");
  if (!existsSync(wavPath)) fail(`WAV file not found: ${wavPath}`);

  const { DeepgramClient, createSageMakerTransportFactory } = await loadSdkAndTransport();

  const wavBuffer = await readFile(wavPath);
  const wavInfo = readWavInfo(wavBuffer);

  // One shared factory + client for the whole process. Each connect()
  // still gets its own underlying SageMakerTransport instance (via the
  // factory's __call__ hook), so stream isolation is preserved -- but
  // they all multiplex over a single AWS HTTP/2 client / connection
  // pool, matching the Python load test's architecture. 400 separate
  // clients hit AWS connection limits and tear streams down ~2s in.
  //
  // Load-test retry tuning: dialed far above the SDK defaults so a
  // single transient AWS-side condition won't end the connection. NOT
  // production defaults.
  const sharedFactory = createSageMakerTransportFactory({
    endpointName,
    region: values.region,
    maxRetries: 100,
    maxBackoffMs: 30_000,
    retryBudgetMs: 3_600_000,
  });
  const sharedClient = new DeepgramClient({
    apiKey: "unused",
    transportFactory: sharedFactory,
  });

  const effectiveBatchSize = batchSize > 0 ? batchSize : connections_n;
  process.stderr.write("=== Deepgram SDK SageMaker Load Test (JavaScript) ===\n");
  process.stderr.write(`Endpoint:       ${endpointName}\n`);
  process.stderr.write(`WAV file:       ${wavPath}\n`);
  process.stderr.write(`Connections:    ${connections_n}\n`);
  process.stderr.write(`Batch size:     ${effectiveBatchSize}\n`);
  process.stderr.write(`Batch delay:    ${batchDelay}s\n`);
  process.stderr.write(`Region:         ${values.region}\n`);
  process.stderr.write(`Model:          ${values.model}\n`);
  process.stderr.write(`Loop:           ${useLoop}\n`);
  process.stderr.write(`Max retries:    ${maxRetries}\n`);
  printResolutionBanner();
  process.stderr.write("\n");
  process.stderr.write(
    `WAV: ${path.basename(wavPath)} | ${wavInfo.sampleRate} Hz | ${wavInfo.channels}ch | ${wavInfo.durationSeconds.toFixed(2)}s\n`,
  );

  const connectOptions = buildConnectOptions({
    service: values.service,
    model: values.model,
    sampleRate: wavInfo.sampleRate,
    channels: wavInfo.channels,
    interimResults: values["interim-results"],
  });

  const connectionsList = [];

  // Default loop-stop policy: when --duration is unset and --loop is on,
  // stop everyone once every connection has done one full pass.
  const stopOnAllFirstPasses = useLoop && duration === 0;
  let firstPassCount = 0;
  const onFirstPassComplete = () => {
    firstPassCount += 1;
    if (stopOnAllFirstPasses && firstPassCount >= connections_n) {
      process.stderr.write(
        `\nAll ${connections_n} connection(s) completed one full pass. Stopping...\n`,
      );
      for (const c of connectionsList) c.stop();
    }
  };

  const testStart = performance.now();
  const numBatches = Math.ceil(connections_n / effectiveBatchSize);
  process.stderr.write(
    `Opening ${connections_n} connection(s) in ${numBatches} batch(es) ` +
      "(one SageMaker client per connection)...\n",
  );

  const dashboardInterval = setInterval(() => printDashboard(connectionsList, testStart), 2000);

  let durationTimer = null;
  if (duration > 0) {
    durationTimer = setTimeout(() => {
      process.stderr.write(`\nDuration limit reached (${duration}s). Stopping...\n`);
      for (const c of connectionsList) c.stop();
    }, duration * 1000);
  }

  const runs = [];
  for (let batchStart = 0; batchStart < connections_n; batchStart += effectiveBatchSize) {
    const batchEnd = Math.min(batchStart + effectiveBatchSize, connections_n);
    const batchNum = Math.floor(batchStart / effectiveBatchSize) + 1;
    process.stderr.write(
      `Opening batch ${batchNum}/${numBatches}: connections ${batchStart + 1}-${batchEnd}...\n`,
    );

    for (let i = batchStart; i < batchEnd; i++) {
      const conn = new SdkStreamingConnection({
        connectionId: i + 1,
        sdkClient: sharedClient,
        service: values.service,
        wavBuffer,
        wavInfo,
        connectOptions,
        loop: useLoop,
        maxRetries,
        awaitFinalResultsMs,
        onFirstPassComplete,
      });
      connectionsList.push(conn);
      runs.push(conn.run().catch((err) => {
        process.stderr.write(`\n[Conn ${conn.connectionId}] fatal: ${err?.message ?? err}\n`);
      }));
    }

    if (batchEnd < connections_n && batchDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, batchDelay * 1000));
    }
  }

  process.stderr.write(`All ${connections_n} connection(s) launched. Streaming...\n\n`);

  await Promise.all(runs);
  clearInterval(dashboardInterval);
  if (durationTimer) clearTimeout(durationTimer);

  const wallTime = (performance.now() - testStart) / 1000;
  printSummary(connectionsList, wallTime);

  // Dump transcripts / reference BEFORE WER so artifacts are on disk even
  // if WER is skipped.
  await writeReferenceIfRequested(connectionsList, values["write-reference"]);
  const dumpDir = await writeTranscriptsIfRequested(connectionsList, values["transcripts-dir"]);

  const reference = await loadReferenceText(values.reference, wavPath);
  if (reference != null) {
    await printWerReport(connectionsList, reference, dumpDir);
  }

  // Diagnostic probe: enumerate handles/requests keeping the event loop
  // alive after all test work is complete. With this we can see exactly
  // what the CLI needs to dispose of before returning.
  if (process.env.DG_PROBE_HANDLES) {
    const handles = process._getActiveHandles?.() ?? [];
    const requests = process._getActiveRequests?.() ?? [];
    const summarize = (h) => {
      const name = h?.constructor?.name ?? typeof h;
      const extra = [];
      if (h?._peername) extra.push(`peer=${h._peername.address}:${h._peername.port}`);
      if (h?.remoteAddress && h?.remotePort) extra.push(`peer=${h.remoteAddress}:${h.remotePort}`);
      if (h?._handle?.fd !== undefined) extra.push(`fd=${h._handle.fd}`);
      if (typeof h?.readable === "boolean") extra.push(`r=${h.readable}`);
      if (typeof h?.writable === "boolean") extra.push(`w=${h.writable}`);
      return `${name}${extra.length ? " (" + extra.join(", ") + ")" : ""}`;
    };
    const tally = (arr) => {
      const counts = new Map();
      for (const item of arr) {
        const key = summarize(item).replace(/peer=[^,)]+/, "peer=...");
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
      return [...counts.entries()].sort((a, b) => b[1] - a[1]);
    };
    process.stderr.write(`\n=== Open handles after main() (${handles.length}) ===\n`);
    for (const [k, v] of tally(handles)) {
      process.stderr.write(`  [${v}x] ${k}\n`);
    }
    process.stderr.write(`=== Open requests after main() (${requests.length}) ===\n`);
    for (const [k, v] of tally(requests)) {
      process.stderr.write(`  [${v}x] ${k}\n`);
    }
  }
}

main().catch((err) => {
  process.stderr.write(`\nFATAL: ${err?.stack ?? err?.message ?? err}\n`);
  process.exit(1);
});
