#!/usr/bin/env node
/**
 * SageMaker TTS (Aura speak.v1) load test for the JS SDK.
 *
 * Mirrors the STT load test's CLI/config shape, but the per-connection
 * flow is text-in, audio-out: send N sentences, send Flush, capture audio
 * chunks until the model emits Flushed (or a timeout fires).
 *
 *   node loadtest/dg-sdk-tts-loadtest.mjs <endpoint-name> \
 *       --connections 400 --region us-east-2 \
 *       --transcripts-dir /tmp/tts-loadtest
 *
 * Success criterion is per-connection: did we receive at least one audio
 * chunk and a Flushed event before the await-flush timeout?
 */

import { mkdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";

import { DeepgramClient } from "@deepgram/sdk";
import { createSageMakerTransportFactory } from "@deepgram/sagemaker";

const DEFAULT_SENTENCES = [
  "Hello, this is a text-to-speech load test running on Amazon SageMaker.",
  "The Deepgram model is generating audio from text in real time.",
  "This audio is being streamed back through the JavaScript SDK transport layer.",
];

const PARSE_ARGS_OPTIONS = {
  connections: { type: "string", short: "c", default: "1" },
  region: { type: "string", default: "us-east-2" },
  model: { type: "string", default: "aura-2-atlas-en" },
  encoding: { type: "string", default: "linear16" },
  "await-flush": { type: "string", default: "30" },
  "max-retries": { type: "string", default: "10" },
  "transcripts-dir": { type: "string" },
  help: { type: "boolean", short: "h", default: false },
};

function parseArgvIntoOptions(argv) {
  const positionals = [];
  const flags = [];
  for (const arg of argv) {
    if (arg.startsWith("-")) {
      flags.push(arg);
    } else if (
      flags.length > 0 &&
      !["--help", "-h"].includes(flags[flags.length - 1])
    ) {
      flags.push(arg);
    } else {
      positionals.push(arg);
    }
  }
  const { values } = parseArgs({
    options: PARSE_ARGS_OPTIONS,
    args: flags,
    allowPositionals: false,
  });
  return { values, positionals };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class TtsConnection {
  constructor({ connectionId, sdkClient, model, encoding, awaitFlushMs, maxRetries }) {
    this.connectionId = connectionId;
    this._sdkClient = sdkClient;
    this._model = model;
    this._encoding = encoding;
    this._awaitFlushMs = awaitFlushMs;
    this._maxRetries = maxRetries;
    this.stats = {
      active: false,
      errored: false,
      audioChunks: 0,
      audioBytes: 0,
      flushed: false,
      retryCount: 0,
      startTime: 0,
      endTime: 0,
      errorMessages: [],
    };
  }

  durationSeconds() {
    if (this.stats.startTime === 0) return 0;
    const end = this.stats.endTime || performance.now();
    return (end - this.stats.startTime) / 1000;
  }

  async run() {
    this.stats.startTime = performance.now();
    this.stats.active = true;
    let attempt = 0;

    while (true) {
      try {
        const socket = await this._sdkClient.speak.v1.createConnection({
          model: this._model,
          encoding: this._encoding,
        });

        let closed = false;
        let closeResolve;
        const closePromise = new Promise((resolve) => {
          closeResolve = resolve;
        });
        const errors = [];

        socket.on("message", (message) => {
          if (Buffer.isBuffer(message) || message instanceof Uint8Array) {
            this.stats.audioChunks += 1;
            this.stats.audioBytes += message.length ?? message.byteLength;
            return;
          }
          if (message && typeof message === "object" && message.type === "Flushed") {
            this.stats.flushed = true;
            return;
          }
        });
        socket.on("error", (err) => {
          errors.push(err?.message ?? String(err));
          if (!closed) {
            closed = true;
            closeResolve();
          }
        });
        socket.on("close", () => {
          if (!closed) {
            closed = true;
            closeResolve();
          }
        });

        socket.connect();
        await socket.waitForOpen();

        for (const sentence of DEFAULT_SENTENCES) {
          socket.sendText({ type: "Speak", text: sentence });
        }
        socket.sendFlush({ type: "Flush" });

        const deadline = performance.now() + this._awaitFlushMs;
        while (!this.stats.flushed && !closed && performance.now() < deadline) {
          await Promise.race([closePromise, sleep(250)]);
        }

        try {
          socket.sendClose({ type: "Close" });
        } catch {
          // best effort
        }
        await Promise.race([closePromise, sleep(2000)]);
        try {
          socket.close();
        } catch {
          // best effort
        }

        if (errors.length > 0 && this.stats.audioChunks === 0) {
          throw new Error(errors.join("; "));
        }

        this.stats.endTime = performance.now();
        this.stats.active = false;
        return;
      } catch (err) {
        const msg = err?.message ?? String(err);
        if (attempt < this._maxRetries) {
          attempt += 1;
          this.stats.retryCount += 1;
          const backoffMs = Math.min(1000 * 2 ** (attempt - 1), 30_000);
          process.stderr.write(
            `\n[Conn ${this.connectionId}] retry ${attempt}/${this._maxRetries} in ${backoffMs}ms: ${msg.slice(0, 100)}\n`,
          );
          await sleep(backoffMs);
          continue;
        }
        this.stats.active = false;
        this.stats.errored = true;
        this.stats.errorMessages.push(msg);
        this.stats.endTime = performance.now();
        return;
      }
    }
  }
}

function printDashboard(conns, startTime) {
  let active = 0;
  let errored = 0;
  let totalChunks = 0;
  let flushed = 0;
  for (const c of conns) {
    if (c.stats.active) active += 1;
    if (c.stats.errored) errored += 1;
    totalChunks += c.stats.audioChunks;
    if (c.stats.flushed) flushed += 1;
  }
  const elapsed = (performance.now() - startTime) / 1000;
  const h = Math.floor(elapsed / 3600);
  const m = Math.floor((elapsed % 3600) / 60);
  const s = Math.floor(elapsed % 60);
  process.stderr.write(
    `\r[${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}] ` +
      `Active: ${active}/${conns.length} | Errored: ${errored} | Flushed: ${flushed} | AudioChunks: ${totalChunks}    `,
  );
}

async function main() {
  const argv = process.argv.slice(2);
  const { values, positionals } = parseArgvIntoOptions(argv);
  if (values.help || positionals.length === 0) {
    process.stderr.write(
      "Usage: node loadtest/dg-sdk-tts-loadtest.mjs <endpoint-name> [--connections N] [--region R] [--model M] [--encoding E] [--await-flush S] [--transcripts-dir DIR]\n",
    );
    process.exit(positionals.length === 0 ? 2 : 0);
  }

  const endpointName = positionals[0];
  const connectionsN = Number(values.connections);
  const awaitFlushMs = Number(values["await-flush"]) * 1000;
  const maxRetries = Number(values["max-retries"]);

  const factory = createSageMakerTransportFactory({
    endpointName,
    region: values.region,
    maxRetries: 100,
    maxBackoffMs: 30_000,
    retryBudgetMs: 3_600_000,
  });
  const sdkClient = new DeepgramClient({ apiKey: "unused", transportFactory: factory });

  process.stderr.write("=== Deepgram SDK SageMaker TTS Load Test (JavaScript) ===\n");
  process.stderr.write(`Endpoint:       ${endpointName}\n`);
  process.stderr.write(`Connections:    ${connectionsN}\n`);
  process.stderr.write(`Region:         ${values.region}\n`);
  process.stderr.write(`Model:          ${values.model}\n`);
  process.stderr.write(`Encoding:       ${values.encoding}\n`);
  process.stderr.write(`Max retries:    ${maxRetries}\n\n`);

  const conns = [];
  for (let i = 0; i < connectionsN; i++) {
    conns.push(
      new TtsConnection({
        connectionId: i + 1,
        sdkClient,
        model: values.model,
        encoding: values.encoding,
        awaitFlushMs,
        maxRetries,
      }),
    );
  }

  const start = performance.now();
  const dashboard = setInterval(() => printDashboard(conns, start), 2000);
  await Promise.all(conns.map((c) => c.run()));
  clearInterval(dashboard);

  const wallTime = (performance.now() - start) / 1000;
  process.stderr.write("\n\n=== TTS SUMMARY ===\n");
  const successful = conns.filter((c) => !c.stats.errored).length;
  const errored = conns.filter((c) => c.stats.errored).length;
  const flushed = conns.filter((c) => c.stats.flushed).length;
  const totalChunks = conns.reduce((s, c) => s + c.stats.audioChunks, 0);
  const totalBytes = conns.reduce((s, c) => s + c.stats.audioBytes, 0);
  const totalRetries = conns.reduce((s, c) => s + c.stats.retryCount, 0);
  const zeroAudio = conns.filter((c) => !c.stats.errored && c.stats.audioChunks === 0).length;

  process.stderr.write(`Total connections:  ${connectionsN}\n`);
  process.stderr.write(`Successful:         ${successful}\n`);
  process.stderr.write(`Errored:            ${errored}\n`);
  process.stderr.write(`Flushed (got Flushed event): ${flushed}\n`);
  process.stderr.write(`Got audio (>=1 chunk): ${connectionsN - zeroAudio - errored}\n`);
  process.stderr.write(`Total audio chunks: ${totalChunks}\n`);
  process.stderr.write(`Total audio bytes:  ${totalBytes}\n`);
  process.stderr.write(`Total retries:      ${totalRetries}\n`);
  process.stderr.write(`Wall time:          ${wallTime.toFixed(2)}s\n`);

  const dumpDir = values["transcripts-dir"];
  if (dumpDir && dumpDir !== "-") {
    await mkdir(dumpDir, { recursive: true });
    const lines = ["conn_id,errored,audio_chunks,audio_bytes,flushed,retries,duration_s,err_msg"];
    for (const c of conns) {
      const errMsg = (c.stats.errorMessages.join(" | ") ?? "")
        .replace(/[\r\n]+/g, " ")
        .replace(/,/g, ";")
        .slice(0, 300);
      lines.push(
        [
          c.connectionId,
          c.stats.errored,
          c.stats.audioChunks,
          c.stats.audioBytes,
          c.stats.flushed,
          c.stats.retryCount,
          c.durationSeconds().toFixed(2),
          errMsg,
        ].join(","),
      );
    }
    const csvPath = path.join(dumpDir, "tts-summary.csv");
    await writeFile(csvPath, lines.join("\n") + "\n");
    process.stderr.write(`Wrote per-connection summary to ${path.resolve(csvPath)}\n`);
  }

  if (errored > 0) {
    process.stderr.write("\n--- Errors ---\n");
    const counts = new Map();
    for (const c of conns) {
      if (!c.stats.errored) continue;
      for (const m of c.stats.errorMessages) {
        const key = (m ?? "").slice(0, 120);
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
    }
    for (const [key, count] of [...counts.entries()].sort((a, b) => b[1] - a[1])) {
      process.stderr.write(`  [${count}x] ${key}\n`);
    }
  }
}

main().catch((err) => {
  process.stderr.write(`\nFATAL: ${err?.stack ?? err?.message ?? err}\n`);
  process.exit(1);
});
