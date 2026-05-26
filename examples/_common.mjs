import { access, readFile, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

export const EXAMPLES_DIR = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(EXAMPLES_DIR, "..");
export const DEFAULT_AUDIO_FILE = path.join(REPO_ROOT, "spacewalk.wav");
export const DEFAULT_CHUNK_SIZE = 8192;

export function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function loadSdkAndTransport() {
  try {
    const [{ DeepgramClient }, transportModule] = await Promise.all([
      import("@deepgram/sdk"),
      import("../dist/index.js"),
    ]);

    return {
      DeepgramClient,
      createSageMakerTransportFactory:
        transportModule.createSageMakerTransportFactory ?? transportModule.SageMakerTransportFactory,
    };
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ERR_MODULE_NOT_FOUND") {
      console.error("Missing required dependency. Install a Deepgram JS SDK build with transportFactory support:");
      console.error("  npm install @deepgram/sdk");
      throw error;
    }

    throw error;
  }
}

export async function ensureAudioFile(audioFilePath) {
  try {
    await access(audioFilePath);
  } catch {
    console.error(`Audio file not found: ${audioFilePath}`);
    console.error("Download from: https://dpgr.am/spacewalk.wav");
    process.exitCode = 1;
    return false;
  }

  return true;
}

export async function getWavPacing(audioFilePath, chunkSize = DEFAULT_CHUNK_SIZE) {
  const audio = await readFile(audioFilePath);
  const sampleRate = audio.readUInt32LE(24);
  const blockAlign = audio.readUInt16LE(32);
  const framesPerChunk = chunkSize / blockAlign;
  const sleepMs = (framesPerChunk / sampleRate) * 1000;

  return {
    audio,
    sampleRate,
    blockAlign,
    chunkSize,
    sleepMs,
  };
}

export async function streamWavRealtime({ audioFilePath, onChunk, chunkSize = DEFAULT_CHUNK_SIZE }) {
  const pacing = await getWavPacing(audioFilePath, chunkSize);

  console.log(
    `Streaming WAV: ${pacing.sampleRate} Hz, block align ${pacing.blockAlign}, pacing ${Math.round(pacing.sleepMs)} ms per chunk`,
  );
  console.log();

  for (let offset = 0; offset < pacing.audio.length; offset += chunkSize) {
    const chunk = pacing.audio.subarray(offset, offset + chunkSize);
    onChunk(chunk);
    await sleep(pacing.sleepMs);
  }

  return pacing;
}

export function onceTransportEvent(connection, eventName) {
  return new Promise((resolve) => {
    const eventTarget = connection?.socket;

    if (!eventTarget || typeof eventTarget.addEventListener !== "function") {
      resolve();
      return;
    }

    eventTarget.addEventListener(eventName, resolve, { once: true });
  });
}

export function bufferFromMessage(data) {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (data instanceof Uint8Array) {
    return Buffer.from(data);
  }

  if (data instanceof ArrayBuffer) {
    return Buffer.from(data);
  }

  return null;
}

export async function writePcmWav(outputFilePath, pcmChunks, { sampleRate = 24000, channels = 1, bitsPerSample = 16 } = {}) {
  const pcm = Buffer.concat(pcmChunks);
  const blockAlign = (channels * bitsPerSample) / 8;
  const byteRate = sampleRate * blockAlign;
  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);

  await writeFile(outputFilePath, Buffer.concat([header, pcm]));
}

export async function playAudioFileIfPossible(outputFilePath) {
  const command =
    process.platform === "darwin"
      ? "afplay"
      : process.platform === "linux"
        ? "aplay"
        : null;

  if (!command) {
    console.log(`Audio saved to ${outputFilePath}`);
    console.log("Automatic playback is only configured for macOS and Linux examples.");
    return;
  }

  await new Promise((resolve) => {
    const child = spawn(command, [outputFilePath], {
      stdio: "inherit",
    });

    child.on("error", () => {
      console.log(`Audio saved to ${outputFilePath}`);
      console.log(`Could not launch ${command}; play the file manually.`);
      resolve();
    });

    child.on("close", () => {
      resolve();
    });
  });
}

export async function loadMicModule() {
  try {
    const micModule = await import("mic");
    return micModule.default ?? micModule;
  } catch (error) {
    console.error("The live microphone examples require the optional 'mic' package.");
    console.error("Install it with:");
    console.error("  npm install mic");
    throw error;
  }
}

export function createSageMakerClient({ endpointName, region }) {
  return loadSdkAndTransport().then(({ DeepgramClient, createSageMakerTransportFactory }) => {
    const transportFactory = createSageMakerTransportFactory({
      endpointName,
      region,
    });

    const client = new DeepgramClient({
      apiKey: "unused",
      transportFactory,
    });

    return { client, transportFactory };
  });
}
