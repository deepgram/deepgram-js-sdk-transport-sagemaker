import process from "node:process";

import {
  createSageMakerClient,
  loadMicModule,
  onceTransportEvent,
  sleep,
} from "./_common.mjs";

const ENDPOINT = process.env.SAGEMAKER_ENDPOINT ?? "deepgram-nova-3";
const REGION = process.env.AWS_REGION ?? "us-west-2";
const MODEL = process.env.DEEPGRAM_MODEL ?? "nova-3";
const SAMPLE_RATE = 16_000;
const CHANNELS = 1;
const CHUNK_SIZE = 8_192;

async function main() {
  const mic = await loadMicModule();
  const { client, transportFactory } = await createSageMakerClient({
    endpointName: ENDPOINT,
    region: REGION,
  });

  try {
    console.log("Live Microphone Transcription via SageMaker");
    console.log(`Endpoint: ${ENDPOINT}`);
    console.log(`Model:    ${MODEL}`);
    console.log(`Region:   ${REGION}`);
    console.log(`Audio:    ${SAMPLE_RATE} Hz, 16-bit, mono`);
    console.log();

    const socket = await client.listen.v1.createConnection({
      model: MODEL,
      interim_results: "true",
      encoding: "linear16",
      sample_rate: String(SAMPLE_RATE),
    });

    let running = true;

    socket.on("message", (message) => {
      if (message.type !== "Results") {
        return;
      }

      const transcript = message.channel?.alternatives?.[0]?.transcript;
      if (!transcript) {
        return;
      }

      if (message.is_final) {
        process.stdout.write(`\u001b[2K\r${transcript}\n`);
      } else {
        process.stdout.write(`\u001b[2K\r  ... ${transcript}`);
      }
    });

    socket.on("error", (error) => {
      console.error("\nError:", error);
      running = false;
    });

    socket.connect();
    await socket.waitForOpen();

    console.log("Listening... speak into your microphone. Press Ctrl+C to stop.\n");

    const micInstance = mic({
      rate: String(SAMPLE_RATE),
      channels: String(CHANNELS),
      bitwidth: "16",
      encoding: "signed-integer",
      endian: "little",
      fileType: "raw",
      device: process.env.MIC_DEVICE,
    });
    const micStream = micInstance.getAudioStream();

    micStream.on("data", (chunk) => {
      if (running) {
        socket.sendMedia(chunk);
      }
    });

    micStream.on("error", (error) => {
      console.error("\nMicrophone error:", error);
      running = false;
    });

    process.on("SIGINT", () => {
      console.log("\nStopping...");
      running = false;
      micInstance.stop();
    });

    micInstance.start();

    while (running) {
      await sleep(100);
    }

    socket.sendCloseStream({ type: "CloseStream" });
    await Promise.race([onceTransportEvent(socket, "close"), sleep(3_000)]);
    socket.close();
    console.log("Done.");
  } finally {
    transportFactory.destroy();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
