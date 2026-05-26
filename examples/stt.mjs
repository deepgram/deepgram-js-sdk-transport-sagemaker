import process from "node:process";

import {
  DEFAULT_AUDIO_FILE,
  createSageMakerClient,
  ensureAudioFile,
  onceTransportEvent,
  streamWavRealtime,
  sleep,
} from "./_common.mjs";

const ENDPOINT = process.env.SAGEMAKER_ENDPOINT ?? "deepgram-nova-3";
const REGION = process.env.AWS_REGION ?? "us-west-2";
const MODEL = process.env.DEEPGRAM_MODEL ?? "nova-3";
const AUDIO_FILE = process.env.AUDIO_FILE ?? DEFAULT_AUDIO_FILE;

async function main() {
  if (!(await ensureAudioFile(AUDIO_FILE))) {
    return;
  }

  const { client, transportFactory } = await createSageMakerClient({
    endpointName: ENDPOINT,
    region: REGION,
  });

  try {
    console.log("Streaming STT via SageMaker (V1 Listen)");
    console.log(`Endpoint: ${ENDPOINT}`);
    console.log(`Model:    ${MODEL}`);
    console.log(`Region:   ${REGION}`);
    console.log();

    const socket = await client.listen.v1.createConnection({
      model: MODEL,
      interim_results: "true",
    });

    socket.on("open", () => {
      console.log("Connection opened");
    });

    socket.on("message", (message) => {
      if (message.type !== "Results") {
        return;
      }

      const transcript = message.channel?.alternatives?.[0]?.transcript;
      if (!transcript) {
        return;
      }

      console.log(`${message.is_final ? "[final]  " : "[interim]"} ${transcript}`);
    });

    socket.on("error", (error) => {
      console.error("Error:", error);
    });

    socket.on("close", (event) => {
      console.log(`Closed (code: ${event.code ?? 1000})`);
    });

    socket.connect();
    await socket.waitForOpen();

    await streamWavRealtime({
      audioFilePath: AUDIO_FILE,
      onChunk: (chunk) => {
        socket.sendMedia(chunk);
      },
    });

    socket.sendCloseStream({ type: "CloseStream" });
    await Promise.race([onceTransportEvent(socket, "close"), sleep(5_000)]);
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
