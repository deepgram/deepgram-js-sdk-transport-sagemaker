import process from "node:process";

import {
  DEFAULT_AUDIO_FILE,
  createSageMakerClient,
  ensureAudioFile,
  onceTransportEvent,
  streamWavRealtime,
  sleep,
} from "./_common.mjs";

const ENDPOINT = process.env.SAGEMAKER_ENDPOINT ?? "deepgram-flux";
const REGION = process.env.AWS_REGION ?? "us-west-2";
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
    console.log("Flux transcription via SageMaker (V2 Listen)");
    console.log(`Endpoint: ${ENDPOINT}`);
    console.log(`Region:   ${REGION}`);
    console.log();

    const socket = await client.listen.v2.createConnection({
      model: "flux-general-en",
    });

    socket.on("message", (message) => {
      if (message.type !== "TurnInfo") {
        return;
      }

      if (message.transcript) {
        console.log(`[${message.event}] turn=${message.turn_index}  ${message.transcript}`);
      }
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
