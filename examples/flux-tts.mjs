import path from "node:path";
import process from "node:process";

import {
  REPO_ROOT,
  bufferFromMessage,
  createSageMakerClient,
  onceTransportEvent,
  playAudioFileIfPossible,
  sleep,
  writePcmWav,
} from "./_common.mjs";

const ENDPOINT = process.env.SAGEMAKER_ENDPOINT ?? "deepgram-flux-tts";
const REGION = process.env.AWS_REGION ?? "us-west-2";
const MODEL = process.env.DEEPGRAM_MODEL ?? "flux-haley-en";
const OUTPUT_FILE = process.env.OUTPUT_FILE ?? path.join(REPO_ROOT, "flux_tts_output.wav");

async function main() {
  const { client, transportFactory } = await createSageMakerClient({
    endpointName: ENDPOINT,
    region: REGION,
  });

  try {
    console.log("Flux Text-to-Speech via SageMaker (V2 Speak)");
    console.log(`Endpoint: ${ENDPOINT}`);
    console.log(`Model:    ${MODEL}`);
    console.log(`Region:   ${REGION}`);
    console.log(`Output:   ${OUTPUT_FILE}`);
    console.log();

    const socket = await client.speak.v2.createConnection({
      model: MODEL,
      encoding: "linear16",
    });
    const audioChunks = [];
    let completed = false;

    socket.on("message", (message) => {
      const audio = bufferFromMessage(message);
      if (audio) {
        audioChunks.push(audio);
        console.log(`Received audio chunk #${audioChunks.length} (${audio.length} bytes)`);
        return;
      }

      if (message?.type === "SpeechMetadata") {
        completed = true;
        console.log("Speech complete");
      }
    });

    socket.on("error", (error) => {
      if (!completed) {
        console.error("Error:", error);
      }
    });

    socket.connect();
    await socket.waitForOpen();

    socket.sendSpeak({
      type: "Speak",
      text: "Hello, this is Flux text to speech running on Amazon SageMaker.",
    });
    socket.sendFlush({ type: "Flush" });
    console.log("Waiting for audio...");

    await sleep(10_000);
    socket.sendClose({ type: "Close" });
    await Promise.race([onceTransportEvent(socket, "close"), sleep(2_000)]);
    socket.close();

    console.log();
    console.log(`Total audio chunks: ${audioChunks.length}`);
    console.log(`Total audio bytes: ${audioChunks.reduce((sum, chunk) => sum + chunk.length, 0)}`);

    if (audioChunks.length > 0) {
      await writePcmWav(OUTPUT_FILE, audioChunks, {
        sampleRate: 24_000,
        channels: 1,
        bitsPerSample: 16,
      });
      console.log(`Audio saved to ${OUTPUT_FILE}`);
      console.log("Playing audio...");
      await playAudioFileIfPossible(OUTPUT_FILE);
    }

    console.log("Done.");
  } finally {
    transportFactory.destroy();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
