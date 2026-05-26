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

const ENDPOINT = process.env.SAGEMAKER_ENDPOINT ?? "deepgram-tts";
const REGION = process.env.AWS_REGION ?? "us-west-2";
const OUTPUT_FILE = process.env.OUTPUT_FILE ?? path.join(REPO_ROOT, "tts_output.wav");

async function main() {
  const { client, transportFactory } = await createSageMakerClient({
    endpointName: ENDPOINT,
    region: REGION,
  });

  try {
    console.log("Text-to-Speech via SageMaker");
    console.log(`Endpoint: ${ENDPOINT}`);
    console.log(`Region:   ${REGION}`);
    console.log(`Output:   ${OUTPUT_FILE}`);
    console.log();

    const socket = await client.speak.v1.createConnection({
      model: "aura-2-atlas-en",
      encoding: "linear16",
    });

    const audioChunks = [];
    let flushed = false;

    socket.on("message", (message) => {
      const audio = bufferFromMessage(message);
      if (audio) {
        audioChunks.push(audio);
        const count = audioChunks.length;
        if (count <= 5 || count % 50 === 1) {
          console.log(`Received audio chunk #${count} (${audio.length} bytes)`);
        }
        return;
      }

      if (message?.type === "Flushed") {
        flushed = true;
        console.log("Flushed - all queued text has been converted");
      }
    });

    socket.on("error", (error) => {
      if (!flushed) {
        console.error("Error:", error);
      }
    });

    socket.connect();
    await socket.waitForOpen();

    const sentences = [
      "Hello, this is a text-to-speech test running on Amazon SageMaker.",
      "The Deepgram model is generating audio from text in real time.",
      "This audio is being streamed back through the JavaScript SDK transport layer.",
    ];

    for (const sentence of sentences) {
      console.log(`Sending: \"${sentence}\"`);
      socket.sendText({ type: "Speak", text: sentence });
    }

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
