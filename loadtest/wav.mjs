/**
 * Minimal RIFF/WAVE parser — just enough to read the canonical 16-bit PCM
 * header so the load test can pace audio in real time without an extra
 * runtime dep. Returns the data offset so callers can slice raw PCM frames
 * directly out of the file buffer.
 */

export function readWavInfo(buffer) {
  if (buffer.length < 44) {
    throw new Error("WAV file too small to be valid (< 44 bytes)");
  }
  if (buffer.toString("ascii", 0, 4) !== "RIFF") {
    throw new Error("Not a RIFF file");
  }
  if (buffer.toString("ascii", 8, 12) !== "WAVE") {
    throw new Error("Not a WAVE file");
  }

  // Walk subchunks from offset 12 until we find "fmt " and "data".
  let offset = 12;
  let fmt = null;
  let dataOffset = -1;
  let dataSize = 0;
  while (offset + 8 <= buffer.length) {
    const id = buffer.toString("ascii", offset, offset + 4);
    const size = buffer.readUInt32LE(offset + 4);
    if (id === "fmt ") {
      const audioFormat = buffer.readUInt16LE(offset + 8);
      const channels = buffer.readUInt16LE(offset + 10);
      const sampleRate = buffer.readUInt32LE(offset + 12);
      const bitsPerSample = buffer.readUInt16LE(offset + 22);
      fmt = { audioFormat, channels, sampleRate, bitsPerSample };
    } else if (id === "data") {
      dataOffset = offset + 8;
      dataSize = size;
      break;
    }
    offset += 8 + size + (size % 2); // chunks are word-aligned
  }
  if (fmt == null) throw new Error("WAV missing fmt chunk");
  if (dataOffset < 0) throw new Error("WAV missing data chunk");

  const bytesPerFrame = (fmt.bitsPerSample / 8) * fmt.channels;
  const frames = Math.floor(dataSize / bytesPerFrame);
  const durationSeconds = frames / fmt.sampleRate;

  return {
    audioFormat: fmt.audioFormat,
    channels: fmt.channels,
    sampleRate: fmt.sampleRate,
    bitsPerSample: fmt.bitsPerSample,
    bytesPerFrame,
    dataOffset,
    dataSize,
    frames,
    durationSeconds,
  };
}
