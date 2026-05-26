# SageMaker load test (JavaScript)

End-to-end load harness that streams a known WAV across N concurrent
connections through the Deepgram JS SDK + the SageMaker transport, and
reports per-connection WER against a reference transcript. Validates that
burst-load fixes to the transport stay within 1 percentage point of the
single-concurrency baseline WER.

Mirrors the Java reference harness in
`deepgram/customer-investigations` flag-for-flag so numbers compare
directly across languages.

## Install / link

The harness dynamically imports `@deepgram/sdk` and `@deepgram/sagemaker`,
so both must be resolvable from this directory. For local testing against
in-progress branches:

```bash
# Build the JS SDK on the PR branch and register it globally
cd /path/to/deepgram-js-sdk
pnpm install && pnpm build && npm link

# Build the transport (this repo)
cd /path/to/deepgram-js-sdk-transport-sagemaker
npm install && npm run build
npm link @deepgram/sdk
# Also self-link so the harness can resolve @deepgram/sagemaker
npm link
npm link @deepgram/sagemaker
```

For a regular install:

```bash
npm install @deepgram/sdk @deepgram/sagemaker
```

## AWS credentials

The transport resolves credentials via the AWS SDK's default chain (env
vars, shared credentials file, IAM role). Use whatever you'd normally use
for the AWS dev account, e.g.:

```bash
export AWS_SHARED_CREDENTIALS_FILE=$HOME/.aws/creds.dev
```

## Generate a reference transcript

The WER pass needs a ground-truth transcript to compare each connection
against. If you don't already have one, run the harness at
`--connections 1` and capture what the model emits:

```bash
node loadtest/dg-sdk-loadtest.mjs <endpoint-name> \
    --file ./english.wav \
    --connections 1 \
    --region us-east-2 \
    --write-reference ./english.txt
```

Subsequent high-concurrency runs will measure WER against that file.

## Run the burst test

Matches the Java single-process burst configuration:

```bash
node loadtest/dg-sdk-loadtest.mjs <endpoint-name> \
    --file ./english.wav \
    --reference ./english.txt \
    --connections 400 \
    --no-loop \
    --region us-east-2 \
    --transcripts-dir /tmp/proc-01-transcripts
```

Output: dashboard line every 2 s, then a summary table, per-connection
`conn-NNNN.txt` files in `--transcripts-dir`, a WER report per connection,
and `summary.csv` with `conn_id, errored, transcripts, chunks, duration_s,
wer_pct, note` columns.

## Sharded variant

For the 1000-connection / 10-instance sharded configuration, spawn N
parallel Node processes, each with its own slice of connections and output
directory:

```bash
for i in $(seq 1 20); do
    node loadtest/dg-sdk-loadtest.mjs <endpoint-name> \
        --file ./english.wav \
        --reference ./english.txt \
        --connections 50 \
        --no-loop \
        --region us-east-2 \
        --transcripts-dir /tmp/proc-$(printf '%02d' "$i")-transcripts &
done
wait
```

## Success criterion

Every connection's WER should land within 1 percentage point of the
single-concurrency baseline. If WER spikes for any connection, the burst
path is dropping audio mid-stream and the storm-absorption logic needs
another look.

**Wall-clock is not a stable metric.** The SDK does retry + buffering, so
some connections finish earlier than others. Don't chase wall-clock parity
across runs.

## CLI reference

```
node loadtest/dg-sdk-loadtest.mjs --help
```

Flag-for-flag with the Java/Python references. Notable flags:

- `--connections N` (default 1) — simultaneous streams
- `--batch-size N` (default 0 = all at once) / `--batch-delay S` — stagger
  connection opens
- `--loop` / `--no-loop` (default no-loop, matches the Salesforce
  production pattern of one play per stream)
- `--duration S` (default 0 = run until audio ends)
- `--max-retries N` (default 10) — caller-side retry on top of the
  transport's own
- `--await-final-results S` (default 15) — post-CloseStream flush window
- `--transcripts-dir DIR` — per-connection transcript dump + summary.csv
- `--write-reference PATH` — capture model transcripts at concurrency 1
- `--reference ''` — explicitly disable WER (smoke runs)
