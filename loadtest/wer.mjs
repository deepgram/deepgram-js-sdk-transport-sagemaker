/**
 * Word error rate computation, byte-for-byte compatible with the Java and
 * Python load tests so cross-language numbers compare directly.
 *
 * The normalization rule is "lowercase, replace ASCII punctuation with
 * whitespace, collapse runs"; the distance is word-level Levenshtein with
 * two rolling rows.
 */

const ASCII_PUNCT = new Set("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");

export function normalizeWords(text) {
  if (text == null) return [];
  let buf = "";
  for (const ch of text) {
    if (ASCII_PUNCT.has(ch)) {
      buf += " ";
    } else {
      buf += ch.toLowerCase();
    }
  }
  return buf.trim().split(/\s+/).filter((w) => w.length > 0);
}

export function editDistance(ref, hyp) {
  if (ref.length === 0) return hyp.length;
  if (hyp.length === 0) return ref.length;
  let prev = new Array(hyp.length + 1);
  let curr = new Array(hyp.length + 1);
  for (let j = 0; j <= hyp.length; j++) prev[j] = j;
  for (let i = 1; i <= ref.length; i++) {
    curr[0] = i;
    const ri = ref[i - 1];
    for (let j = 1; j <= hyp.length; j++) {
      const cost = ri === hyp[j - 1] ? 0 : 1;
      curr[j] = Math.min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
    }
    [prev, curr] = [curr, prev];
  }
  return prev[hyp.length];
}

/**
 * Word error rate = editDistance(refWords, hypWords) / refWords.length.
 *
 * Returns ``null`` when the hypothesis is empty or the reference has no
 * words. Mirrors the Java/Python harness so "no transcript" rows show as
 * blank in summary.csv rather than an inflated 100% miss.
 */
export function computeWer(reference, hypothesis) {
  if (hypothesis == null || hypothesis.trim() === "") return null;
  const ref = normalizeWords(reference);
  const hyp = normalizeWords(hypothesis);
  if (ref.length === 0) return null;
  return editDistance(ref, hyp) / ref.length;
}
