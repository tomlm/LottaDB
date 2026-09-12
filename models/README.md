# Test models

The vector-search tests need an ONNX embedding model. It lives here at
`models/bge-micro-v2/`, but it is **not committed** — `.gitignore` excludes it.

## Why it isn't in git

The model is 18 MB extracted. Committing it would add that to every clone of this repository
permanently, and git history can't be slimmed afterwards without a rewrite. For a library that
ships on NuGet, that's a poor trade for a test fixture.

So it is hosted as a **GitHub release asset** on the
[`test-assets-v1`](https://github.com/tomlm/LottaDB/releases/tag/test-assets-v1) tag
(a pre-release — not a product release) and fetched on demand.

## Why it isn't just downloaded from HuggingFace

It used to be. `ElBruno.LocalEmbeddings` fetches the model from HuggingFace on first use, and
CI went red when huggingface.co returned `429 TooManyRequests` — every `VectorSearchTests` case
failed with nothing wrong in the repository. Serving it from our own release removes that
dependency, and CI caches it so it is usually not downloaded at all.

## Getting it

```bash
./scripts/fetch-test-model.sh
```

Re-running is a no-op if the model is already present. The script verifies the archive's
sha256 before extracting.

**You may not need it.** `LocalModel.Options()` (in `src/LottaDB.Tests/LocalModel.cs`) falls
back to the original download-from-HuggingFace behaviour when this directory is absent, so the
tests still run on a fresh checkout — they just depend on the network, as they did before.

## How the tests find it

`LocalModel` walks up from the test assembly's directory looking for `models/bge-micro-v2`,
then sets `LocalEmbeddingsOptions.ModelPath` to it. When `ModelPath` is set,
`LocalEmbeddingGenerator` returns before it ever constructs a `ModelDownloader`, so no network
call happens at all.

Walking up avoids copying 17 MB into the build output of all five test projects just to locate
it.

## Contents

| File | Size | Purpose |
|---|---|---|
| `model_quantized.onnx` | 17.4 MB | Quantized model weights |
| `tokenizer.json`, `tokenizer_config.json`, `vocab.txt` | ~945 KB | Tokenizer, loaded from this same directory |
| `config.json` | 745 B | Model config |
| `LICENSE` | — | Upstream license (MIT) |

This is the **quantized** model, not the 69 MB `model.onnx`. The tests set
`PreferQuantized = true` and `ResolveModelPath` looks for `model_quantized.onnx` first, so the
full-precision weights are never needed.

## Provenance and license

From [SmartComponents/bge-micro-v2](https://huggingface.co/SmartComponents/bge-micro-v2), a
preservation fork of [TaylorAI/bge-micro-v2](https://huggingface.co/TaylorAI/bge-micro-v2).

**MIT licensed** — redistribution permitted. The upstream `LICENSE` is included in the archive.

## Updating the model

1. Download the new files from HuggingFace, keeping the file names identical —
   `ModelDownloader` in `ElBruno.LocalEmbeddings` expects exactly `model_quantized.onnx` /
   `model_int8.onnx` and `tokenizer.json` / `tokenizer_config.json` / `vocab.txt`.
2. `tar -czf bge-micro-v2.tar.gz bge-micro-v2` from within `models/`.
3. Upload under a **new** tag (`test-assets-v2`) rather than replacing the existing asset, so
   older commits keep building.
4. Update `ASSET_TAG` and `EXPECTED_SHA256` in `scripts/fetch-test-model.sh`, and the cache key
   in `.github/workflows/BuildAndRunTests.yml`.
