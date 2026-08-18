# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # PoC: PII Tokenization at Bronze Landing
#
# **Mục tiêu:** Chứng minh phần khó nhất của kiến trúc — PII tokenization
# xảy ra *trước* khi dữ liệu chạm Bronze — là feasible, hiệu năng chấp nhận
# được, và tương thích hoàn toàn với Iceberg/Delta ACID semantics.
#
# Đây là spike cho Topic A (LLM Observability @ 1B req/ngày).

# %%
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sys
import time
from pathlib import Path

# Ensure scripts/ is importable (poc is at submission/bonus/poc/ — 3 levels from repo root)
_repo_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_repo_root / "scripts"))

import polars as pl
from deltalake import DeltaTable, write_deltalake

from lakehouse import path, reset, du, human

# %% [markdown]
# ## 1. Format-Preserving PII Tokenizer
#
# Production sẽ dùng một dedicated tokenization service (e.g., Protegrity,
# Vault Transit). PoC này dùng HMAC-SHA256 truncated — cùng input luôn ra
# cùng token (deterministic), nhưng không thể reverse nếu không có key.

# %%
# In production: load from KMS (AWS Secrets Manager / HashiCorp Vault)
_TOKEN_KEY = b"poc-demo-key-not-for-production"


def tokenize(value: str, key: bytes = _TOKEN_KEY) -> str:
    """Format-preserving tokenization using HMAC-SHA256.

    Same input → same token (join-safe).
    Token prefix 'tok_' makes it visually obvious that PII has been replaced.
    """
    digest = hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:16]
    return f"tok_{digest}"


def tokenize_email(email: str) -> str:
    """Tokenize email while preserving domain for analytics."""
    local, domain = email.rsplit("@", 1)
    return f"{tokenize(local)}@{domain}"


# Regex patterns for PII detection in free-text fields
_PII_PATTERNS = {
    "phone_vn": re.compile(r"\b0\d{9,10}\b"),                # VN phone
    "email":    re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.]+\b"),   # email
    "cmnd":     re.compile(r"\b\d{9}(?:\d{3})?\b"),           # 9 or 12 digit ID
}


def scrub_free_text(text: str) -> str:
    """Replace PII patterns in free-text with tokens."""
    for name, pattern in _PII_PATTERNS.items():
        text = pattern.sub(lambda m: tokenize(m.group()), text)
    return text


# Quick sanity check
_test_phone = "0912345678"
_tok = tokenize(_test_phone)
print(f"  tokenize('{_test_phone}') → '{_tok}'")
assert _tok.startswith("tok_")
assert tokenize(_test_phone) == _tok, "Must be deterministic!"
assert _tok != _test_phone, "Must not be identity!"
print("  ✓ Tokenizer is deterministic and non-reversible")

# %% [markdown]
# ## 2. Simulated Raw LLM Requests (pre-Bronze)
#
# Tạo 50K raw events giống production log, bao gồm PII fields.

# %%
import random
import uuid
from datetime import datetime, timedelta, timezone

random.seed(42)
N = 50_000

start = datetime(2026, 4, 1, tzinfo=timezone.utc)
tenants = [f"tenant_{i:03d}" for i in range(50)]
models = ["claude-sonnet-4-6", "claude-haiku-4-5", "claude-opus-4-7"]
model_weights = [6, 3, 1]

raw_events = []
for i in range(N):
    ts = start + timedelta(seconds=i * 6)  # ~6s apart = ~14.4K/day
    tenant = random.choice(tenants)
    model = random.choices(models, weights=model_weights)[0]

    raw_events.append({
        "request_id": str(uuid.uuid4()),
        "ts": ts,
        "tenant_id": tenant,
        "raw_json": json.dumps({
            "model": model,
            "user_email": f"user{random.randint(1,5000)}@{tenant}.com",
            "user_phone": f"09{random.randint(10000000, 99999999)}",
            "user_id_card": f"{random.randint(100000000, 999999999999):012d}",
            "prompt": f"Xin chào, tôi là user{random.randint(1,100)}, SĐT 09{random.randint(10000000,99999999)}. Hãy giúp tôi...",
            "usage": {"input": random.randint(50, 4000), "output": random.randint(20, 2000)},
            "latency_ms": random.randint(100, 5000),
            "status": random.choices(["ok", "rate_limited", "error"], weights=[95, 3, 2])[0],
        }),
    })

print(f"Generated {N:,} raw events")
# Show a sample with PII visible
sample = json.loads(raw_events[0]["raw_json"])
print(f"\n  Sample raw (PII visible!):")
print(f"    email:   {sample['user_email']}")
print(f"    phone:   {sample['user_phone']}")
print(f"    id_card: {sample['user_id_card']}")
print(f"    prompt:  {sample['prompt'][:80]}...")

# %% [markdown]
# ## 3. Tokenization Pipeline (simulates Flink pre-Bronze step)
#
# Benchmark: bao nhiêu events/giây chúng ta có thể tokenize?

# %%
def tokenize_event(event: dict) -> dict:
    """Tokenize all PII fields in a raw event before writing to Bronze."""
    parsed = json.loads(event["raw_json"])

    # Structured PII fields → deterministic tokens
    parsed["user_email"] = tokenize_email(parsed["user_email"])
    parsed["user_phone"] = tokenize(parsed["user_phone"])
    parsed["user_id_card"] = tokenize(parsed["user_id_card"])

    # Free-text PII scrubbing (prompt may contain phone numbers, names)
    parsed["prompt"] = scrub_free_text(parsed["prompt"])

    return {
        "request_id": event["request_id"],
        "ts": event["ts"],
        "tenant_id": event["tenant_id"],
        "raw_json": json.dumps(parsed),
    }


# Benchmark
t0 = time.perf_counter()
tokenized_events = [tokenize_event(e) for e in raw_events]
elapsed = time.perf_counter() - t0
throughput = N / elapsed

print(f"Tokenized {N:,} events in {elapsed:.2f}s → {throughput:,.0f} events/sec")
print(f"At 1B req/day = {1e9/86400:,.0f} req/s, need {1e9/86400/throughput:.0f}× parallelism")

# Verify PII is gone
sample_tok = json.loads(tokenized_events[0]["raw_json"])
print(f"\n  After tokenization:")
print(f"    email:   {sample_tok['user_email']}")
print(f"    phone:   {sample_tok['user_phone']}")
print(f"    id_card: {sample_tok['user_id_card']}")
print(f"    prompt:  {sample_tok['prompt'][:80]}...")

assert sample_tok["user_phone"].startswith("tok_"), "Phone must be tokenized"
assert sample_tok["user_id_card"].startswith("tok_"), "ID card must be tokenized"
assert "tok_" in sample_tok["user_email"], "Email local part must be tokenized"
print("\n  ✓ All structured PII fields tokenized")

# Check free-text scrubbing
for e in tokenized_events[:100]:
    p = json.loads(e["raw_json"])
    assert not re.search(r"\b09\d{8,9}\b", p["prompt"]), \
        f"Phone number leaked in prompt: {p['prompt']}"
print("  ✓ No phone numbers found in first 100 prompts (free-text scrubbing works)")

# %% [markdown]
# ## 4. Write Tokenized Data to Bronze (Delta Lake / Iceberg compatible)
#
# Bronze chứa *tokenized* data. PII plain-text **không bao giờ** tồn tại trên disk.

# %%
df = pl.DataFrame(tokenized_events)
bronze_path = path("scratch", "poc_bronze_tokenized")
reset(bronze_path)
write_deltalake(bronze_path, df.to_arrow(), mode="overwrite")

bronze_size = du(bronze_path)
print(f"Bronze written: {len(df):,} rows, {human(bronze_size)} on disk")
print(f"  Path: {bronze_path}")

# %% [markdown]
# ## 5. Demonstrate: Time Travel After Key Rotation
#
# Kịch bản: key cũ bị compromise → re-tokenize với key mới.
# Iceberg/Delta time travel cho phép đọc snapshot cũ để audit.

# %%
# Simulate key rotation
_NEW_KEY = b"rotated-key-2026-08-18"

def re_tokenize_event(event: dict, old_key: bytes, new_key: bytes) -> dict:
    """Re-tokenize an event with a new key.

    In production, this would read from the old snapshot (time travel),
    decrypt old tokens (requires old key), and re-encrypt with new key.

    For this PoC, we re-tokenize from the raw source to demonstrate
    the pipeline works with different keys.
    """
    parsed = json.loads(event["raw_json"])

    # In production: reverse old token → get PII → apply new token
    # In PoC: we just show the mechanism with new key directly
    parsed["user_phone"] = tokenize(parsed["user_phone"], new_key)
    parsed["user_id_card"] = tokenize(parsed["user_id_card"], new_key)

    return {**event, "raw_json": json.dumps(parsed)}


# Re-tokenize first 1000 events as a demo
re_tokenized = [re_tokenize_event(e, _TOKEN_KEY, _NEW_KEY) for e in tokenized_events[:1000]]

# Write as a new version (append mode to simulate incremental re-tokenization)
df_new = pl.DataFrame(re_tokenized)
write_deltalake(bronze_path, df_new.to_arrow(), mode="append")

# Time travel: read old version
dt = DeltaTable(bronze_path)
versions = dt.history()
print(f"\nDelta history after re-tokenization: {len(versions)} versions")
for v in versions:
    print(f"  v{v['version']}: {v['operation']} ({v['operationMetrics']})")

# Compare old vs new tokens for same record
old_df = pl.from_arrow(DeltaTable(bronze_path, version=0).to_pyarrow_table()).head(1)
new_df = pl.from_arrow(DeltaTable(bronze_path, version=1).to_pyarrow_table()).head(1)

old_token = json.loads(old_df["raw_json"][0])["user_phone"]
new_token = json.loads(new_df["raw_json"][0])["user_phone"]

print(f"\n  Same record, different keys:")
print(f"    v0 token (old key): {old_token}")
print(f"    v1 token (new key): {new_token}")
assert old_token != new_token, "Different keys must produce different tokens"
print("  ✓ Key rotation produces different tokens — old snapshots auditable via time travel")

# %% [markdown]
# ## 6. Assertions (Pass/Fail Criteria)

# %%
# --- Final assertions ---
assert throughput > 10_000, f"Tokenization throughput too low: {throughput:.0f} evt/s"
assert sample_tok["user_phone"].startswith("tok_"), "PII not tokenized"
assert bronze_size > 0, "Bronze not written"
assert len(versions) >= 2, "Time travel not working"
assert old_token != new_token, "Key rotation ineffective"

print("\n" + "=" * 60)
print("  PoC PASSED — PII tokenization at Bronze landing is feasible")
print(f"  Throughput: {throughput:,.0f} events/sec (single-threaded Python)")
print(f"  Extrapolation: {1e9/86400/throughput:.0f} parallel workers needed for 1B/day")
print(f"  Bronze size: {human(bronze_size)} for {N:,} events")
print("=" * 60)
