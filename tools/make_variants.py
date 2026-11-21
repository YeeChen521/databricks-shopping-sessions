#!/usr/bin/env python3
"""
Builds robustness variants of a generated dataset, exercising paths the generator never takes:
  long   - many user / click / event IDs longer than 15 bytes (hashed-key path), applied
           consistently across events, clicks and links, plus long IDs that share a 15-byte
           prefix (ordering must fall back to full strings)
  nolink - no links/ directory at all
  ragged - files without a trailing newline, zero-byte files, extra empty files
Usage: make_variants.py SRC_DATASET OUT_ROOT
"""
import hashlib
import os
import shutil
import sys

src, root = sys.argv[1], sys.argv[2]


def h(s):
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


def longify(v, kind):
    # deterministic: same input -> same output in every file
    if not v or '"' in v:
        return v
    x = h(kind + v)
    if x % 3 == 0:
        return v + "-" + "abcdefghijklmnop"[: 8 + x % 8]  # > 15 bytes, distinct per ID
    if x % 7 == 0:
        return "SHAREDPREFIX123" + v  # long, first 15 bytes identical across IDs
    return v


def transform_csv(path_in, path_out, cols):
    with open(path_in, encoding="utf-8", newline="") as f:
        data = f.read()
    lines = data.split("\n")
    out = [lines[0]]
    for line in lines[1:]:
        parts = line.split(",")
        new = []
        for i, p in enumerate(parts):
            kind = cols.get(i)
            if kind is None:
                new.append(p)
                continue
            q = p.startswith('"') and p.endswith('"') and len(p) >= 2 and p.count('"') == 2
            v = p[1:-1] if q else p
            v2 = longify(v, kind)
            new.append(f'"{v2}"' if q else v2)
        out.append(",".join(new))
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(out))


def copy_lookups(dst):
    for name in ("products.csv", "campaigns.csv", "manifest.json"):
        shutil.copy(os.path.join(src, name), os.path.join(dst, name))


# long
dst = os.path.join(root, "v_long")
shutil.rmtree(dst, ignore_errors=True)
os.makedirs(dst)
copy_lookups(dst)
for folder, cols in (("events", {0: "e", 1: "u"}), ("clicks", {0: "k", 1: "u"}), ("links", {0: "u", 1: "u"})):
    os.makedirs(os.path.join(dst, folder))
    for name in os.listdir(os.path.join(src, folder)):
        transform_csv(os.path.join(src, folder, name), os.path.join(dst, folder, name), cols)

# nolink
dst = os.path.join(root, "v_nolink")
shutil.rmtree(dst, ignore_errors=True)
os.makedirs(dst)
copy_lookups(dst)
for folder in ("events", "clicks"):
    shutil.copytree(os.path.join(src, folder), os.path.join(dst, folder))

# ragged
dst = os.path.join(root, "v_ragged")
shutil.rmtree(dst, ignore_errors=True)
os.makedirs(dst)
copy_lookups(dst)
for folder in ("events", "clicks", "links"):
    shutil.copytree(os.path.join(src, folder), os.path.join(dst, folder))
    names = sorted(os.listdir(os.path.join(dst, folder)))
    for i, name in enumerate(names):
        p = os.path.join(dst, folder, name)
        if i % 2 == 0:
            with open(p, "rb+") as f:
                f.seek(0, 2)
                size = f.tell()
                if size:
                    f.seek(size - 1)
                    if f.read(1) == b"\n":
                        f.truncate(size - 1)
    open(os.path.join(dst, folder, "zz-empty.csv"), "w").close()
    with open(os.path.join(dst, folder, "zz-header-only-no-newline.csv"), "w") as f:
        f.write({"events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                 "clicks": "click_id,user_id,campaign_id,ts", "links": "user_id_a,user_id_b"}[folder])
print("variants written to", root)
