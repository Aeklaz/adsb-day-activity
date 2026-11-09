"""Day-level flight activity for every aircraft in an ICAO block, from adsb.lol's DAILY BULK
ARCHIVE, reading only the heatmap section of each day's tar with HTTP range requests.

Why: the per-aircraft files (fetch_history.py) are a rolling 43 days. The bulk archive reaches
back to 2023-02-16 but is ~3.4 GB a day with no index. Its last ~0.8 GB is `heatmap/`: 48
half-hour files holding a position for every aircraft every 10 seconds. That answers, for any hex
on any day: was it seen, for how long, airborne or on the ground, and where.

Stdlib only, no database: it writes one small CSV per day.

    python fetch_heatmap.py --from 2026-11-06 --to 2026-11-08 --out out
    python fetch_heatmap.py --dates 2026-11-07 --slices 40-41 --out out

Archive layout (verified 2026-11-09 on v2026.11.07-planes-readsb-prod-0):
    ./traces/xx/trace_full_<hex>.json      first ~76% of the bytes
    ./acas/...   ./LICENSE-cc0.txt
    ./heatmap/NN.bin.ttf  x 48             gzip; NN = half-hour of the UTC day (00 = 00:00-00:30)
    ./LICENSE-ODbL.txt   ./README.txt
Assets: github.com/adsblol/globe_history_<year>/releases/download/<tag>/<tag>.tar.aa, .ab, ...
        tag = v<YYYY.MM.DD>-planes-readsb-<instance>. Parts are 2,000,000,000 bytes, a multiple of
        512, so tar block alignment survives the split and any offset can be probed for a header.

Heatmap record = 4 little-endian int32: [hex | flags, lat*1e6, lon*1e6, altitude/speed word]
    upper byte of word 0 non-zero  -> time marker (0x0E7F7C9D, every 10 s) or non-ICAO address
    lat >= 2^30                    -> callsign/type info record, not a position
    low 16 bits of word 3          -> altitude / 25 ft, signed; -123 means on the ground

Output, one row per aircraft seen that day:
    hex, obs_date, points, airborne_points, slices, first_lat, first_lon, last_lat, last_lon
    points x 10 s = time observed; `slices` is a bitmask of the half-hours it appeared in.
The first line of each CSV is a `#key=value;...` comment recording the release tag and which
half-hours were read, so an aircraft with no row means "not seen in the half-hours read",
never simply "did not fly".

Data: adsb.lol globe_history, (c) adsb.lol contributors, Open Database License (ODbL) 1.0.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import logging
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

log = logging.getLogger("heatmap")

ASSET = "https://github.com/adsblol/globe_history_{year}/releases/download/{tag}/{tag}.tar.{part}"
PART_NAMES = ("aa", "ab", "ac", "ad", "ae", "af")
UA = "adsb-day-activity/0.1 (ODbL archive reader)"
GROUND = 0xFF85  # -123 as an unsigned 16-bit value
SIGNED_URL_TTL = 240  # GitHub's redirect target is signed for ~5 minutes
COLUMNS = ("hex", "obs_date", "points", "airborne_points", "slices",
           "first_lat", "first_lon", "last_lat", "last_lon")
CSV_NAME = "adsb_day_{day}.csv.gz"

class NoRelease(RuntimeError):
    """No archive published for that day and instance."""


# ---------------------------------------------------------------- archive access

def _open(url: str, start: int, length: int, tries: int = 6):
    """Range GET with retries. Returns (status, body, final_url, headers); 404/416 are returned."""
    last: Exception | None = None
    for attempt in range(tries):
        req = urllib.request.Request(
            url, headers={"User-Agent": UA, "Range": f"bytes={start}-{start + length - 1}"}
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                return r.status, r.read(), r.url, r.headers
        except urllib.error.HTTPError as e:
            if e.code in (404, 416):
                return e.code, b"", url, e.headers
            last = e
        except Exception as e:  # reset, timeout, truncated read
            last = e
        time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"range read failed after {tries} tries: {url} @{start}+{length}: {last!r}")


class Archive:
    """One day's split tar, addressed by a single global byte offset."""

    def __init__(self, day: dt.date, instance: str):
        self.tag = f"v{day:%Y.%m.%d}-planes-readsb-{instance}"
        self.parts: list[tuple[str, int]] = []  # (github url, size)
        self._signed: dict[int, tuple[str, float]] = {}
        # Releases dated around 1 January can sit in the neighbouring year's repository.
        for year in (day.year, day.year - 1, day.year + 1):
            for name in PART_NAMES:
                url = ASSET.format(year=year, tag=self.tag, part=name)
                status, _, final, headers = _open(url, 0, 1)
                if status != 206:
                    break
                self.parts.append((url, int(headers["Content-Range"].rsplit("/", 1)[1])))
                self._signed[len(self.parts) - 1] = (final, time.monotonic())
            if self.parts:
                break
        if not self.parts:
            raise NoRelease(self.tag)
        self.size = sum(s for _, s in self.parts)
        self.bytes_read = 0

    def read(self, offset: int, length: int) -> bytes:
        out = bytearray()
        while length > 0 and offset < self.size:
            idx, start = 0, offset
            while start >= self.parts[idx][1]:
                start -= self.parts[idx][1]
                idx += 1
            n = min(length, self.parts[idx][1] - start)
            signed, at = self._signed.get(idx, ("", 0.0))
            fresh = bool(signed) and time.monotonic() - at < SIGNED_URL_TTL
            status, body, final, _ = _open(signed if fresh else self.parts[idx][0], start, n)
            if fresh and (status != 206 or len(body) != n):
                fresh = False  # stale signature: go back through github.com once
                status, body, final, _ = _open(self.parts[idx][0], start, n)
            if status != 206 or len(body) != n:
                raise RuntimeError(
                    f"{self.tag}: short read at {offset} (HTTP {status}, {len(body)}/{n} bytes)"
                )
            if not fresh:
                self._signed[idx] = (final, time.monotonic())
            out += body
            offset += n
            length -= n
            self.bytes_read += n
        return bytes(out)


def _tar_header(block: bytes) -> tuple[str, int] | None:
    """(name, size) if `block` is a valid ustar header, else None."""
    if len(block) < 512 or block[257:262] != b"ustar":
        return None
    try:
        checksum = int(block[148:156].split(b"\0")[0].strip() or b"0", 8)
        size = int(block[124:136].split(b"\0")[0].strip() or b"0", 8)
    except ValueError:
        return None
    if sum(block[:148]) + 8 * 32 + sum(block[156:512]) != checksum:
        return None
    prefix, name = block[345:500].split(b"\0")[0], block[:100].split(b"\0")[0]
    return ((prefix + b"/" + name) if prefix else name).decode("utf-8", "replace"), size


def _first_header(ar: Archive, offset: int, window: int) -> tuple[int, str, int] | None:
    offset = offset // 512 * 512
    data = ar.read(offset, window)
    for i in range(0, len(data) - 511, 512):
        h = _tar_header(data[i : i + 512])
        if h:
            return offset + i, h[0], h[1]
    return None


def locate_heatmap(ar: Archive) -> dict[int, tuple[int, int]]:
    """{slice index: (data offset, size)} for the heatmap files, found without reading the traces.

    `traces/` comes first and is dense with headers; everything after it is a few large files. So
    "does a window at this offset contain a traces header" is monotonic and can be binary-searched,
    then the handful of remaining headers are walked one by one.
    """
    window, lo = 262144, 0
    for _ in range(4):  # widen the window if an unusually large trace file fooled a probe
        hi, anchor = ar.size, None
        while hi - lo > window:
            mid = (lo + hi) // 2
            found = _first_header(ar, mid, window)
            if found and "/traces/" in found[1]:
                lo, anchor = found[0], found[0]
            else:
                hi = mid
        offset = anchor if anchor is not None else lo // 512 * 512
        members: dict[int, tuple[int, int]] = {}
        for _ in range(400):
            h = _tar_header(ar.read(offset, 512))
            if h is None:
                break
            name, size = h
            m = re.fullmatch(r"\./heatmap/(\d\d)\.bin\.ttf", name)
            if m:
                members[int(m.group(1))] = (offset + 512, size)
            elif members:
                return members  # past the heatmap directory
            offset += 512 + (size + 511) // 512 * 512
        if members:
            return members
        lo, window = offset, window * 4  # still inside traces: search again from here
    raise RuntimeError(f"{ar.tag}: heatmap section not found")


# ---------------------------------------------------------------- parsing

def parse_slice(data: bytes, lo: int, hi: int, agg: dict[int, list], bit: int) -> None:
    """Fold one decompressed half-hour file into
    agg[hex] = [points, airborne, slices, first_lat, first_lon, last_lat, last_lon]."""
    n = len(data) // 16 * 16
    u, s = memoryview(data)[:n].cast("I"), memoryview(data)[:n].cast("i")
    for h, lat, lon, w in zip(u[0::4], s[1::4], s[2::4], u[3::4], strict=True):
        if h & 0xFF000000 or not lo <= h <= hi or lat >= 1 << 30:
            continue
        air = (w & 0xFFFF) != GROUND
        rec = agg.get(h)
        if rec is None:
            agg[h] = [1, int(air), bit, lat, lon, lat, lon]
        else:
            rec[0] += 1
            rec[1] += air
            rec[2] |= bit
            rec[5] = lat
            rec[6] = lon


def fetch_day(day: dt.date, slices: list[int], *, instances: list[str], lo: int, hi: int,
              workers: int) -> tuple[dict, list[tuple]]:
    """Read one day. Returns (meta, rows) with rows in COLUMNS order; writes nothing."""
    ar = None
    for inst in instances:
        try:
            ar = Archive(day, inst)
            break
        except NoRelease:
            continue
    if ar is None:
        raise NoRelease(f"{day}: none of {instances}")
    t0 = time.monotonic()
    members = locate_heatmap(ar)
    t_locate = time.monotonic() - t0
    want = sorted(i for i in slices if i in members)
    agg: dict[int, list] = {}
    mask, t_parse = 0, 0.0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Download `workers` slices at a time, then parse them in time order so first/last
        # position mean what they say and memory stays bounded.
        for k in range(0, len(want), workers):
            group = want[k : k + workers]
            blobs = list(pool.map(lambda i: ar.read(*members[i]), group))
            t1 = time.monotonic()
            for i, blob in zip(group, blobs, strict=True):
                parse_slice(gzip.decompress(blob), lo, hi, agg, 1 << i)
                mask |= 1 << i
            t_parse += time.monotonic() - t1
    rows = [
        (f"{h:06x}", day.isoformat(), r[0], r[1], r[2],
         round(r[3] / 1e6, 5), round(r[4] / 1e6, 5), round(r[5] / 1e6, 5), round(r[6] / 1e6, 5))
        for h, r in sorted(agg.items())
    ]
    meta = {"tag": ar.tag, "mask": mask, "slices": len(want), "bytes": ar.bytes_read,
            "seconds": time.monotonic() - t0, "locate_s": t_locate, "parse_s": t_parse}
    return meta, rows


# ---------------------------------------------------------------- outputs

def write_csv(outdir: Path, day: dt.date, meta: dict, rows: list[tuple]) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / CSV_NAME.format(day=day)
    tmp = path.with_name(path.name + ".tmp")
    with gzip.open(tmp, "wt", newline="", compresslevel=9) as f:
        f.write(f"#tag={meta['tag']};slices_fetched={meta['mask']};bytes={meta['bytes']};"
                "source=adsb.lol globe_history (ODbL 1.0)\n")
        w = csv.writer(f)
        w.writerow(COLUMNS)
        w.writerows(rows)
    tmp.replace(path)  # a half-written file never looks finished
    return path


# ---------------------------------------------------------------- CLI

def parse_slices(spec: str, step: int) -> list[int]:
    if spec == "all":
        out = list(range(48))
    else:
        out = []
        for part in spec.split(","):
            a, _, b = part.partition("-")
            out += list(range(int(a), int(b or a) + 1))
    return out[::step]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dates", help="comma-separated YYYY-MM-DD (instead of --from/--to)")
    ap.add_argument("--from", dest="start", type=dt.date.fromisoformat)
    ap.add_argument("--to", dest="end", type=dt.date.fromisoformat)
    ap.add_argument("--step", type=int, default=1, help="every Nth day counting back from --to")
    ap.add_argument("--slices", default="all",
                    help='half-hours of the UTC day, e.g. "24-47,0-5" (default all 48)')
    ap.add_argument("--slice-step", type=int, default=1, help="take every Nth of the selected slices")
    ap.add_argument("--instance", default="prod-0,staging-0", help="release instances to try, in order")
    ap.add_argument("--hex-range", default="a00000-afffff", help="ICAO block to keep (default: United States)")
    ap.add_argument("--workers", type=int, default=3, help="parallel slice downloads")
    ap.add_argument("--out", type=Path, default=Path("out"), metavar="DIR",
                    help="directory for the per-day CSV files (default ./out)")
    ap.add_argument("--refetch", action="store_true", help="re-read days whose CSV already exists")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if a.dates:
        days = [dt.date.fromisoformat(d) for d in a.dates.split(",")]
    elif a.start and a.end:
        days = [a.end - dt.timedelta(days=i) for i in range(0, (a.end - a.start).days + 1, a.step)]
    else:
        ap.error("give --dates, or --from and --to")
    lo, hi = (int(x, 16) for x in a.hex_range.split("-"))
    slices = parse_slices(a.slices, a.slice_step)

    have = {d for d in days if (a.out / CSV_NAME.format(day=d)).exists()}

    failed = 0
    for day in days:
        if day in have and not a.refetch:
            log.info("%s already stored, skipping", day)
            continue
        try:
            meta, rows = fetch_day(day, slices, instances=a.instance.split(","), lo=lo, hi=hi,
                                   workers=a.workers)
            write_csv(a.out, day, meta, rows)
        except NoRelease as e:
            log.warning("no archive: %s", e)
            continue
        except Exception:
            log.exception("%s failed; continuing with the next day", day)
            failed += 1
            continue
        log.info("%s %s: %d slices, %.0f MB in %.0fs (locate %.0fs, parse %.0fs) -> %d aircraft",
                 day, meta["tag"], meta["slices"], meta["bytes"] / 1e6, meta["seconds"],
                 meta["locate_s"], meta["parse_s"], len(rows))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
