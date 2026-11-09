"""Day-level flight activity for every aircraft in an ICAO block, from adsb.lol's DAILY BULK
ARCHIVE, reading only the heatmap section of each day's tar with HTTP range requests.

Why: the per-aircraft files (fetch_history.py) are a rolling 43 days. The bulk archive reaches
back to 2023-02-16 but is up to ~3.4 GB a day with no index. About a quarter of it is `heatmap/`:
48 half-hour files holding a position for every aircraft every 10 seconds (every 30 s before
2023-08-02 07:30 UTC). That answers, for any hex on any day: was it seen, for how long, airborne
or on the ground, and where.

Stdlib only, no database: it writes one small CSV per day.

    python fetch_heatmap.py --from 2026-11-06 --to 2026-11-08 --out out
    python fetch_heatmap.py --dates 2026-11-07 --slices 40-41 --out out

Archive layout (one tar per day and instance):
    ./traces/xx/trace_full_<hex>.json      most of the bytes, hundreds of thousands of small files
    ./heatmap/NN.bin.ttf  x 48             gzip; NN = half-hour of the UTC day (00 = 00:00-00:30)
    ./acas/...  ./README.txt  ./LICENSE-*.txt
The ORDER of those top-level entries differs between eras of the archive (heatmap before the
traces in mid-2023 and mid-2024, after them in 2026), so it is discovered, never assumed.
Assets: github.com/adsblol/globe_history_<year>/releases/download/<tag>/<tag>.tar when the day is
        under 2 GB, else split as <tag>.tar.aa, .tar.ab, ... in parts of 2,000,000,000 bytes (a
        multiple of 512, so tar block alignment survives the split and any offset can be probed
        for a header). tag = v<YYYY.MM.DD>-planes-readsb-<instance>.

Heatmap record = 4 little-endian int32: [hex | flags, lat*1e6, lon*1e6, altitude/speed word]
(format per readsb globe_index.c, identical in every era checked, Feb 2023 - Nov 2026)
    file start                     -> index, one entry per sample time, then the time markers
    word 0 = 0x0E7F7C9D            -> time marker; its altitude field = sample interval in ms
    bit 24 of word 0               -> non-ICAO address (skipped)
    bits 27-31 of word 0           -> address type (ADS-R/UAT, TIS-B, MLAT ...), masked off
    lat >= 2^30                    -> callsign/squawk info record, not a position
    low 16 bits of word 3          -> altitude / 25 ft, signed; -123 on the ground, -124 unknown

Output, one row per aircraft seen that day:
    hex, obs_date, points, airborne_points, slices, first_lat, first_lon, last_lat, last_lon
    points x 10 s = time observed (a 30 s sample counts 3); `slices` is a bitmask of the
    half-hours it appeared in.
The first line of each CSV is a `#key=value;...` comment recording the release tag, which
half-hours were read and the READER version, so an aircraft with no row means "not seen in the
half-hours read", never simply "did not fly", and files from two readers are never mixed.

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
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

log = logging.getLogger("heatmap")

ASSET = "https://github.com/adsblol/globe_history_{year}/releases/download/{tag}/{tag}.tar{part}"
PART_SUFFIXES = (".aa", ".ab", ".ac", ".ad", ".ae", ".af")
# Tried in order; the one holding the most half-hours wins (see fetch_day). prod-0 is the main
# aggregator and staging-0 its twin; the rest are names the data was published under in other periods
# (test-* Feb-Mar 2023, prod-1 Mar-Oct 2023, *tmp May-Jun 2025). mlatonly-0 is left out on
# purpose: it holds MLAT positions only, so it would look like a day when almost nobody flew.
INSTANCES = "prod-0,staging-0,prod-0tmp,staging-0tmp,prod-1,test-0,test-1"
HEATMAP_FILE = re.compile(r"\./heatmap/(\d\d)\.bin\.ttf")
WALK_CHUNK = 32768  # bytes fetched per step when walking tar headers
UA = "adsb-day-activity/0.1 (ODbL archive reader)"
GROUND = 0xFF85  # -123 as an unsigned 16-bit value
MARKER = 0x0E7F7C9D  # word 0 of a time marker
SAMPLE_S = 10  # one point = 10 s observed; samples of a 30 s archive count 3 points each
# Smaller heatmap files are outages, read as "not available": an index-only file is under 1 KB,
# a real half-hour is megabytes even in the thinnest month (Feb 2023).
MIN_SLICE_BYTES = 16384
SIGNED_URL_TTL = 240  # GitHub's redirect target is signed for ~5 minutes
COLUMNS = ("hex", "obs_date", "points", "airborne_points", "slices",
           "first_lat", "first_lon", "last_lat", "last_lon")
CSV_NAME = "adsb_day_{day}.csv.gz"
# Bump when a change alters the numbers in the CSV (not for speed or logging changes). Files from
# different readers are not comparable: reader 1 dropped every ADS-R, TIS-B and MLAT position
# (~8% of US aircraft, 16% of our sample) and counted 30 s samples as one point. load_heatmap.py
# refuses a folder whose files are not all from this reader.
READER = 2

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
            if self._add_part(year, PART_SUFFIXES[0]):  # split archive (over 2 GB)
                for suffix in PART_SUFFIXES[1:]:
                    if not self._add_part(year, suffix):
                        break
                break
            if self._add_part(year, ""):  # single .tar
                break
        if not self.parts:
            raise NoRelease(self.tag)
        self.size = sum(s for _, s in self.parts)
        self.bytes_read = 0

    def _add_part(self, year: int, suffix: str) -> bool:
        url = ASSET.format(year=year, tag=self.tag, part=suffix)
        status, _, final, headers = _open(url, 0, 1)
        if status != 206:
            return False
        self.parts.append((url, int(headers["Content-Range"].rsplit("/", 1)[1])))
        self._signed[len(self.parts) - 1] = (final, time.monotonic())
        return True

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


def _members(ar: Archive, offset: int):
    """(header offset, name, size) of each tar member from `offset` to the end of the archive.

    Fetches a chunk at a time, so a run of small members costs one request instead of one each."""
    buf, base = b"", 0
    while True:
        if not base <= offset <= base + len(buf) - 512:
            buf, base = ar.read(offset, WALK_CHUNK), offset
        h = _tar_header(buf[offset - base : offset - base + 512])
        if h is None:
            return
        yield offset, h[0], h[1]
        offset += 512 + (h[1] + 511) // 512 * 512


def _scan(ar: Archive, offset: int, *, through_traces: bool,
          limit: int = 3000) -> tuple[dict[int, tuple[int, int]], int | None]:
    """Walk members from `offset`, collecting {slice index: (data offset, size)} of heatmap files.

    The second value is None when the walk ended by itself (past the heatmap directory, or at
    the end of the archive). Otherwise it is the offset where the walk gave up: the first
    `traces/` member when `through_traces` is false, or wherever `limit` members ran out.
    """
    slices: dict[int, tuple[int, int]] = {}
    for n, (at, name, size) in enumerate(_members(ar, offset)):
        m = HEATMAP_FILE.fullmatch(name)
        if m:
            slices[int(m.group(1))] = (at + 512, size)
        elif slices and not name.startswith("./heatmap/"):
            return slices, None
        elif (name.startswith("./traces/") and not through_traces) or n >= limit:
            return slices, at
    return slices, None


def locate_heatmap(ar: Archive) -> dict[int, tuple[int, int]]:
    """{slice index: (data offset, size)} for the heatmap files, found without reading the traces.

    Each top-level directory is contiguous, but their order is not fixed. `traces/` is dense with
    small members; everything else is a handful of files. So walk from the start: if the heatmap
    comes before `traces/`, that walk finds it. Otherwise "a window at this offset contains a
    traces header" is monotonic from the first traces member on, so the end of `traces/` is
    binary-searched and the few members after it are walked.
    """
    slices, traces_at = _scan(ar, 0, through_traces=False)
    if slices:
        return slices
    if traces_at is None:
        raise RuntimeError(f"{ar.tag}: neither heatmap nor traces found")
    window, lo = 262144, traces_at
    for _ in range(4):  # widen the window if an unusually large trace file fooled a probe
        hi = ar.size
        while hi - lo > window:
            mid = (lo + hi) // 2
            found = _first_header(ar, mid, window)
            if found and "/traces/" in found[1]:
                lo = found[0]
            else:
                hi = mid
        slices, resume = _scan(ar, lo, through_traces=True)
        if slices:
            return slices
        if resume is None:
            break  # walked to the end of the archive: this day has no heatmap
        lo, window = resume, window * 4  # still inside traces: search again from here
    raise RuntimeError(f"{ar.tag}: heatmap section not found")


# ---------------------------------------------------------------- parsing

def parse_slice(data: bytes, lo: int, hi: int, agg: dict[int, list], bit: int) -> None:
    """Fold one decompressed half-hour file into
    agg[hex] = [points, airborne, slices, first_lat, first_lon, last_lat, last_lon].

    The file opens with an index, one entry per sample time; its first entry holds the index's
    own length, and right after it comes the first time marker, whose altitude field is the
    sample interval in ms (30 000 until 2023-08-02 07:30 UTC, 10 000 since). Points are counted
    in SAMPLE_S units so `points x 10 s` means the same in every era. ValueError if that header
    is not there."""
    n = len(data) // 16 * 16
    u, s = memoryview(data)[:n].cast("I"), memoryview(data)[:n].cast("i")
    k = u[0] if n else 0
    step = u[4 * k + 3] & 0xFFFF if 0 < k < len(u) // 4 and u[4 * k] == MARKER else 0
    if not step or step % (SAMPLE_S * 1000):
        raise ValueError(f"no time marker with a 10 s multiple interval after the index ({step})")
    weight = step // (SAMPLE_S * 1000)
    i = 4 * k
    for h, lat, lon, w in zip(u[i::4], s[i + 1 :: 4], s[i + 2 :: 4], u[i + 3 :: 4], strict=True):
        # Bits 24-26 set: a time marker or a non-ICAO address. Bits 27-31 are only the address
        # type (ADS-R/UAT, TIS-B, MLAT ...) of a real ICAO address: masked off, not skipped.
        if h & 0x07000000 or lat >= 1 << 30:
            continue
        h &= 0xFFFFFF
        if not lo <= h <= hi:
            continue
        air = (w & 0xFFFF) != GROUND
        rec = agg.get(h)
        if rec is None:
            agg[h] = [weight, weight * air, bit, lat, lon, lat, lon]
        else:
            rec[0] += weight
            rec[1] += weight * air
            rec[2] |= bit
            rec[5] = lat
            rec[6] = lon


def fetch_day(day: dt.date, slices: list[int], *, instances: list[str], lo: int, hi: int,
              workers: int) -> tuple[dict, list[tuple]]:
    """Read one day from the instance holding the most heatmap data for the wanted half-hours.
    Instances are looked at in order and the search stops at the first that has every wanted
    half-hour, so a normal day costs one look. Returns (meta, rows) with rows in COLUMNS order;
    writes nothing. NoRelease if no instance published that day.

    Why not simply the first: on outage days one instance stops early while its twin carries the
    whole day (2024-10-29: prod-0 37 of 48 half-hours, staging-0 all 48). Why bytes and not the
    count: a half-hour cut short still counts as present (2026-05-05: staging-0 has 14 half-hours
    in 167 MB, prod-0 13 in 235 MB).
    """
    t0 = time.monotonic()
    best: tuple[int, Archive, dict[int, tuple[int, int]]] | None = None
    seen: list[str] = []
    error: Exception | None = None
    for inst in instances:
        try:
            ar = Archive(day, inst)
            members = locate_heatmap(ar)
        except NoRelease:
            continue
        except Exception as e:  # broken or heatmap-less archive: another instance may be fine
            log.warning("%s %s unusable (%s)", day, inst, e)
            error = e
            continue
        have = _available(members, slices)
        size = sum(members[i][1] for i in have)
        seen.append(f"{inst} {len(have)}/{len(slices)} in {size / 1e6:.0f} MB")
        if best is None or size > best[0]:
            best = (size, ar, members)
        if len(have) == len(slices):
            break
    if best is None:
        if error:
            raise error
        raise NoRelease(f"{day}: none of {instances}")
    if len(seen) > 1 or len(_available(best[2], slices)) < len(slices):
        log.info("%s half-hours available: %s -> using %s", day, ", ".join(seen), best[1].tag)
    return _read_archive(best[1], best[2], day, slices, lo, hi, workers, t0)


def _available(members: dict[int, tuple[int, int]], slices: list[int]) -> list[int]:
    """The wanted half-hours whose heatmap file holds data. Outage days leave empty or
    index-only files behind (2023-04-02: nine of 48 are 0 bytes in every instance)."""
    return sorted(i for i in slices if members.get(i, (0, 0))[1] >= MIN_SLICE_BYTES)


def _read_archive(ar: Archive, members: dict[int, tuple[int, int]], day: dt.date,
                  slices: list[int], lo: int, hi: int, workers: int,
                  t0: float) -> tuple[dict, list[tuple]]:
    t_locate = time.monotonic() - t0
    want = _available(members, slices)
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
                try:
                    data = gzip.decompress(blob)
                    parse_slice(data, lo, hi, agg, 1 << i)
                except (EOFError, OSError, zlib.error, ValueError) as e:
                    # Files cut short by a crash (sizes at 4 KB page multiples, e.g. 2023-03-09
                    # staging-0). parse_slice checks before it adds anything, so skipping is clean.
                    log.warning("%s half-hour %02d unreadable (%s); left out", ar.tag, i, e)
                    continue
                mask |= 1 << i
            t_parse += time.monotonic() - t1
    rows = [
        (f"{h:06x}", day.isoformat(), r[0], r[1], r[2],
         round(r[3] / 1e6, 5), round(r[4] / 1e6, 5), round(r[5] / 1e6, 5), round(r[6] / 1e6, 5))
        for h, r in sorted(agg.items())
    ]
    meta = {"tag": ar.tag, "mask": mask, "slices": mask.bit_count(), "bytes": ar.bytes_read,
            "seconds": time.monotonic() - t0, "locate_s": t_locate, "parse_s": t_parse}
    return meta, rows


# ---------------------------------------------------------------- outputs

def write_csv(outdir: Path, day: dt.date, meta: dict, rows: list[tuple]) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / CSV_NAME.format(day=day)
    tmp = path.with_name(path.name + ".tmp")
    with gzip.open(tmp, "wt", newline="", compresslevel=9) as f:
        f.write(f"#tag={meta['tag']};slices_fetched={meta['mask']};bytes={meta['bytes']};"
                f"reader={READER};source=adsb.lol globe_history (ODbL 1.0)\n")
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
    ap.add_argument("--instance", default=INSTANCES, help="release instances to try, in order")
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
