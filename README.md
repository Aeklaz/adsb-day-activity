# adsb-day-activity

Builds a small day-by-day activity table for US-registered aircraft from the public
[adsb.lol](https://www.adsb.lol/docs/open-data/historical) daily archives.

For each day it reads only the `heatmap/` section of that day's archive (about 0.8 of 3.4 GB,
fetched with HTTP range requests) and writes one row per aircraft seen:

| column | meaning |
|---|---|
| `hex` | ICAO 24-bit address (US block `a00000`-`afffff` by default) |
| `obs_date` | UTC day |
| `points` | position reports, one per 10 seconds, so `points x 10 s` is time observed |
| `airborne_points` | of those, reports not flagged "on ground" |
| `slices` | bitmask of the half-hours (UTC) in which the aircraft appeared; bit 0 = 00:00-00:30 |
| `first_lat`, `first_lon`, `last_lat`, `last_lon` | first and last position of the day |

The first line of every file is a `#key=value;...` comment naming the release it came from and
which half-hours were read. An aircraft with no row was not seen in the half-hours read; that is
not the same as "did not fly" (no ADS-B Out, or out of receiver range, look identical).

## Run it

GitHub: **Actions > Pull day activity > Run workflow**, give a first and last day. The finished
run has one artifact, `adsb-day-activity_<start>_<end>`, containing `adsb_day_<date>.csv.gz` files.

Locally (Python 3.10+, no dependencies):

    python fetch_heatmap.py --from 2026-11-06 --to 2026-11-08 --out out

## Data licence and attribution

Source data: adsb.lol `globe_history`, (c) adsb.lol contributors, made available under the
[Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/). The files this
repository produces are a derivative database and are made available under the same licence.
