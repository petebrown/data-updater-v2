"""Bring the Sofascore CSVs in this folder up to date.

Sofascore's API returns 403 to scripted requests, so the fetching happens in
your own browser and this script only builds the browser snippet and merges
what it downloads.

    1. uv run sofascore-json/update_sofascore.py snippet | pbcopy
    2. Open https://www.sofascore.com in Chrome, open DevTools > Console,
       paste and run. It downloads sofascore_new_<date>.json when it's done.
    3. uv run sofascore-json/update_sofascore.py merge ~/Downloads/sofascore_new_<date>.json

New rows are added to the top of each CSV, newest first. Existing rows are
never changed, and IDs already present are skipped.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

TEAM_ID = 51  # Tranmere Rovers

HERE = Path(__file__).parent
MATCH_STATS = HERE / "match_stats.csv"  # columns: match_id, match_stats
MATCHDAY_STATS = HERE / "matchday_stats.csv"  # columns: id, data

csv.field_size_limit(sys.maxsize)

SNIPPET = """\
(async () => {
  const TEAM = %(team)d;
  const haveStats = new Set(%(have_stats)s);
  const haveLineups = new Set(%(have_lineups)s);
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const getJson = async url => { const r = await fetch(url); return r.ok ? r.json() : null; };

  // Page back through results until we hit a page that overlaps the existing data.
  const events = [];
  for (let n = 0; ; n++) {
    const page = await getJson(`/api/v1/team/${TEAM}/events/last/${n}`);
    if (!page) break;
    events.push(...page.events);
    console.log(`Got results page ${n}`);
    if (page.events.some(e => haveStats.has(e.id) || haveLineups.has(e.id)) || !page.hasNextPage) break;
    await sleep(400);
  }

  const todo = events
    .filter(e => e.status.type === 'finished')
    .filter(e => !haveStats.has(e.id) || !haveLineups.has(e.id))
    .sort((a, b) => b.startTimestamp - a.startTimestamp);

  const out = { stats: [], lineups: [] };
  for (const [i, e] of todo.entries()) {
    if (!haveStats.has(e.id)) {
      const d = await getJson(`/api/v1/event/${e.id}/statistics`);
      if (d) out.stats.push({ id: e.id, data: d });
      await sleep(250);
    }
    if (!haveLineups.has(e.id)) {
      const d = await getJson(`/api/v1/event/${e.id}/lineups`);
      if (d) out.lineups.push({ id: e.id, data: d });
      await sleep(250);
    }
    console.log(`${i + 1}/${todo.length}: ${e.homeTeam.name} v ${e.awayTeam.name}`);
  }

  console.log(`Fetched ${out.stats.length} statistics and ${out.lineups.length} lineups`);
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([JSON.stringify(out)], { type: 'application/json' }));
  a.download = `sofascore_new_${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
})();
"""


def read_csv(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        return header, list(reader)


def write_csv(path, header, rows):
    # Matches pandas' to_csv defaults, which the notebook originally used.
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def existing_ids(path):
    _, rows = read_csv(path)
    return {int(r[0]) for r in rows}


def snippet(_args):
    print(
        SNIPPET
        % {
            "team": TEAM_ID,
            "have_stats": json.dumps(sorted(existing_ids(MATCH_STATS))),
            "have_lineups": json.dumps(sorted(existing_ids(MATCHDAY_STATS))),
        }
    )


def match_stats_items(data):
    # Same selection as the notebook: the whole-match period's first group.
    return data["statistics"][0]["groups"][0]["statisticsItems"]


def prepend(path, new_rows):
    header, rows = read_csv(path)
    have = {int(r[0]) for r in rows}
    fresh = [r for r in new_rows if r[0] not in have]
    # str() gives the same Python-repr format pandas wrote for dicts and lists.
    write_csv(path, header, [[i, str(v)] for i, v in fresh] + rows)
    return len(rows), len(fresh)


def merge(args):
    d = json.loads(Path(args.file).read_text())
    before, added = prepend(MATCH_STATS, [(s["id"], match_stats_items(s["data"])) for s in d["stats"]])
    print(f"{MATCH_STATS.name}: {before} rows, added {added}")
    before, added = prepend(MATCHDAY_STATS, [(l["id"], l["data"]) for l in d["lineups"]])
    print(f"{MATCHDAY_STATS.name}: {before} rows, added {added}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(required=True)
    sub.add_parser("snippet", help="print the browser console snippet").set_defaults(func=snippet)
    m = sub.add_parser("merge", help="merge a downloaded sofascore_new_*.json")
    m.add_argument("file")
    m.set_defaults(func=merge)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
