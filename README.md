# Recruiting Tracker

A personal site that checks a set of job boards once a day for new internship and
externship openings (business / strategy / finance track, US or remote), flags what's
new, verifies each posting is still live, and links straight to the application.

## How it runs
- `sources.json` lists the boards to check.
- `pipeline.py --live` fetches, filters, de-duplicates, flags new roles, and re-verifies
  each one before publishing to `data/jobs.json`. Nothing unverified is ever published.
- `build_site.py` renders `docs/index.html` from `data/jobs.json`.
- `.github/workflows/update.yml` runs both once a day and commits the results.
- GitHub Pages serves `docs/`.

## Add a board
Edit `sources.json`. Copy an entry and fill in the token (Greenhouse), slug (Lever),
or collection id + host (Getro). No code changes needed.
