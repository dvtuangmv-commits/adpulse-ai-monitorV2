# AdPulse AI Monitor V2

Live-only ad monitoring dashboard for TikTok Ads and Meta Ads with 1h/3h/5h analysis windows, traceable diagnosis, CSV import, optional OpenAI explanation, and Sora 2 / Sora 2 Pro video generation.

## Deployment on Render

Build command:
```
pip install -r requirements.txt
```

Start command:
```
python app.py
```

## Required environment variables for real data

TikTok:
- `TIKTOK_ACCESS_TOKEN`
- `TIKTOK_ADVERTISER_ID`

Meta:
- `META_ACCESS_TOKEN`
- `META_AD_ACCOUNT_ID`
- optional `META_GRAPH_VERSION` (defaults to v26.0 in this package)

OpenAI (optional for AI explanation/video):
- `OPENAI_API_KEY`
- optional `OPENAI_VIDEO_MODEL` (`sora-2` or `sora-2-pro`)

## Data honesty rules

- No synthetic/demo data is seeded.
- 1h/3h/5h analysis uses hourly data only.
- The app does not invent or interpolate conversion revenue/ROAS when the platform exposes those metrics at a coarser granularity.
- Diagnosis is based first on measurable deltas versus the immediately preceding equivalent window. AI explanation is optional and is constrained to the provided observations.
