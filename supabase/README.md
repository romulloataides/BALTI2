# Supabase setup (Phase 5)

This project can run with or without Supabase. If Supabase is not configured, report and vote actions fall back to offline behavior.

## 1. Create a Supabase project

1. Go to `https://supabase.com` and sign in.
2. Create a new project (free tier is enough for development).
3. Wait for project provisioning to complete.

## 2. Run the schema migration

1. Open your project in Supabase Dashboard.
2. Go to **SQL Editor**.
3. Open [`supabase/migrations/001_initial_schema.sql`](./migrations/001_initial_schema.sql).
4. Copy/paste the SQL into the editor and run it.

This migration creates the live `reports` + `votes` tables and the `report_vote_counts` view that the dashboard now reads for community report cards and map markers.

If you already ran an older version of the schema, rerun the full migration so the view picks up:
- `lat` / `lng` for report markers
- `security_invoker = true`
- `grant select on public.report_vote_counts to anon, authenticated`

## 3. Get API credentials

1. In Supabase Dashboard, open **Settings -> API**.
2. Copy:
- **Project URL**
- **anon/public key**

## 4. Configure local credentials

1. Open [`supabase/config.js`](./config.js).
2. Replace placeholders:
- `window.SUPABASE_URL`
- `window.SUPABASE_ANON_KEY`

Important: use the Supabase **anon/public** key only. This dashboard is a static client app, so the anon key is expected to be browser-visible and can be committed for GitHub Pages deployment. Never put a Supabase `service_role` key in this repo.

## 5. Optional CI secrets (GitHub)

If CI or deployment workflows need Supabase credentials, add repository secrets:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

GitHub path: **Repo -> Settings -> Secrets and variables -> Actions**.
