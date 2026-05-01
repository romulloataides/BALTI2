# Supabase setup (Phases 5, 6, and 7)

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
5. Open [`supabase/migrations/002_ai_admin_pilot.sql`](./migrations/002_ai_admin_pilot.sql).
6. Copy/paste the SQL into the editor and run it too.

This migration creates the live `reports` + `votes` tables and the `report_vote_counts` view that the dashboard now reads for community report cards and map markers.

If you already ran an older version of the schema, rerun the full migration so the view picks up:
- `lat` / `lng` for report markers
- `security_invoker = true`
- `grant select on public.report_vote_counts to anon, authenticated`
- `source`, `pilot_slug`, `block_label`, `observed_on`, and `metadata` on `reports`
- `pilot_accuracy_votes` and `pilot_accuracy_vote_counts`
- `admin_users`, `analysis_prompt_profiles`, `analysis_sessions`, `analysis_messages`, and `spending_events`

## 3. Allowlist admin emails

The live Phase 6 desk now requires a real authenticated admin account. Add at least one allowlisted email:

```sql
insert into public.admin_users (email, role)
values ('you@example.org', 'admin')
on conflict do nothing;
```

## 4. Get API credentials

1. In Supabase Dashboard, open **Settings -> API**.
2. Copy:
- **Project URL**
- **anon/public key**

## 5. Configure local credentials

1. Open [`supabase/config.js`](./config.js).
2. Replace placeholders:
- `window.SUPABASE_URL`
- `window.SUPABASE_ANON_KEY`
3. Leave these feature flags `false` until the extra Phase 6 / 7 backend pieces are actually live:
- `window.BALTI2_ENABLE_ANALYSIS_DESK`
- `window.BALTI2_ENABLE_PILOT_VOTES`

Important: use the Supabase **anon/public** key only. This dashboard is a static client app, so the anon key is expected to be browser-visible and can be committed for GitHub Pages deployment. Never put a Supabase `service_role` key in this repo.

## 6. Configure auth redirect URLs

The admin desk uses Supabase Auth magic links. In **Authentication -> URL Configuration**, allow the dashboard URLs you actually use, for example:

- your GitHub Pages URL
- any local preview URL you use during QA

If the redirect URL is not allowed, admin sign-in will fail even though the UI is wired correctly.

## 7. Add Edge Function secrets

In **Edge Functions -> Secrets**, set:

- `OPENAI_API_KEY` required
- `OPENAI_MODEL` optional, defaults to `gpt-5.4`
- `DASHBOARD_DATA_URL` optional, defaults to the published `BALTI2/data.json`

The `analysis-desk` function uses the built-in Supabase secrets too:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY` / `SB_PUBLISHABLE_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`

## 8. Deploy the analysis function

Deploy [`supabase/functions/analysis-desk/index.ts`](./functions/analysis-desk/index.ts) with your normal Supabase function workflow.

That function:

- verifies the caller's Supabase session
- checks the caller against `public.admin_users`
- runs a real OpenAI Responses API tool loop
- stores analysis sessions and messages in Supabase
- can query:
  - dashboard neighborhood data
  - live reports
  - 311 proxy history
  - spending records
  - pilot accuracy votes

## 9. Turn on the Phase 6 / 7 feature flags

After the migration and Edge Function are really live:

- set `window.BALTI2_ENABLE_ANALYSIS_DESK = true`
- set `window.BALTI2_ENABLE_PILOT_VOTES = true`

Until then, keeping them `false` gives you a cleaner public demo because the frontend will not probe undeployed endpoints.

## 10. Optional CI secrets (GitHub)

If CI or deployment workflows need Supabase credentials, add repository secrets:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

If your deployment workflow will also deploy the analysis function, add:

- `OPENAI_API_KEY`
- optional `OPENAI_MODEL`
- optional `DASHBOARD_DATA_URL`

GitHub path: **Repo -> Settings -> Secrets and variables -> Actions**.
