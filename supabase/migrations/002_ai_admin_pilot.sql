alter table public.reports
  add column if not exists source text not null default 'dashboard',
  add column if not exists pilot_slug text,
  add column if not exists block_label text,
  add column if not exists observed_on date,
  add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists reports_source_scope_idx
  on public.reports (source, pilot_slug, nsa, created_at desc);

drop view if exists public.report_vote_counts;

create view public.report_vote_counts
with (security_invoker = true) as
  select r.id, r.tracking_id, r.nsa, r.category, r.description,
         r.severity, r.author_role, r.status, r.created_at,
         r.lat, r.lng, r.source, r.pilot_slug, r.block_label,
         r.observed_on, r.metadata,
         count(v.id) filter (where v.vote_type = 'confirm') as confirms,
         count(v.id) filter (where v.vote_type = 'dispute') as disputes
  from public.reports r
  left join public.votes v on v.report_id = r.id
  group by r.id;

grant select on public.report_vote_counts to anon, authenticated;

create table if not exists public.admin_users (
  id         bigint generated always as identity primary key,
  user_id    uuid references auth.users(id) on delete set null,
  email      text not null,
  role       text not null default 'admin' check (role in ('admin', 'moderator', 'analyst')),
  created_at timestamptz not null default now()
);

create unique index if not exists admin_users_email_lower_idx
  on public.admin_users (lower(email));

create table if not exists public.analysis_prompt_profiles (
  id           bigint generated always as identity primary key,
  slug         text not null unique,
  label        text not null,
  system_prompt text not null,
  context_note text,
  created_at   timestamptz not null default now()
);

create table if not exists public.analysis_sessions (
  id           uuid primary key default gen_random_uuid(),
  user_id      uuid references auth.users(id) on delete set null,
  admin_email  text not null,
  profile_slug text not null references public.analysis_prompt_profiles(slug) on update cascade,
  pilot_slug   text,
  title        text not null default 'New analysis session',
  status       text not null default 'active' check (status in ('active', 'archived')),
  scope        jsonb not null default '{}'::jsonb,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

create index if not exists analysis_sessions_scope_idx
  on public.analysis_sessions (admin_email, updated_at desc);

create table if not exists public.analysis_messages (
  id           uuid primary key default gen_random_uuid(),
  session_id   uuid not null references public.analysis_sessions(id) on delete cascade,
  role         text not null check (role in ('user', 'assistant', 'tool', 'system')),
  content      text not null,
  tool_name    text,
  tool_payload jsonb,
  created_at   timestamptz not null default now()
);

create index if not exists analysis_messages_session_idx
  on public.analysis_messages (session_id, created_at);

create table if not exists public.spending_events (
  id           uuid primary key default gen_random_uuid(),
  nsa          text not null,
  category     text not null,
  program_name text not null,
  amount       numeric(12,2),
  started_on   date,
  completed_on date,
  status       text not null default 'planned' check (status in ('planned', 'active', 'completed', 'cancelled')),
  source       text not null default 'manual',
  details      text,
  created_at   timestamptz not null default now()
);

create index if not exists spending_events_scope_idx
  on public.spending_events (nsa, category, created_at desc);

create table if not exists public.pilot_accuracy_votes (
  id         uuid primary key default gen_random_uuid(),
  pilot_slug text not null,
  nsa        text not null,
  issue_key  text not null,
  response   text not null check (response in ('yes', 'no', 'not_sure')),
  voter_fp   text not null,
  created_at timestamptz not null default now(),
  unique (pilot_slug, nsa, issue_key, voter_fp)
);

create index if not exists pilot_accuracy_votes_scope_idx
  on public.pilot_accuracy_votes (pilot_slug, nsa, issue_key, created_at desc);

create or replace view public.pilot_accuracy_vote_counts
with (security_invoker = true) as
  select pilot_slug,
         nsa,
         issue_key,
         count(*) filter (where response = 'yes') as yes_count,
         count(*) filter (where response = 'no') as no_count,
         count(*) filter (where response = 'not_sure') as not_sure_count,
         count(*) as total_votes,
         max(created_at) as updated_at
  from public.pilot_accuracy_votes
  group by pilot_slug, nsa, issue_key;

alter table public.admin_users enable row level security;
alter table public.analysis_prompt_profiles enable row level security;
alter table public.analysis_sessions enable row level security;
alter table public.analysis_messages enable row level security;
alter table public.spending_events enable row level security;
alter table public.pilot_accuracy_votes enable row level security;

drop policy if exists "admin users self read" on public.admin_users;
drop policy if exists "admin profile read" on public.analysis_prompt_profiles;
drop policy if exists "admin session manage" on public.analysis_sessions;
drop policy if exists "admin message manage" on public.analysis_messages;
drop policy if exists "admin spending read" on public.spending_events;
drop policy if exists "public read pilot votes" on public.pilot_accuracy_votes;
drop policy if exists "public insert pilot votes" on public.pilot_accuracy_votes;
drop policy if exists "public update pilot votes" on public.pilot_accuracy_votes;

create policy "admin users self read"
  on public.admin_users
  for select
  to authenticated
  using (lower(email) = lower(coalesce(auth.jwt() ->> 'email', '')));

create policy "admin profile read"
  on public.analysis_prompt_profiles
  for select
  to authenticated
  using (
    exists (
      select 1
      from public.admin_users au
      where lower(au.email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  );

create policy "admin session manage"
  on public.analysis_sessions
  for all
  to authenticated
  using (
    auth.uid() = user_id
    and exists (
      select 1
      from public.admin_users au
      where lower(au.email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  )
  with check (
    auth.uid() = user_id
    and lower(admin_email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    and exists (
      select 1
      from public.admin_users au
      where lower(au.email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  );

create policy "admin message manage"
  on public.analysis_messages
  for all
  to authenticated
  using (
    exists (
      select 1
      from public.analysis_sessions s
      where s.id = session_id
        and s.user_id = auth.uid()
        and lower(s.admin_email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  )
  with check (
    exists (
      select 1
      from public.analysis_sessions s
      where s.id = session_id
        and s.user_id = auth.uid()
        and lower(s.admin_email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  );

create policy "admin spending read"
  on public.spending_events
  for select
  to authenticated
  using (
    exists (
      select 1
      from public.admin_users au
      where lower(au.email) = lower(coalesce(auth.jwt() ->> 'email', ''))
    )
  );

create policy "public read pilot votes"
  on public.pilot_accuracy_votes
  for select
  using (true);

create policy "public insert pilot votes"
  on public.pilot_accuracy_votes
  for insert
  with check (true);

create policy "public update pilot votes"
  on public.pilot_accuracy_votes
  for update
  using (true)
  with check (true);

grant select on public.admin_users to authenticated;
grant select on public.pilot_accuracy_vote_counts to anon, authenticated;
grant select, insert, update on public.pilot_accuracy_votes to anon, authenticated;

do $$
begin
  if not exists (
    select 1
    from pg_publication_tables
    where pubname = 'supabase_realtime'
      and schemaname = 'public'
      and tablename = 'pilot_accuracy_votes'
  ) then
    alter publication supabase_realtime add table public.pilot_accuracy_votes;
  end if;
end
$$;

insert into public.analysis_prompt_profiles (slug, label, system_prompt, context_note)
values
  (
    'default',
    'Baltimore admin analyst',
    'You are the BALTI2 analysis desk for Baltimore community health, reports, and pilot operations. Use the provided tools before making specific claims about neighborhood trends, community reports, 311 service history, or spending records. Treat derived metrics, proxy metrics, and scaffolded benchmarks carefully: state when a result is proxy, derived, or missing. Be concise, evidence-led, and operational. When the user asks for priorities, surface the strongest issues first, mention uncertainty directly, and suggest the next best data pull when evidence is incomplete.',
    'General-purpose prompt for the admin analysis desk.'
  ),
  (
    'pilot',
    'Carrollton pilot analyst',
    'You are the BALTI2 pilot analysis desk focused on Carrollton Ridge / Franklin Square workflows. Prioritize illegal dumping, broadband access, resident-submitted reports, pilot accuracy votes, and any available spending or intervention records. Keep the distinction between official data, community-signal proxies, and direct resident submissions explicit. When evidence is mixed, name the blind spot and say what a Digital Navigator or city analyst should verify next in the field.',
    'Focused prompt for the Carrollton Ridge pilot workflow.'
  )
on conflict (slug) do update
set label = excluded.label,
    system_prompt = excluded.system_prompt,
    context_note = excluded.context_note;
