-- Reports: community-submitted neighborhood conditions
create table if not exists public.reports (
  id           uuid primary key default gen_random_uuid(),
  tracking_id  text generated always as ('BLT-' || upper(substr(id::text,1,8))) stored,
  nsa          text not null,
  category     text not null,
  description  text not null,
  severity     text not null default 'medium',
  author_role  text not null default 'anonymous',
  lat          float,
  lng          float,
  status       text not null default 'open' check (status in ('open','review','verified','closed')),
  created_at   timestamptz not null default now()
);

create table if not exists public.votes (
  id         uuid primary key default gen_random_uuid(),
  report_id  uuid not null references public.reports(id) on delete cascade,
  vote_type  text not null check (vote_type in ('confirm','dispute')),
  voter_fp   text,
  created_at timestamptz not null default now(),
  unique(report_id, voter_fp, vote_type)
);

create table if not exists public.annotations (
  id          uuid primary key default gen_random_uuid(),
  nsa         text not null,
  metric      text,
  body        text not null,
  author_role text not null default 'resident',
  created_at  timestamptz not null default now()
);

create or replace view public.report_vote_counts
with (security_invoker = true) as
  select r.id, r.tracking_id, r.nsa, r.category, r.description,
         r.severity, r.author_role, r.status, r.created_at,
         r.lat, r.lng,
         count(v.id) filter (where v.vote_type = 'confirm') as confirms,
         count(v.id) filter (where v.vote_type = 'dispute') as disputes
  from public.reports r
  left join public.votes v on v.report_id = r.id
  group by r.id;

alter table public.reports     enable row level security;
alter table public.votes       enable row level security;
alter table public.annotations enable row level security;

create policy "public read reports"           on public.reports     for select using (true);
create policy "public insert reports"         on public.reports     for insert with check (true);
create policy "public read votes"             on public.votes       for select using (true);
create policy "public insert votes"           on public.votes       for insert with check (true);
create policy "public read annotations"       on public.annotations for select using (true);
create policy "public insert annotations"     on public.annotations for insert with check (true);

grant select on public.report_vote_counts to anon, authenticated;
