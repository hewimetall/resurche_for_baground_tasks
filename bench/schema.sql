create table if not exists jobs (
    id text primary key,
    status text not null default 'running',
    cancelled bool not null default false,
    fail_reason text,
    expected_n int,
    created_at timestamptz not null default clock_timestamp()
);

create table if not exists expected (
    job_id text not null,
    item int not null,
    sent_at timestamptz not null default clock_timestamp(),
    primary key (job_id, item)
);

create table if not exists attempts (
    id bigserial primary key,
    job_id text not null,
    item int not null,
    worker text,
    pid int,
    note text,
    started_at timestamptz not null default clock_timestamp()
);

create table if not exists results (
    job_id text not null,
    item int not null,
    executions int not null default 1,
    finished_at timestamptz not null default clock_timestamp(),
    primary key (job_id, item)
);

create table if not exists progress (
    job_id text not null,
    step int not null,
    primary key (job_id, step)
);

create table if not exists notes (
    id bigserial primary key,
    ts timestamptz not null default clock_timestamp(),
    job_id text,
    kind text not null,
    body text
);
