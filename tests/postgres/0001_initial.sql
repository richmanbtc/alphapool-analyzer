-- Initial schema for new databases only; production already has these objects.

BEGIN;

CREATE TABLE public.positions (
    id serial PRIMARY KEY,
    "timestamp" bigint,
    model_id text,
    positions jsonb,
    weights jsonb,
    delay double precision,
    exchange text,
    orders jsonb,
    tournament text
);

CREATE INDEX ix_positions_1564027b16549066
    ON public.positions (tournament, "timestamp", model_id);

CREATE UNIQUE INDEX ix_positions_1aa7498365340060
    ON public.positions ("timestamp", model_id);

COMMIT;
