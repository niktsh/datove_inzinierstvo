-- Prehľad data lake pre informačný balíček (iba čítanie, SELECT).
-- Spustenie na serveri:
--   docker compose -f docker-compose.prod.yml exec -T postgres sh -c 'psql -U "$POSTGRES_USER" "$POSTGRES_DB"' < tools/lake_prehlad.sql > /tmp/out.txt
\pset pager off
\echo === Q1 team/channel
select team, channel, count(*) n, min(received_at)::timestamp(0) od, max(received_at)::timestamp(0) do_, count(*) filter (where payload_json is null) json_null, string_agg(distinct content_type, ' ; ') ct from lake.message group by 1,2 order by 1,2;
\echo === Q1b spolu
select count(*) spolu, now()::timestamp(0) cas from lake.message;
\echo === Q2 nase udalosti
select payload_json->>'event_type' typ, payload_json->>'source' src, count(*) from lake.message where team='tuke-krakow' group by 1,2 order by 3 desc;
\echo === Q3 team_7 sse udalost a tabulka
select source_ref->>'event' ev, payload_json->>'table' tbl, count(*) from lake.message where team='team_7' and channel='sse' group by 1,2 order by 3 desc;
\echo === Q4 team_7 observations
with s as (select payload_json p from lake.message where team='team_7' and channel='sse' and source_ref->>'event'='snapshot' and payload_json->>'table' like '%observations'), r as (select p->>'table' t, x from s, jsonb_array_elements(p->'rows') x) select t, count(*) riadkov, count(*) filter (where x->>'is_sold'='true') sold_true, count(*) filter (where x->>'is_sold'='false') sold_false, string_agg(distinct x->>'currency', ',') meny, count(*) filter (where x->>'seats_available' is not null) s_miestami, count(*) filter (where x->>'flight_number' is null) bez_cisla from r group by 1;
\echo === Q5 team_7 rest divadlo
with s as (select payload_json->'data' d from lake.message where team='team_7' and channel='rest' and jsonb_typeof(payload_json->'data')='array'), r as (select x from s, jsonb_array_elements(d) x) select x->>'venue' venue, x->>'currency' mena, count(*) n, count(*) filter (where x->>'is_sold'='true') sold_true, min(x->>'starts_at') od, max(x->>'starts_at') do_ from r group by 1,2 order by 3 desc;
\echo === Q6a team_4 flights kluce
with f as (select x from lake.message m, jsonb_array_elements(case when jsonb_typeof(m.payload_json->'flights')='array' then m.payload_json->'flights' else '[]'::jsonb end) x where m.team='team_4' and m.channel='rest_flights') select k, count(*) from f, jsonb_object_keys(x) k group by 1 order by 2 desc;
\echo === Q6b team_4 flights kvalita
with f as (select x from lake.message m, jsonb_array_elements(case when jsonb_typeof(m.payload_json->'flights')='array' then m.payload_json->'flights' else '[]'::jsonb end) x where m.team='team_4' and m.channel='rest_flights') select count(*) zaznamov, count(distinct (x->>'origin', x->>'destination', x->>'flight_number', x->>'departure_at')) unikatnych, count(*) filter (where coalesce(x->>'flight_number','')='') bez_cisla, string_agg(distinct x->>'destination', ',') ciele, count(*) filter (where x->>'departure_at' ~ '(Z|[+-][0-9]{2}:[0-9]{2})$') s_pasmom, min(x->>'departure_at') od, max(x->>'departure_at') do_ from f;
\echo === Q7 team_4 divadlo
with t as (select x from lake.message m, jsonb_array_elements(case when jsonb_typeof(m.payload_json)='array' then m.payload_json else '[]'::jsonb end) x where m.team='team_4' and m.channel='rest_theatre') select case when x ? 'event_name' then 'availability' else 'tickets_sold' end pohlad, count(*) riadkov, count(distinct coalesce(x->>'event_name', x->>'event')) inscenacii, string_agg(distinct x->>'currency', ',') meny, count(*) filter (where coalesce(x->>'performance_datetime', x->>'performance_date') ~ '(Z|[+-][0-9]{2}:[0-9]{2})$') s_pasmom, min(coalesce(x->>'performance_datetime', x->>'performance_date')) od, max(coalesce(x->>'performance_datetime', x->>'performance_date')) do_ from t group by 1;
\echo === Q8 team_5 webhook
select payload_json->>'title' title, payload_json->>'status' st, count(*) n, min(received_at)::date od, max(received_at)::date do_ from lake.message where team='team_5' and channel='webhook' group by 1,2 order by 3 desc limit 15;
\echo === Q9 team_5 s3 vzorka 3 percenta
with s as (select payload_json pj from lake.message tablesample system (3) where team='team_5' and channel='s3' and jsonb_typeof(payload_json)='array'), i as (select e->'metadata'->>'scraper_id' sc, e->'payload' p from s, jsonb_array_elements(pj) e) select sc, count(*) items, count(*) filter (where p->>'is_direct'='false') s_prestupmi, count(*) filter (where p->>'tickets_sold' is not null) ma_sold, count(*) filter (where p->>'flight_number' is null) bez_cisla, string_agg(distinct p->>'currency', ',') meny, count(*) filter (where coalesce(p->>'departure_time', p->>'departureTime', '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2})?([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})?$') zly_cas from i group by 1 order by 2 desc;
\echo === Q9b team_5 s3 ciele
with s as (select payload_json pj from lake.message tablesample system (3) where team='team_5' and channel='s3' and jsonb_typeof(payload_json)='array'), i as (select e->'payload' p from s, jsonb_array_elements(pj) e) select p->>'destination' ciel, count(*) from i group by 1 order by 2 desc limit 8;
