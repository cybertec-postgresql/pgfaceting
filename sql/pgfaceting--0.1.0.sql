CREATE FUNCTION faceting._identifier_append(ident text, append text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT CASE WHEN right(ident, 1) = '"' THEN
        substr(ident, 1, length(ident) - 1) || append || '"'
    ELSE ident || append END;
$$;

CREATE FUNCTION faceting._name_only(ident text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT regexp_replace(ident, '^([^"]*|"([^\"]|\\")*")\.', '');
$$;

CREATE FUNCTION faceting._qualified(schemaname text, tablename text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT format('%s.%s', quote_ident(schemaname), quote_ident(tablename));
$$;

CREATE TABLE faceting.faceted_table (
    table_id oid primary key,
    schemaname text,
    tablename text,
    facets_table text,
    delta_table text,
    key name,
    key_type text,
    chunk_bits int
);

SELECT pg_catalog.pg_extension_config_dump('faceting.faceted_table', '');

CREATE TABLE faceting.facet_definition (
    table_id oid NOT NULL REFERENCES faceted_table (table_id),
    facet_id int NOT NULL,
    facet_name text NOT NULL,
    facet_type text NOT NULL,
    base_column name,
    params jsonb,
    is_multi bool not null,
    supports_delta bool not null,
    PRIMARY KEY (table_id, facet_id)
);

CREATE UNIQUE INDEX facet_definition_uniq_name ON faceting.facet_definition (table_id, facet_name);

SELECT pg_catalog.pg_extension_config_dump('faceting.facet_definition', '');

CREATE FUNCTION faceting.add_faceting_to_table(p_table regclass,
                                               key name,
                                               facets facet_definition[],
                                               chunk_bits int = 20,
                                               keep_deltas bool = true,
                                               populate bool = true)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    schemaname text;
    tablename text;
    facet_tablename text;
    delta_tablename text;
    v_table_id int;
    v_facet_defs faceting.facet_definition[];
    key_type text;
BEGIN
    SELECT relname, nspname INTO tablename, schemaname
        FROM pg_class c JOIN pg_namespace n ON relnamespace = n.oid WHERE c.oid = p_table::oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Cannot find table %', p_table;
    END IF;

    -- Can't make us of highest bit of int4 because don't want to dealt with negative values
    IF chunk_bits NOT BETWEEN 1 AND 31 THEN
        RAISE EXCEPTION 'Invalid number of bits per chunk: %', chunk_bits;
    END IF;

    -- Default chunking size is 1Mi.
    /* TODO: namespace qualify to be in the same schema as parent table */
    facet_tablename := faceting._identifier_append(tablename, '_facets');
    delta_tablename := faceting._identifier_append(tablename, '_facets_deltas');

    SELECT t.typname INTO key_type FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                                       WHERE attrelid = p_table::oid AND attname = key;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Key column % not found in %s.%s', key, schemaname, tablename;
    ELSIF key_type NOT IN ('int2', 'int4', 'int8') THEN
        RAISE EXCEPTION 'Key column type % is not supported.', key_type;
    END IF;

    INSERT INTO faceting.faceted_table (table_id, schemaname, tablename, facets_table, delta_table, key,
                                        key_type, chunk_bits)
    VALUES (p_table::oid, schemaname, tablename, facet_tablename, CASE WHEN keep_deltas THEN delta_tablename END, key,
            key_type, chunk_bits)
    RETURNING table_id INTO v_table_id;

    WITH stored_definitions AS (
        INSERT INTO faceting.facet_definition (table_id, facet_id, facet_name, facet_type, base_column, params, is_multi, supports_delta)
            SELECT v_table_id, assigned_id, facet_name, facet_type, base_column, params, is_multi, supports_delta
            FROM UNNEST(facets) WITH ORDINALITY AS x(_, _, facet_name, facet_type, base_column, params, is_multi, supports_delta, assigned_id)
            RETURNING *)
    SELECT array_agg(f) INTO v_facet_defs FROM stored_definitions f;

    -- Create facet storage
    EXECUTE format($sql$
        CREATE TABLE %s (
            facet_id int4 not null,
            chunk_id int4 not null,
            facet_value text collate "C" null,
            postinglist roaringbitmap not null,
            primary key (facet_id, facet_value, chunk_id)
        );
        ALTER TABLE %s SET (toast_tuple_target = 8160);$sql$,
        faceting._qualified(schemaname, facet_tablename),
        faceting._qualified(schemaname, facet_tablename));

    IF keep_deltas THEN
        -- Delta storage
        EXECUTE format($sql$
            CREATE TABLE %s (
                facet_id int4 not null,
                facet_value text collate "C" null,
                posting %s not null,
                delta int2,
                primary key (facet_id, facet_value, posting)
            );
            $sql$, faceting._qualified(schemaname, delta_tablename), key_type);

        PERFORM faceting.create_delta_trigger(v_table_id);
    END IF;

    IF populate THEN
        PERFORM faceting.populate_facets(v_table_id, false);
    END IF;
END;
$$;

CREATE FUNCTION faceting._get_values_clause(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT faceting.%s_facet_values($1, $2, $3)', fdef.facet_type) INTO result
            USING fdef, extra_cols, table_alias;
    RETURN result;
END;
$$;

CREATE FUNCTION faceting._get_subquery_clause(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT faceting.%s_facet_subquery($1, $2, $3)', fdef.facet_type) INTO result
            USING fdef, extra_cols, table_alias;
    RETURN result;
END;
$$;

CREATE FUNCTION faceting.add_facets(p_table regclass,
                                    facets facet_definition[],
                                    populate bool = true)
    RETURNS SETOF int4
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_table_id oid;
    tdef faceting.faceted_table;
    highest_facet_id int4;
    v_facet_names text[];
    v_facet_ids int4[];
BEGIN
    v_table_id := p_table::oid;
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = v_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % is not faceted', p_table;
    END IF;
    SELECT MAX(facet_id) INTO highest_facet_id FROM faceting.facet_definition WHERE table_id = v_table_id;

    WITH stored_definitions AS (
        INSERT INTO faceting.facet_definition (table_id, facet_id, facet_name, facet_type, base_column, params, is_multi, supports_delta)
            SELECT v_table_id, highest_facet_id + assigned_id, facet_name, facet_type, base_column, params, is_multi, supports_delta
            FROM UNNEST(facets) WITH ORDINALITY AS x(_, _, facet_name, facet_type, base_column, params, is_multi, supports_delta, assigned_id)
            RETURNING *)
    SELECT array_agg(f.facet_name), array_agg(f.facet_id) INTO v_facet_names, v_facet_ids FROM stored_definitions f;

    IF tdef.delta_table IS NOT NULL THEN
        PERFORM faceting.create_delta_trigger(v_table_id);
    END IF;

    IF populate THEN
        PERFORM faceting.populate_facets(v_table_id, false, facets := v_facet_names);
    END IF;
    RETURN QUERY SELECT unnest(v_facet_ids);
END;
$$;

CREATE FUNCTION faceting.drop_facets(p_table regclass,
                                    facets text[])
    RETURNS SETOF text
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_table_id oid;
    tdef faceting.faceted_table;
    v_dropped_names text[];
    v_dropped_ids int4[];
BEGIN
    v_table_id := p_table::oid;
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = v_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % is not faceted', p_table;
    END IF;

    WITH dropped_facets AS (
        DELETE FROM faceting.facet_definition
           WHERE table_id = v_table_id AND facet_name = ANY (facets)
           RETURNING facet_id, facet_name)
    SELECT array_agg(facet_id), array_agg(facet_name) INTO v_dropped_ids, v_dropped_names FROM dropped_facets;

    EXECUTE format('DELETE FROM %s WHERE facet_id = ANY ($1)',
                   faceting._qualified(tdef.schemaname, tdef.facets_table))
        USING v_dropped_ids;
    IF tdef.delta_table IS NOT NULL THEN
        -- Important to replace trigger first so deletion runs with a new snapshot
        PERFORM faceting.create_delta_trigger(v_table_id);
        EXECUTE format('DELETE FROM %s WHERE facet_id = ANY ($1)',
                       faceting._qualified(tdef.schemaname, tdef.delta_table))
            USING v_dropped_ids;
    END IF;

    RETURN QUERY SELECT unnest(v_dropped_names);
END;
$$;

CREATE FUNCTION faceting.populate_facets_query(p_table_id oid, facets text[] = null)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    sql text;
    values_entries text[];
    subquery_entries text[];
    clauses text[];
    v_chunk_bits int;
    v_keycol name;
    tdef faceting.faceted_table;
BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    SELECT chunk_bits, key INTO v_chunk_bits, v_keycol FROM faceting.faceted_table WHERE table_id = p_table_id;
    SELECT array_agg(faceting._get_values_clause(fd, '', 'd.') ORDER BY facet_id) INTO values_entries
            FROM faceting.facet_definition fd WHERE (facets IS NULL OR fd.facet_name = ANY (facets))
                                                    AND table_id = p_table_id AND NOT fd.is_multi;

    SELECT array_agg(faceting._get_subquery_clause(fd, '', 'd.')) INTO subquery_entries
            FROM faceting.facet_definition fd WHERE (facets IS NULL OR fd.facet_name = ANY (facets))
                                                    AND table_id = p_table_id AND fd.is_multi;

    IF array_length(values_entries, 1) > 0 THEN
        clauses := array[format('VALUES %s', array_to_string(values_entries, E',\n               '))];
    ELSE
        clauses := array[];
    END IF;
    clauses := clauses || subquery_entries;

    sql := format($sql$
SELECT facet_id, (%s >> %s)::int4 chunk_id, facet_value collate "POSIX", rb_build_agg((%s & ((1 << %s) - 1))::int4 ORDER BY %s)
FROM %s d,
    LATERAL (
        %s
    ) t(facet_id, facet_value)
GROUP BY facet_id, facet_value collate "POSIX", chunk_id
    $sql$,
        v_keycol,
        v_chunk_bits,
        v_keycol,
        v_chunk_bits,
        v_keycol,
        p_table_id::regclass::text,
        array_to_string(clauses, E'\n            UNION ALL\n        ')
        );
    RETURN sql;
END;
$$;

CREATE FUNCTION create_delta_trigger(p_table_id oid, p_create bool = true)
    RETURNS text
    LANGUAGE plpgsql
AS $$
DECLARE
    tfunc_name text;
    trg_name text;
    sql text;
    tdef faceting.faceted_table;
    insert_values text[];
    insert_subqueries text[];
    insert_clauses text[];
    delete_values text[];
    delete_subqueries text[];
    delete_clauses text[];
    base_columns text[];
BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    tfunc_name := faceting._identifier_append(tdef.tablename, '_facets_trigger');
    trg_name := faceting._identifier_append(tdef.tablename, '_facets_update');

    SELECT array_agg(faceting._get_values_clause(fd, format(', NEW.%I, 1', tdef.key) , 'NEW.')
                     ORDER BY facet_id),
           array_agg(faceting._get_values_clause(fd, format(', OLD.%I, -1', tdef.key) , 'OLD.')
                     ORDER BY facet_id),
           array_agg(fd.base_column)
                INTO insert_values, delete_values, base_columns
            FROM faceting.facet_definition fd WHERE table_id = p_table_id AND NOT fd.is_multi AND fd.supports_delta;

    SELECT array_agg(faceting._get_subquery_clause(fd, format(', NEW.%I, 1', tdef.key) , 'NEW.')
                     ORDER BY facet_id),
           array_agg(faceting._get_subquery_clause(fd, format(', OLD.%I, -1', tdef.key) , 'OLD.')
                     ORDER BY facet_id),
           array_agg(fd.base_column) || base_columns
                INTO insert_subqueries, delete_subqueries, base_columns
            FROM faceting.facet_definition fd WHERE table_id = p_table_id AND fd.is_multi AND fd.supports_delta;

    insert_clauses := CASE WHEN array_length(insert_values, 1) > 0 THEN
            array['VALUES ' || array_to_string(insert_values, E',\n                       ')]
        ELSE
            '{}'::text[]
        END || insert_subqueries;
    delete_clauses := CASE WHEN array_length(delete_values, 1) > 0 THEN
            array['VALUES ' || array_to_string(delete_values, E',\n                       ')]
        ELSE
            '{}'::text[]
        END || delete_subqueries;

    sql := format($sql$
CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $func$
    BEGIN
        IF TG_OP = 'UPDATE' AND OLD.%I != NEW.%I THEN
            RAISE EXCEPTION 'Update of key column of faceted tables is not supported';
        END IF;
        IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
            INSERT INTO %s (facet_id, facet_value, posting, delta)
                %s
                ON CONFLICT (facet_id, facet_value, posting) DO UPDATE
                    SET delta = EXCLUDED.delta + %s.delta;
        END IF;
        IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
            INSERT INTO %s (facet_id, facet_value, posting, delta)
                %s
                ON CONFLICT (facet_id, facet_value, posting) DO UPDATE
                SET delta = EXCLUDED.delta + %s.delta;
        END IF;
        RETURN NULL;
    END;
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER %s
    AFTER INSERT OR DELETE OR UPDATE OF %s ON %s
    FOR EACH ROW EXECUTE FUNCTION %s();
$sql$,
            -- Trigger name
            faceting._qualified(tdef.schemaname, tfunc_name),
            -- Key update check
            tdef.key, tdef.key,
            -- Positive deltas
            faceting._qualified(tdef.schemaname, tdef.delta_table),
            array_to_string(insert_clauses, E'\n                    UNION ALL\n                '),
            faceting._qualified(tdef.schemaname, tdef.delta_table),
            -- Negative deltas
            faceting._qualified(tdef.schemaname, tdef.delta_table),
            array_to_string(delete_clauses, E'\n                    UNION ALL\n                '),
            faceting._qualified(tdef.schemaname, tdef.delta_table),
            -- Trigger definition
            trg_name,
            array_to_string(base_columns, ', '),
            faceting._qualified(tdef.schemaname, tdef.tablename),
            faceting._qualified(tdef.schemaname, tfunc_name)
        );
    IF p_create THEN
        EXECUTE sql;
    END IF;
    RETURN sql;
END;
$$;

CREATE FUNCTION faceting.populate_facets(p_table_id oid, p_use_copy bool = false, debug bool = false, facets text[] = null)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    tdef faceting.faceted_table;
    query text;
    sql text;
BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    query := faceting.populate_facets_query(p_table_id, facets => facets);
    IF p_use_copy THEN
        EXECUTE format($copy$COPY %s FROM PROGRAM $prog$ psql -h localhost %s -c "COPY (%s) TO STDOUT" $prog$ $copy$,
            faceting._qualified(tdef.schemaname, tdef.facets_table),
            current_database(),
            replace(query, '"', '\"'));
        RETURN;
    END IF;
    sql := format('INSERT INTO %s %s', faceting._qualified(tdef.schemaname, tdef.facets_table), query);
    IF debug THEN
        RAISE NOTICE '%s', sql;
    END IF;
    EXECUTE sql;
END;
$$;

CREATE FUNCTION faceting.datetrunc_facet(col name, "precision" text, p_facet_name text = null)
    RETURNS facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'datetrunc', col, jsonb_build_object('precision', "precision"), false, true;
    $$;

CREATE FUNCTION faceting.datetrunc_facet_values(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, date_trunc(%L, %s%I)::text%s)',
                fdef.facet_id, fdef.params->>'precision', table_alias, fdef.base_column, extra_cols);
        END;
    $$;

CREATE FUNCTION faceting.plain_facet(col name, p_facet_name text = null)
    RETURNS facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'plain', col, '{}'::jsonb, false, true;
    $$;

CREATE FUNCTION faceting.plain_facet_values(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, %s%I::text%s)', fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

CREATE FUNCTION faceting.bucket_facet(col name, buckets anyarray, p_facet_name text = null)
    RETURNS facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'bucket', col, jsonb_build_object('buckets', buckets::text), false, true;
    $$;

CREATE FUNCTION faceting.bucket_facet_values(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, width_bucket(%s%I, %L)::text%s)',
                fdef.facet_id, table_alias, fdef.base_column, fdef.params->>'buckets', extra_cols);
        END;
    $$;

CREATE FUNCTION faceting.array_facet(col name, p_facet_name text = null)
    RETURNS facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'array', col, '{}'::jsonb, true, true;
    $$;

CREATE FUNCTION faceting.array_facet_subquery(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(SELECT %s, element_value::text%s FROM unnest(%s%I) element_value)',
                fdef.facet_id, extra_cols, table_alias, fdef.base_column);
        END;
    $$;

CREATE FUNCTION faceting.joined_plain_facet(col text, from_clause text, correlation text, p_facet_name text = null)
    RETURNS facet_definition
    LANGUAGE plpgsql AS $$
        DECLARE
            base_col_name text;
        BEGIN
            SELECT ident[array_upper(ident, 1)] INTO base_col_name FROM parse_ident(col) ident;
            RETURN row(null::oid, null::int, coalesce(p_facet_name, base_col_name), 'joined_plain'::text, NULL::name,
                jsonb_build_object('col', col, 'from_clause', from_clause, 'correlation', correlation),
                true, false);
        END;
    $$;

CREATE FUNCTION faceting.joined_plain_facet_subquery(fdef facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        DECLARE
            correlation_clause text;
        BEGIN
            correlation_clause := replace(fdef.params->>'correlation', '{TABLE}.', table_alias);
            RETURN format('(SELECT %s, %s::text%s FROM %s WHERE %s)',
                fdef.facet_id, fdef.params->>'col', extra_cols, fdef.params->>'from_clause', correlation_clause);
        END;
    $$;

CREATE TYPE faceting.facet_counts AS (
    facet_name text,
    facet_value text,
    cardinality int8
);

CREATE FUNCTION faceting.top_values(p_table_id oid, n int = 5, facets text[] = null)
    RETURNS SETOF faceting.facet_counts
    LANGUAGE plpgsql
AS $$
DECLARE
    tdef faceting.faceted_table;
    facet_filter text = '';
BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    IF facets IS NOT NULL THEN
        SELECT format('WHERE facet_id = ANY (''%s'')', array_agg(facet_id)::text) INTO facet_filter
            FROM faceting.facet_definition fd WHERE fd.facet_name = ANY (facets);
    END IF;

    RETURN QUERY EXECUTE format($sql$
        SELECT facet_name, facet_value, sum::int8 FROM (
            SELECT facet_id, facet_value, sum, rank() OVER (PARTITION BY facet_id ORDER BY sum DESC) rank
            FROM (
                SELECT facet_id, facet_value, sum(rb_cardinality(postinglist))
                FROM %s
                %s
                GROUP BY 1, 2
                ) x
            ) counts JOIN faceting.facet_definition fd USING (facet_id)
        WHERE rank <= 5 AND table_id = $1
        ORDER BY facet_id, rank, facet_value;
    $sql$,
        faceting._qualified(tdef.schemaname, tdef.facets_table),
        facet_filter)
    USING p_table_id;
END;
$$;

CREATE TYPE faceting.facet_filter AS
(
    facet_name  text,
    facet_value text
);

CREATE FUNCTION faceting.count_results(p_table_id oid, filters facet_filter[])
    RETURNS SETOF faceting.facet_counts
    LANGUAGE plpgsql
AS $$
DECLARE
    tdef faceting.faceted_table;
    select_facets int[];
    sql text;
BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    SELECT array_agg(facet_id) INTO select_facets
        FROM faceting.facet_definition
        WHERE table_id = p_table_id
          AND facet_name NOT IN (SELECT f.facet_name FROM unnest(filters) f);

    sql := format($sql$
    WITH filters AS (
        SELECT facet_id, facet_name, facet_value
            FROM faceting.facet_definition JOIN unnest($1) t USING (facet_name)
            WHERE table_id = $2
    ), lookup AS (
        SELECT chunk_id, rb_and_agg(postinglist) postinglist
            FROM %s d JOIN filters USING (facet_id, facet_value)
            GROUP BY chunk_id
    ), results AS (
    SELECT facet_id, facet_value, sum(rb_and_cardinality(lookup.postinglist, d.postinglist))::int8 cardinality
        FROM lookup JOIN %s d USING (chunk_id)
        WHERE facet_id = ANY ($3)
        GROUP BY facet_id, facet_value
    )
    SELECT facet_name, facet_value, cardinality
    FROM results JOIN faceting.facet_definition fd USING (facet_id)
    WHERE fd.table_id = $2
    ORDER BY facet_id, cardinality DESC, facet_value
    $sql$,
        faceting._qualified(tdef.schemaname, tdef.facets_table),
        faceting._qualified(tdef.schemaname, tdef.facets_table));

    RETURN QUERY EXECUTE sql USING filters, p_table_id, select_facets;
END;
$$;

CREATE FUNCTION faceting.merge_deltas(p_table_id oid)
    RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
	sql text;
    tdef faceting.faceted_table;
 BEGIN
    SELECT t.* INTO tdef FROM faceting.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

	sql := format($sql$
WITH to_be_aggregated AS (DELETE FROM %s RETURNING *),
chunk_deltas AS (
    SELECT facet_id,
		   (posting >> %s) chunk_id,
		   facet_value,
		   coalesce(rb_build_agg((posting & ((1<<%s) - 1))::int4) FILTER (WHERE delta > 0), '\x3a30000000000000') AS postings_added,
		   coalesce(rb_build_agg((posting & ((1<<%s) - 1))::int4) FILTER (WHERE delta < 0), '\x3a30000000000000') AS postings_deleted
    FROM to_be_aggregated
    GROUP BY 1,2,3
),
updates AS (UPDATE %s AS d
    SET postinglist = rb_or(rb_andnot(postinglist, postings_deleted), postings_added)
    FROM chunk_deltas
    WHERE d.facet_id = chunk_deltas.facet_id
        AND d.facet_value = chunk_deltas.facet_value
        AND d.chunk_id = chunk_deltas.chunk_id)
INSERT INTO %s SELECT facet_id, chunk_id, facet_value, postings_added
    FROM chunk_deltas
    ON CONFLICT (facet_id, facet_value, chunk_id) DO NOTHING;
		$sql$,
		faceting._qualified(tdef.schemaname, tdef.delta_table),
		tdef.chunk_bits, tdef.chunk_bits, tdef.chunk_bits,
		faceting._qualified(tdef.schemaname, tdef.facets_table),
        faceting._qualified(tdef.schemaname, tdef.facets_table)
	);
    EXECUTE sql;
	RETURN;
END;
$$;

CREATE PROCEDURE faceting.run_maintenance(debug bool = false)
	LANGUAGE plpgsql
	AS $$
DECLARE
	tdef faceting.faceted_table;
	start_ts timestamptz;
	end_ts timestamptz;
BEGIN
		FOR tdef IN SELECT * FROM faceting.faceted_table LOOP
			IF debug THEN
				RAISE NOTICE 'Starting facets maintenance of %', tdef.tablename;
			END IF;
			start_ts := clock_timestamp();
			PERFORM faceting.merge_deltas(tdef.table_id);
			COMMIT;
			end_ts := clock_timestamp();
			IF debug THEN
				RAISE NOTICE 'End facets maintenance of %, duration: %s', tdef.tablename, end_ts - start_ts;
			END IF;
		END LOOP;
END;
$$;

