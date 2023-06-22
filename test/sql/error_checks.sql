CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pgfaceting;

CREATE SCHEMA errorchecks;
SET SEARCH_PATH = 'errorchecks', 'public';

CREATE TABLE uuid_based_docs (
    id uuid primary key,
    type text
);

CREATE TABLE text_based_docs (
    id text primary key,
    type text
);

CREATE TABLE int4_based_docs (
    id int4 primary key,
    type text
);

-- Expected error on key col not existing
SELECT faceting.add_faceting_to_table('uuid_based_docs',
        key => 'nonexistent',
        facets => array[faceting.plain_facet('type')]
    );

-- Expected error on wrong type key column
SELECT faceting.add_faceting_to_table('uuid_based_docs',
        key => 'id',
        facets => array[faceting.plain_facet('type')]
    );
SELECT faceting.add_faceting_to_table('text_based_docs',
        key => 'id',
        facets => array[faceting.plain_facet('type')]
    );

-- Wrong chunk bits
SELECT faceting.add_faceting_to_table('int4_based_docs',
        key => 'id',
        chunk_bits => 42,
        facets => array[faceting.plain_facet('type')]
    );


-- Expected no error
SELECT faceting.add_faceting_to_table('int4_based_docs',
        key => 'id',
        facets => array[faceting.plain_facet('type')]
    );
