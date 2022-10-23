# pgfaceting

PostgreSQL extension to quickly calculate facet counts using inverted index built with
[roaring bitmaps](https://roaringbitmap.org/). Requires
[pg_roaringbitmap](https://github.com/ChenHuajun/pg_roaringbitmap) to be installed.

## Build and install

    make install
    make installcheck
    psql -c "create extension roaringbitmap" -c "create extension pgfaceting"
 
## Usage

pgfaceting creates and maintains two extra tables for your main table. `tbl_facets` contains for each facet and
value combination a list of id values for rows containing that combination. The list is stored as a roaring
bitmap for quick intersection and cardinality operations. Because updating this list is a heavy operation any changes
to the main table get stored in `tbl_facets_deltas` as a combination of facet, value, id and +1 or -1 depending
on the kind of update. A periodic maintenance job is responsible for merging deltas into the main facets table.

Currently only 32bit integer id columns are supported. When pg\_roaringbitmap adds support for 64bit bitmaps
then int8 and possibly ctid could be supported.

### Adding faceting to a table

    SELECT faceting.add_faceting_to_table(
        'documents',
        key => 'id',
        facets => array[
            faceting.datetrunc_facet('created', 'month'),
            faceting.datetrunc_facet('finished', 'month'),
            faceting.plain_facet('category_id'),
            faceting.plain_facet('type'),
            faceting.bucket_facet('size', buckets => array[0,1000,5000,10000,50000,100000,500000])
        ]
    );

The `add_faceting_to_table()` adds the facets tables and populates the contents. It takes an array of facets
to extract from each row.

* `plain_facet(col name)` - Takes the column value as is as the facet value.
* `datetrunc_facet(col name, precision text)` - Applies a date\_trunc function on a column to get the facet value.
   Useful for timebucketing (yearly, monthly, etc.)
* `bucket_facet(col name, buckets anyarray)` - Assigns a continuous variable (price, weight, etc.) to a set of buckets
  and stores the index of the chosen bucket as the facet value.

For merging changes create a periodic job that runs:

    CALL faceting.run_maintenance();

This will run delta merging on all faceted tables. There is also a function for maintaining a single table:

    SELECT faceting.merge_deltas('documents'::regclass);

### Querying facets

Getting top 10 values for each kind of facet:

    SELECT * FROM faceting.top_values('documents'::regclass, n => 10);

We can also filter by some facets and get the results of other facets:

    SELECT * FROM faceting.count_results('documents'::regclass,
                                         filters => array[row('category_id', '24'),
                                                          row('type', 'image/jpeg')]::faceting.facet_filter[]);

For advanced usage the inverted index tables can be accessed directly.

    WITH lookup AS (
        SELECT id >> 20 AS chunk_id, rb_build_agg(id) postinglist
        FROM documents
        WHERE ...
    )
    SELECT facet_id, facet_value, sum(rb_and_cardinality(flt.postinglist, fct.postinglist))
    FROM lookup flt JOIN documents_facets USING (chunk_id)
    GROUP BY 1, 2;

### How fast is it

Calculating facets for 61% of rows in 100M row table: 

    postgres=# SELECT facet_name, count(distinct facet_value), sum(cardinality)
    postgres-# FROM faceting.count_results('documents'::regclass,
    postgres-#     filters => array[row('category_id', 24)]::faceting.facet_filter[])
    postgres-# GROUP BY 1;
     facet_name | count |   sum    
    ------------+-------+----------
     created    |   154 | 60812252
     finished   |   154 | 60812252
     size       |     7 | 60812252
     type       |     8 | 60812252
    (4 rows)
    
    Time: 164.712 ms
