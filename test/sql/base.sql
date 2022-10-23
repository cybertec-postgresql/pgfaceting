CREATE EXTENSION roaringbitmap;
CREATE EXTENSION pgfaceting;

CREATE SCHEMA facetingtestsuite;

CREATE TYPE facetingtestsuite.mimetype AS ENUM (
   'application/pdf',
    'text/html',
    'image/jpeg',
    'image/png',
    'application/msword',
    'text/csv',
    'application/zip',
    'application/vnd.ms-powerpoint'
    );

CREATE TABLE facetingtestsuite.documents (
    id int8 primary key,
    created timestamptz not null,
    finished timestamptz,
    category_id int8 not null,
    tags text[],
    type facetingtestsuite.mimetype,
    size int8,
    title text
);

COPY facetingtestsuite.documents (id, created, finished, category_id, tags, type, size, title) FROM stdin;
1	2010-01-01 00:00:42+02	2010-01-01 09:45:29+02	8	{blue,burlywood,antiquewhite,olive}	application/pdf	71205	Interracial marriage Science Research
2	2010-01-01 00:00:37+02	2010-01-01 03:55:08+02	12	{lightcoral,bisque,blue,"aqua blue","red purple",aqua}	text/html	682069	Odour and trials helped to improve the country's history through the public
3	2010-01-01 00:00:33+02	2010-01-02 18:29:15+02	9	{"mustard brown","very light pink"}	application/pdf	143708	Have technical scale, ordinary, commonsense notions of absolute time and length independent of the
4	2010-01-01 00:00:35+02	2010-01-02 01:12:08+02	24	{orange,green,blue}	text/html	280663	Database of (/ˈdɛnmɑːrk/; Danish: Danmark [ˈd̥ænmɑɡ̊]) is a spiral
5	2010-01-01 00:01:06+02	2010-01-01 23:18:56+02	24	{orange,chocolate}	image/jpeg	111770	Passage to now resumed
6	2010-01-01 00:01:05+02	2010-01-01 10:25:29+02	8	{blue,aquamarine}	application/pdf	110809	East. Mesopotamia, BCE – 480 BCE), when determining a value that
7	2010-01-01 00:00:57+02	2010-01-02 00:41:01+02	24	{}	application/pdf	230803	Bahía de It has also conquered 13 South American finds and another
8	2010-01-01 00:01:11+02	2010-01-01 14:22:11+02	24	{blue,burlywood,"dirt brown",orange,ivory,brown,green,olive,lightpink}	image/jpeg	1304196	15-fold: from the mid- to late-20th
9	2010-01-01 00:01:47+02	2010-01-01 09:59:57+02	9	{green,blue,orange}	application/pdf	142410	Popular Western localized function model. Psychiatric interventions such as local businesses, but also
10	2010-01-01 00:01:31+02	2010-01-01 05:49:47+02	24	{green,lavender,blue,orange,red,darkslateblue}	text/html	199703	Rapidly expanding Large Interior Form, 1953-54, Man Enters the Cosmos and Nuclear Energy.
\.

SELECT faceting.add_faceting_to_table('facetingtestsuite.documents',
        key => 'id',
        facets => array[
            faceting.datetrunc_facet('created', 'month'),
            faceting.datetrunc_facet('finished', 'month'),
            faceting.plain_facet('category_id'),
            faceting.plain_facet('type'),
            faceting.bucket_facet('size', buckets => array[0,1000,5000,10000,50000,100000,500000])
        ],
        populate => false
    );

SELECT faceting.populate_facets('facetingtestsuite.documents'::regclass);

SELECT * FROM faceting.top_values('facetingtestsuite.documents'::regclass);

COPY facetingtestsuite.documents (id, created, finished, category_id, tags, type, size, title) FROM stdin;
11	2010-01-01 00:01:21+02	2010-01-01 20:31:12+02	9	{blue,pink,orange}	image/png	679323	Additional 32 Martin, Saint Pierre and
12	2010-01-01 00:02:12+02	2010-01-02 10:33:25+02	24	{green,maroon,blue,coral,orange}	application/pdf	166940	To harness between continents. By the mid-19th century?
13	2010-01-01 00:02:20+02	2010-01-01 03:59:11+02	24	{orange,"pale peach",blue,"peachy pink",chartreuse,aqua,brown}	application/pdf	333191	The synchrocyclotron, been exposed
14	2010-01-01 00:02:32+02	2010-01-01 18:50:37+02	24	{orange,cherry,brown}	application/pdf	12421	And supernovae as ways to indirectly measure these elusive phenomenological entities.
15	2010-01-01 00:02:47+02	2010-01-01 14:29:27+02	24	{orange,blue,cyan,red,floralwhite,darkslateblue}	application/pdf	459132	Ratio. \n the nucleus of a cumulus or cumulonimbus.
16	2010-01-01 00:02:38+02	2010-01-01 20:53:15+02	24	{blue,orange,purple,"pale gold"}	application/pdf	140909	Pacific. A observance of halakha may pose serious
17	2010-01-01 00:02:48+02	2010-01-02 08:19:47+02	9	{orange,blue,rust}	image/png	414066	Gravity equivalent, it attract the wrath of
18	2010-01-01 00:03:05+02	2010-01-02 15:16:47+02	24	{dimgray,orange,red}	image/jpeg	113942	Jim Crow classification methods including
19	2010-01-01 00:03:23+02	2010-01-02 06:33:01+02	24	{"candy pink",blue,orange,brown}	text/csv	100419	Trans-Atlantic trade archdioceses, the Archdiocese of Atlanta.
20	2010-01-01 00:03:23+02	2010-01-02 02:24:17+02	24	{cadetblue,blue,green}	image/png	705939	Normandy with others. Laughter is a kind of case that
\.

SELECT faceting.merge_deltas('facetingtestsuite.documents'::regclass);

SELECT * FROM faceting.top_values('facetingtestsuite.documents'::regclass);

(SELECT 'created' AS facet_name, date_trunc('month', created)::text AS facet_value, COUNT(*) AS cardinality FROM facetingtestsuite.documents GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5)
    UNION ALL
(SELECT 'finished', date_trunc('month', finished)::text, COUNT(*) FROM facetingtestsuite.documents GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5)
    UNION ALL
(SELECT 'category_id', category_id::text, COUNT(*) FROM facetingtestsuite.documents GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5)
    UNION ALL
(SELECT 'type', type::text, COUNT(*) FROM facetingtestsuite.documents GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5)
    UNION ALL
(SELECT 'size', width_bucket(size, array[0,1000,5000,10000,50000,100000,500000])::text, COUNT(*) FROM facetingtestsuite.documents GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5);

SELECT * FROM faceting.count_results('facetingtestsuite.documents'::regclass,
                                     filters => array[row('category_id', 24)]::faceting.facet_filter[]);
