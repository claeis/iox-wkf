-- SCHEMA: PUBLIC (DEFAULT) --
CREATE SCHEMA public

CREATE TABLE public.csvimport
(
  attr1 character varying NOT NULL,
  attr2 character varying,
  attr3 character varying,
  CONSTRAINT csvimport_pkey PRIMARY KEY (attr1)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.csvimportnopkcolumn
(
  attr1 text,
  attr2 text,
  attr3 text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.csvimportwithheader
(
  id text,
  abbreviation text,
  state text
)
WITH (
  OIDS=FALSE
);


-- SCHEMA: CSVTODBSCHEMA --

CREATE SCHEMA csvtodbschema

CREATE TABLE csvtodbschema.csvimport
(
  attr1 character varying NOT NULL,
  attr2 character varying,
  attr3 character varying,
  CONSTRAINT csvimport_pkey PRIMARY KEY (attr1)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE csvtodbschema.csvimportnopk
(
  attr1 text,
  attr2 text,
  attr3 text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE csvtodbschema.csvimportwithheader
(
  id character varying NOT NULL,
  abbreviation character varying,
  state character varying,
  CONSTRAINT csvimportwithheader_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE csvtodbschema.csvimportwithheadernopk
(
  id text,
  abbreviation text,
  state text
)
WITH (
  OIDS=FALSE
);