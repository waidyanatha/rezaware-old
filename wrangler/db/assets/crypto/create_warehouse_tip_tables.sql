/* tables for staging property asset scrapped data */
DROP TABLE IF EXISTS warehouse.mcap_past;
CREATE TABLE warehouse.mcap_past
(
  mcap_past_pk serial primary key,
  uuid character varying(100),
  data_source character varying(100),
  asset_name character varying(100),
  asset_symbol character varying(50),
  mcap_date timestamp without time zone,
  mcap_value decimal(32,16),
  mcap_rank integer,
  mcap_log_ror decimal(32,16),
  mcap_simp_ror decimal(32,16),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(200),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(200),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on table warehouse.mcap_past is 'time series of distinct asset mcap values and rate of returns';
comment on column warehouse.mcap_past.mcap_past_pk is 'postgres auto incremental primary key';
comment on column warehouse.mcap_past.uuid is '[optional] a UUID to identify the record';
comment on column warehouse.mcap_past.data_source is 'the url or api that the data is coming from';
comment on column warehouse.mcap_past.asset_name is 'asset full name (e.g. bitcoin, ethereum)';
comment on column warehouse.mcap_past.asset_symbol is 'asset abbreviation (e.g. btc, eth)';
comment on column warehouse.mcap_past.mcap_date is 'date of the market cap value';
comment on column warehouse.mcap_past.mcap_value is 'asset market cap value';
comment on column warehouse.mcap_past.mcap_log_ror is 'ln, log10, log2, etc rate of returns';
comment on column warehouse.mcap_past.mcap_simp_ror is 'simple rate of returns';

ALTER TABLE warehouse.mcap_past
  OWNER TO farmraider;