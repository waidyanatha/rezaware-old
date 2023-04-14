/* tables for staging property asset scrapped data */
DROP TABLE IF EXISTS warehouse.mcap_past;
CREATE TABLE warehouse.mcap_past
(
  mcap_past_pk serial primary key,
  uuid character varying(50),
  data_source character varying(100),
  asset_name character varying(50),
  asset_symbol character varying(15),
  alt_asset_id character varying(50),
  currency character varying(5),
  price_date timestamp without time zone,
  price_value decimal(32,16),
  price_log_ror decimal(32,16),
  price_simp_ror decimal(32,16),
  mcap_date timestamp without time zone,
  mcap_value decimal(32,16),
  mcap_rank integer,
  mcap_log_ror decimal(32,16),
  mcap_simp_ror decimal(32,16),
  volume_date timestamp without time zone,
  volume_size decimal(32,16),
  volume_change decimal(32,16),
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
comment on column warehouse.mcap_past.alt_asset_id is 'asset id given by the data source owner';
comment on column warehouse.mcap_past.currency is 'price and mcap currency common denominator';
comment on column warehouse.mcap_past.price_date 'timestamp of the asset price';
comment on column warehouse.mcap_past.price_value 'asset price, daily max, min, avg, median';
comment on column warehouse.mcap_past.price_log_ror is 'ln, log10, log2, etc rate of returns on price';
comment on column warehouse.mcap_past.price_simp_ror is 'simple rate of returns on price fluctuation';
comment on column warehouse.mcap_past.mcap_date is 'date of the market cap value';
comment on column warehouse.mcap_past.mcap_value is 'asset market cap value';
comment on column warehouse.mcap_past.mcap_log_ror is 'ln, log10, log2, etc rate of returns on marketcap';
comment on column warehouse.mcap_past.mcap_simp_ror is 'simple rate of returns on marketcap fluctuation';
comment on column warehouse.mcap_past.volume_date is 'timestamp of the supplied asset volume';
comment on column warehouse.mcap_past.volume_size is 'quantity of supplied asset volumne on date';

ALTER TABLE warehouse.mcap_past
  OWNER TO farmraider;