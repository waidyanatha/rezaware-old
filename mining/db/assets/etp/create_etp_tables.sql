/* tables for staging property asset scrapped data */
/* \i path_to_sql_file */
DROP TABLE IF EXISTS warehouse.mcap_stats;
CREATE TABLE warehouse.mcap_stats
(
  uuid character varying(100),
  asset_objid character varying(100),
  asset_name character varying(100),
  mcap_date date,
  mcap_value decimal(32,16),
  mcap_ror decimal(32,16),
  ror_07day_sma decimal(32,16),
  ror_07day_smsd decimal(32,16),
  ror_14day_sma decimal(32,16),
  ror_14day_smsd decimal(32,16),
  ror_21day_sma decimal(32,16),
  ror_21day_smsd decimal(32,16),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column warehouse.mcap_stats.uuid is '[optional] a UUID to identify the record';
comment on column warehouse.mcap_stats.asset_objid is '[mandatory] ObjID to identify asset';
comment on column warehouse.mcap_stats.asset_name is '[mandatory] asset full name (e.g. bitcoin)';
comment on column warehouse.mcap_stats.mcap_date is '[mandatory] date of the market cap value';
comment on column warehouse.mcap_stats.mcap_value is '[mandatory] asset market cap value';
comment on column warehouse.mcap_stats.mcap_ror is 'asset market cap retio of return';
comment on column warehouse.mcap_stats.ror_07day_sma is 'asset ror 7 day simple moving avg';
comment on column warehouse.mcap_stats.ror_07day_smsd is 'asset ror 7 day simple moving stdv';
comment on column warehouse.mcap_stats.ror_14day_sma is 'asset ror 14 day simple moving avg';
comment on column warehouse.mcap_stats.ror_14day_smsd is 'asset ror 14 day simple moving stdv';
comment on column warehouse.mcap_stats.ror_21day_sma is 'asset ror 21 day simple moving avg';
comment on column warehouse.mcap_stats.ror_21day_smsd is 'asset ror 21 day simple moving stdv';

ALTER TABLE warehouse.mcap_stats
  OWNER TO farmraider;