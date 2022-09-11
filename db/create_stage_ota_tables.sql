/* tables for staging OTA scrapped data */
DROP TABLE IF EXISTS lakehouse.ota_property_prices;
CREATE TABLE lakehouse.ota_property_prices
(
  id integer,
  ota_name character varying(100),
  search_dt timestamp without time zone[],
  property_name character varying,
  checkin_date timestamp without time zone[],
  checkout_date  timestamp without time zone[],
  room_type character varying,
  room_rate double precision,
  currency character varying(3),
  review_score double precision,
  destination_id character varying,
  location  character varying,
  other character varying,
  datasource character varying(100),
  dataowner  character varying(100),
  created_by character varying(100),
  create_dt timestamp without time zone[],
  create_proc character varying(100),
  modified_by character varying(100),
  modified_dt timestamp without time zone[],
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone[]
)
WITH (
  OIDS=FALSE
);
ALTER TABLE lakehouse.ota_property_prices
  OWNER TO postgres;
