/* tables for staging OTA scrapped data */
DROP TABLE IF EXISTS lakehouse.ota_property_prices;
CREATE TABLE lakehouse.ota_property_prices
(
  uuid integer,
  ota_name character varying(100),
  search_dt timestamp without time zone,
  property_name character varying,
  checkin_date timestamp without time zone,
  checkout_date  timestamp without time zone,
  room_type character varying,
  room_rate double precision,
  currency character varying(3),
  review_score double precision,
  destination_id character varying(10),
  destination_name character varying,
  location_desc  character varying,
  other_info character varying,
  data_source character varying(100),
  data_owner  character varying(100),
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
/* tables for staging OTA destinations */
DROP TABLE IF EXISTS lakehouse.ota_destinations;
CREATE TABLE lakehouse.ota_destinations
(
  uuid integer,
  ota_name character varying(100),
  property_city character varying (100),
  property_state character varying (100),
  destination_id character varying(10),
  gis_location_id integer,
  data_source character varying(100),
  data_owner  character varying(100),
  created_dt timestamp without time zone,
  created_by character varying(100),
  create_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
ALTER TABLE lakehouse.ota_property_prices
  OWNER TO postgres;
ALTER TABLE lakehouse.ota_destinations
  OWNER TO postgres;