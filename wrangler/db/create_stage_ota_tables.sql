/* tables for staging property OTA scrapped data */
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
  room_cate character varying,
  room_rate character varying(20),
  room_price double precision,
  currency character varying(3),
  review_score double precision,
  destination_id character varying(10),
  destination_name character varying,
  destination_Lx character varying,
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
comment on lakehouse.ota_property_prices.room_type is 'room type description given by the OTA';
comment on lakehouse.ota_property_prices.room_cate is 'room category matching rezgate definition';
comment on lakehouse.ota_property_prices.room_rate is 'per room value given with currency e.g. US$10.00';
comment on lakehouse.ota_property_prices.room_price is 'price in decimals extracet from room_rate';
comment on lakehouse.ota_property_prices.destination_id is 'unique identifier for the property location; e.g. city code';
comment on lakehouse.ota_property_prices.destination_name is 'human readable name of the destination';
comment on lakehouse.ota_property_prices.destination_Lx is 'Location administrative name; .e.g city, state, province';

/* tables for staging property OTA scrapped data */
DROP TABLE IF EXISTS lakehouse.ota_airline_routes;
CREATE TABLE lakehouse.ota_airline_routes
(
  uuid integer,
  ota_name character varying(100),
  search_dt timestamp without time zone,
  agent_name character varying(100),
  depart_port_name character varying(100),
  depart_port_code character varying(10),
  arrive_port_name character varying(100),
  arrive_port_code character varying(10),
  depart_time timestamp without time zone,
  arrive_time  timestamp without time zone,
  num_stops character varying(100),
  duration character varying(100),
  cabin_type character varying,
  booking_price double precision,
  currency character varying(3),
  num_passengers smallint,
  airline_name character varying(100),
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
/* tables for staging scrapped events data */

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