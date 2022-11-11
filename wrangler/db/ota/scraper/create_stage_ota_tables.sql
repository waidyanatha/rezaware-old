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
  similarity double precision,
  room_rate character varying(20),
  room_price double precision,
  currency character varying(3),
  review_score double precision,
  destination_id character varying(10),
  dest_lx_name character varying (100),
  dest_lx_type character varying (100),
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
comment on column lakehouse.ota_property_prices.uuid is '[optional] a UUID to identify the record';
comment on column lakehouse.ota_property_prices.ota_name is 'ota unique name (e.g. booking.com';
comment on column lakehouse.ota_property_prices.search_dt is 'data search and scrape date time stamp with timezone';
comment on column lakehouse.ota_property_prices.property_name is 'hotel or facility name displayed in ota';
comment on column lakehouse.ota_property_prices.checkin_date is 'booking checkin date time stamp with time zone';
comment on column lakehouse.ota_property_prices.checkout_date is 'booking checkout date time stamp with time zone';
comment on column lakehouse.ota_property_prices.room_type is 'room type description given by the OTA';
comment on column lakehouse.ota_property_prices.room_cate is 'room category matching rezgate definition';
comment on column lakehouse.ota_property_prices.similarity is 'the similarity score between the room type and category';
comment on column lakehouse.ota_property_prices.room_rate is 'per room value given with currency e.g. US$10.00';
comment on column lakehouse.ota_property_prices.room_price is 'price in decimals extracet from room_rate';
comment on column lakehouse.ota_property_prices.currency is 'currency ISO standard three letted abbreviation';
comment on column lakehouse.ota_property_prices.review_score is 'score on a scale of 1-10 indicated for the property by the ota';
comment on column lakehouse.ota_property_prices.destination_id is 'unique identifier for the property location; e.g. city code';
comment on column lakehouse.ota_property_prices.dest_lx_name is 'location administrative boundary name; e.g. Los Angeles';
comment on column lakehouse.ota_property_prices.dest_lx_type is 'Location administrative boundary type; .e.g city, state';
comment on column lakehouse.ota_property_prices.location_desc is 'description about the location proximity to landmarks';
comment on column lakehouse.ota_property_prices.other_info is 'other information provided by the ota about the property';
comment on column lakehouse.ota_property_prices.data_source is 'the url or api that the data is coming from';
comment on column lakehouse.ota_property_prices.data_owner is 'the ota that produces and owns the data';

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
  depart_lx_type character varying(100),
  arrive_port_name character varying(100),
  arrive_port_code character varying(10),
  arrive_lx_type character varying(100),
  flight_times character varying(100),
  depart_time timestamp without time zone,
  arrive_time timestamp without time zone,
  num_stops character varying(100),
  duration character varying(100),
  cabin_type character varying,
  booking_rate character varying(20),
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
comment on column lakehouse.ota_airline_routes.uuid is '[optional] a UUID to identify the record';
comment on column lakehouse.ota_airline_routes.ota_name is 'ota unique name (e.g. kayak.com';
comment on column lakehouse.ota_airline_routes.search_dt is 'data search and scrape date time stamp with timezone';
comment on column lakehouse.ota_airline_routes.agent_name is 'the source making the booking';
comment on column lakehouse.ota_airline_routes.depart_port_name is 'flight departure airport location';
comment on column lakehouse.ota_airline_routes.depart_port_code is 'flight departure airport code';
comment on column lakehouse.ota_airline_routes.depart_lx_type is 'flight departure airport location type e.g. city, state,';
comment on column lakehouse.ota_airline_routes.arrive_port_name is 'flight arrival airport location';
comment on column lakehouse.ota_airline_routes.arrive_port_code is 'flight arrival airport code';
comment on column lakehouse.ota_airline_routes.arrive_lx_type is 'flight arrival airport location type e.g. city, state,';
comment on column lakehouse.ota_airline_routes.flight_times is 'flight departure arrival times';
comment on column lakehouse.ota_airline_routes.depart_time is 'flight departure times';
comment on column lakehouse.ota_airline_routes.arrive_time is 'flight arrival times';
comment on column lakehouse.ota_airline_routes.num_stops is 'number of stops between origin and destination ports';
comment on column lakehouse.ota_airline_routes.duration is 'flight duration in hours and minutes';
comment on column lakehouse.ota_airline_routes.cabin_type is 'cabin type; i.e. first class, business, economy';
comment on column lakehouse.ota_airline_routes.booking_rate is 'booking currency value per passenger';
comment on column lakehouse.ota_airline_routes.booking_price is 'booking price as decimal value';
comment on column lakehouse.ota_airline_routes.currency is 'currency type of the booking; e.g. USD, GBP';
comment on column lakehouse.ota_airline_routes.num_passengers is 'number of passengers on the flight';
comment on column lakehouse.ota_airline_routes.airline_name is 'name of the carrier; e.g. Luftanza';
comment on column lakehouse.ota_airline_routes.data_source is 'the url or api that the data is coming from';
comment on column lakehouse.ota_airline_routes.data_owner is 'the ota that produces and owns the data';

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
ALTER TABLE lakehouse.ota_airline_routes
  OWNER TO postgres;
ALTER TABLE lakehouse.ota_destinations
  OWNER TO postgres;