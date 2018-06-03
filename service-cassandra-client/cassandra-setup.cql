
CREATE  KEYSPACE IF NOT EXISTS NorthAmerica WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE  KEYSPACE IF NOT EXISTS common WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

// data receive sessions
create table common.sessions(
  id UUID,            // id of the session
  run_ts text,        // time of execution
  topic text,         // name of the topic
  status text,        // failed/complete
  events_counter text, // messages count within the session
  last_updt_date text,// last updated date within session i.e. of last record
  PRIMARY KEY(id) );

// Catalog
create table northamerica.catalog(
  id UUID,
  country text,
  name text,
  state text,
  city text,
  content_type text,
  timezone text,
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS catalog_content_type on northamerica.catalog(content_type);
CREATE INDEX IF NOT EXISTS catalog_country on northamerica.catalog(country);
CREATE INDEX IF NOT EXISTS catalog_state on northamerica.catalog(state);

INSERT INTO northamerica.catalog (id, country, state, city, name, content_type, timezone) VALUES (11e21f55-65a3-11e8-9eab-0242ac110006, 'USA', 'IL', 'Chicago', 'Chicago Data Portal', 'data', 'CDT');
INSERT INTO northamerica.catalog (id, country, state, city, name, content_type, timezone) VALUES (17c91eb1-6517-11e8-8394-0242ac110005, 'USA', 'NY', 'New York', 'New York Data Portal', 'data', 'EST');


// Category
create table northamerica.category(
  id UUID,
  catalog_id UUID,
  category_name text,
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS category_type on northamerica.category(category);

INSERT INTO northamerica.category (id, catalog_id, category_name) VALUES (3500b8fb-65a3-11e8-83d2-0242ac110006, 11e21f55-65a3-11e8-9eab-0242ac110006, 'transportation');
INSERT INTO northamerica.category (id, catalog_id, category_name) VALUES (390784fb-60a3-10e8-83d2-0142ac190006, 11e21f55-65a3-11e8-9eab-0242ac110006, 'environment');


// Dataset
create table northamerica.dataset(
  id UUID,
  category_id UUID,
  created_ts text,
  description text,
  name text,
  topic text,
  frequency text,
  pii boolean,
  updated_ts text,
  ingest_endpoint text,
  schema_table text,
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS dataset_name on northamerica.dataset(name);
CREATE INDEX IF NOT EXISTS dataset_frequency on northamerica.dataset(frequency);
CREATE INDEX IF NOT EXISTS dataset_created on northamerica.dataset(created);
CREATE INDEX IF NOT EXISTS dataset_updated on northamerica.dataset(updated);

INSERT INTO northamerica.dataset (id, category_id, created_ts, description, name, topic, frequency, pii, updated_ts, ingest_endpoint, schema_table) VALUES
(3500b8f2-65a3-11e8-83d1-0242ac110006, 3500b8fb-65a3-11e8-83d2-0242ac110006, '2018-03-02T10:10:18.000', 'This dataset contains the current estimated speed for about 1250 segments covering 300 miles of arterial roads. For a more detailed description, go to: http://bit.ly/Q9AZAD.', 'Chicago Traffic Tracker - Congestion Estimates by Segments', 'us.chicago-city.transportation.traffic-tracker-congestion-estimates', 'every 15 minutes', false, '2018-05-02T10:10:18.000', '/us/il/chicago/data/transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Segment', 'TTCongestionEstimatesBySegment' );
INSERT INTO northamerica.dataset (id, category_id, created_ts, description, name, topic, frequency, pii, updated_ts, ingest_endpoint, schema_table) VALUES
(84dedce9-668a-11e8-b39a-0242ac110006, 3500b8fb-65a3-11e8-83d2-0242ac110006, '2018-05-02T10:10:18.000', 'This dataset contains the historical estimated congestion for over 1,000 traffic segments, starting in approximately March 2018. Older records are in https://data.cityofchicago.org/d/77hq-huss.', 'Chicago Traffic Tracker - Historical Congestion Estimates by Segment - 2018-Current', 'us.chicago-city.transportation.traffic-tracker-2018-current', 'every 15 minutes', false, '2018-05-31T10:10:18.000', '/us/il/chicago/data/transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Segment-2018-Current', 'TTCongestionEstimatesBySegment2018Current' );


//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
// Entry
create table northamerica.TTCongestionEstimatesBySegment (
  id UUID,
  dataset_id UUID,
  session_id UUID,
  direction text,
  fromst text,
  last_updt text,
  length text,
  lif_lat text,
  lit_lat text,
  lit_lon text,
  strheading text,
  tost text,
  traffic text,
  segmentid text,
  start_lon text,
  street text,
  PRIMARY KEY((id), session_id ))
  WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
// TODO: PRIMARY KEY should be {topic, status, }


CREATE INDEX IF NOT EXISTS dataset_direction on northamerica.datasetentry(direction);
CREATE INDEX IF NOT EXISTS dataset_segmentid on northamerica.datasetentry(segmentid);
CREATE INDEX IF NOT EXISTS dataset_street on northamerica.datasetentry(street);
CREATE INDEX IF NOT EXISTS dataset_strheading on northamerica.datasetentry(strheading);
CREATE INDEX IF NOT EXISTS dataset_last_updt on northamerica.datasetentry(last_updt);
CREATE INDEX IF NOT EXISTS dataset_traffic on northamerica.datasetentry(traffic);

CREATE TYPE northamerica.point (
  longitude text,
  latitude text
);
create table northamerica.ChicagoTrafficTrackerHistoricalCongestionEstimatesBySegment2018Current(
  id UUID,
  dataset_id UUID,
  session_id UUID,
  time text,
  bus_count text,
  day_of_week text,
  direction text,
  end_latitude text,
  end_location point,
  end_longitude text,
  from_street text,
  hour text,
  length text,
  message_count text,
  month text,
  record_id text,
  segment_id text,
  speed text,
  start_latitude text,
  start_location point,
  start_longitude text,
  street text,
  street_heading text,
  to_street text,
  PRIMARY KEY((id), session_id ))
  WITH compression = { 'sstable_compression' : 'LZ4Compressor' };

//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
// Alternative, more generic design
create table northamerica.generic_datasetentry(
  id UUID,
  dataset_id UUID,
  session_id UUID,
  record text,
  PRIMARY KEY((id), session_id ))
  WITH compression = { 'sstable_compression' : 'LZ4Compressor' };

INSERT INTO northamerica.generic_datasetentry (id, dataset_id, session_id, record) VALUES (3500b8f2-65a3-11e8-83d1-0242ac110006, 3500b8fb-65a3-11e8-83d2-0242ac110006, 11e21f55-65a3-11e8-9eab-0242ac110006, '{
        \"bus_count\": \"1\",
        \"day_of_week\": \"6\",
        \"direction\": \"NB\",
        \"end_latitude\": \"41.9542892901\",
        \"end_location\": {
            \"type\": \"Point\",
            \"coordinates\": [
                -87.6688650541,
                41.9542892901
            ]
        },
        \"end_longitude\": \"-87.6688650541\",
        \"from_street\": \"Addison\",
        \"hour\": \"10\",
        \"length\": \"0.5\",
        \"message_count\": \"1\",
        \"month\": \"3\",
        \"record_id\": \"0127-201803021010\",
        \"segment_id\": \"127\",
        \"speed\": \"25\",
        \"start_latitude\": \"41.9469890515\",
        \"start_location\": {
            \"type\": \"Point\",
            \"coordinates\": [
                -87.6686743455,
                41.9469890515
            ]
        },
        \"start_longitude\": \"-87.6686743455\",
        \"street\": \"Ashland\",
        \"street_heading\": \"N\",
        \"time\": \"2018-03-02T10:10:18.000\",
        \"to_street\": \"Irving Park\"
    }' )




//eof
