
CREATE  KEYSPACE IF NOT EXISTS NorthAmerica WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE  KEYSPACE IF NOT EXISTS common WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

// data receive sessions
create table common.sessions(
  id UUID,            // id of the session
  run_ts text,        // time of execution
  topic text,         // name of the topic
  status text,        // failed/complete
  events_counter int, // messages count within the session
  last_updt_date text,// last updated date within session i.e. of last record
  PRIMARY KEY(id) );

// Catalog
create table northamerica.catalog(
  id UUID,
  country text,
  name text,
  state text,
  content_type text,
  timezone text,
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS catalog_content_type on northamerica.catalog(content_type);
CREATE INDEX IF NOT EXISTS catalog_country on northamerica.catalog(country);
CREATE INDEX IF NOT EXISTS catalog_state on northamerica.catalog(state);


// Category
create table northamerica.category(
  id UUID,
  catalog_id UUID,
  type text,
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS category_type on northamerica.category(type);


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
  PRIMARY KEY(id) );

CREATE INDEX IF NOT EXISTS dataset_name on northamerica.dataset(name);
CREATE INDEX IF NOT EXISTS dataset_frequency on northamerica.dataset(frequency);
CREATE INDEX IF NOT EXISTS dataset_created on northamerica.dataset(created);
CREATE INDEX IF NOT EXISTS dataset_updated on northamerica.dataset(updated);


// Entry
create table northamerica.datasetentry(
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
  PRIMARY KEY((id), session_id ));
// TODO: PRIMARY KEY should be {topic, status, }

CREATE INDEX IF NOT EXISTS dataset_direction on northamerica.datasetentry(direction);
CREATE INDEX IF NOT EXISTS dataset_segmentid on northamerica.datasetentry(segmentid);
CREATE INDEX IF NOT EXISTS dataset_street on northamerica.datasetentry(street);
CREATE INDEX IF NOT EXISTS dataset_strheading on northamerica.datasetentry(strheading);
CREATE INDEX IF NOT EXISTS dataset_last_updt on northamerica.datasetentry(last_updt);
CREATE INDEX IF NOT EXISTS dataset_traffic on northamerica.datasetentry(traffic);
