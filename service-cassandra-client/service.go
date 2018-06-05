package main

import (
	"fmt"
	"log"
	"net/http"
  "io"
  "io/ioutil"
  "encoding/json"
  _ "strconv"
  _ "html"
  _ "html/template"
  "os"
  "sync"
	"time"
  _ "encoding/base64"
	"strings"

	"google.golang.org/appengine"
  "cloud.google.com/go/pubsub"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"

	"github.com/newrelic/go-agent"
)

var (
	messagesMu sync.Mutex
  countMu sync.Mutex
	count   int
  subscription *pubsub.Subscription

	datasetKeyspace string
	sUsername string
	sPassword string
	sHost string

	traffictrackerTopic string
	traffictrackerTopic2018 string
	sessionsTopic string
	controlsTopic string

	isSchemaDefined bool
)

func main() {

	datasetKeyspace = getENV("CASSANDRA_KEYSPACE")
	sUsername = getENV("CASSANDRA_UNAME")
	sPassword = getENV("CASSANDRA_UPASS")
	sHost = getENV("CASSANDRA_HOST")

	traffictrackerTopic = getENV("TRAFFIC_TRACKER_TOPIC")
	traffictrackerTopic2018 = getENV("TRAFFIC_TRACKER2018_TOPIC")
	sessionsTopic = getENV("SESSIONS_TOPIC")
	controlsTopic = getENV("CONTROLS_TOPIC")

	newrelicKey := getENV("NEWRELIC_KEY")

	//  newrelic part
	config := newrelic.NewConfig("cassandra-client-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}

	r := mux.NewRouter()
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/liveness_check", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/readiness_check", healthCheckHandler))
	r.HandleFunc("/", homeHandler)
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/insert/{fromtopic}", insertHandler)).Methods("POST")
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/insert/{fromtopic}/{session_id}", insertHandler)).Queries("schema", "{schema}").Methods("POST")

	http.Handle("/", r)

	log.Print("Starting service.....")
	appengine.Main()
}


type pushRequest struct {
    Message struct {
        Attributes map[string]string
        Data       string `json:"data"`
        message_id     string `json:"message_id"`
        messageId      string `json:"messageId"`
        publish_time   string `json:"publish_time"`
        publishTime    string `json:"publishTime"`
    }
    Subscription string
}

type datasetentryStruct struct {
	SessionID string `json:"session_id"`
	Direction string `json:"_direction"`
	Fromst string `json:"_fromst"`
	Last_updt string `json:"_last_updt"`
	Length string `json:"_length"`
	Lif_lat string `json:"_lif_lat"`
	Lit_lat string `json:"_lit_lat"`
	Lit_lon string `json:"_lit_lon"`
	Strheading string `json:"_strheading"`
	Tost string `json:"_tost"`
	Traffic string `json:"_traffic"`
	Segmentid string `json:"segmentid"`
	Start_lon string `json:"start_lon"`
	Street string `json:"street"`
}

type sessionStruct struct {
		Id string `json:"id"`
    RunTS string `json:"run_ts"`
		Topic string `json:"topic"`
		Status string `json:"status"`
		Counter string `json:"counter"`
		LastUpdt string `json:"last_updt"`
		// dataset ID - to be populated by Cassandra Clent service
}

func insertHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	fromtopic := strings.ToLower(mux.Vars(r)["fromtopic"])
	schema := strings.ToLower(mux.Vars(r)["schema"])
	session_id := strings.ToLower(mux.Vars(r)["session_id"])
	if fromtopic == "" {
		io.WriteString(w, "ERROR:  Can't have {fromtopic} empty...\n")
		return
	}

	log.Print("DEBUG: insertHandler: Receive.. fromtopic: " + fromtopic + " ,schema: " + schema)

  if r.Body == nil {
      log.Print("ERROR: Please send a request body\n")
      return
  }
  body, err := ioutil.ReadAll(r.Body)
  defer r.Body.Close()
  if err != nil {
    log.Printf("ERROR:  Can't read http body ioutil.ReadAll...\n")
		return
	}

	var kspace, table string

	// mapping b/w topics and keyspace-table
	switch fromtopic {

		case traffictrackerTopic:
			if schema == "" || schema == "false" {
				kspace = "northamerica"
				table = "generic_datasetentry"
				datasetentryCassandraSchemalessWriter(w, r, kspace, table, body, session_id)
			}else{
				kspace = "northamerica"
				table = "TTCongestionEstimatesBySegment"
				datasetentryCassandraSchemaWriter(w, r, kspace, table, body, session_id)
			}

		case traffictrackerTopic2018:
				kspace = "northamerica"
				table = "generic_datasetentry"
				datasetentryCassandraSchemalessWriter(w, r, kspace, table, body, session_id)

		case sessionsTopic:
			kspace = "common"
			table = "sessions"
			sessionsCassandraWriter(w, r, kspace, table, body)

		default:
			log.Printf("ERROR:  Handler for topic " + fromtopic + "not implemented yet.")
			return
	}

	w.WriteHeader(http.StatusOK)
}

//-----------------------------------------------------------------------------------------------
var thesession *gocql.Session

func initSession() error {
    var err error
    if thesession == nil || thesession.Closed() {
        thesession, err = getCluster().CreateSession()
    }
    return err
}

func getCluster() *gocql.ClusterConfig {
     cluster := gocql.NewCluster(sHost)
		 cluster.Timeout = 15 * time.Second
		 cluster.NumConns = 50
	   cluster.Authenticator = gocql.PasswordAuthenticator{
	 		Username: sUsername,
	 		Password: sPassword,
	 	}
     cluster.Consistency = gocql.One
     cluster.Port = 9042   // default port
     return cluster
}


func datasetentryCassandraSchemalessWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, _body []byte, session_id string) {

	err := initSession()
	if err != nil {
		msg := "Error creating Cassandra session: " + err.Error()
		log.Printf(msg)
		io.WriteString(w, msg)
		//log.Fatalf(msg)
		return
	}

	defer thesession.Close()
	query := fmt.Sprintf(
			"INSERT INTO %s.%s (id, dataset_id, session_id, record) VALUES (%s, %s, %s, '%s')",
			keyspace, table,
			gocql.TimeUUID(), gocql.TimeUUID() /* placeholder for dataset_id*/, session_id, _body )
	err = thesession.Query(query).Exec()
	if err != nil {
		msg := "ERROR: datasetentryCassandraSchemalessWriter: Query: "+query+". Error writing to Cassandra " + err.Error()
		io.WriteString(w, msg)
		log.Printf(msg)
		//log.Fatalf(msg)
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
}

func datasetentryCassandraSchemaWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, _body []byte, session_id string ) {

	var message datasetentryStruct
  if err := json.Unmarshal([]byte(_body), &message); err != nil {
		w.WriteHeader(http.StatusNotImplemented)
		errmsg := "ERROR: Could not decode body into datasetentryStruct with Unmarshal. " + " Error: " + err.Error()
		io.WriteString(w, errmsg)
    log.Fatalf(errmsg)
		return
  }

	err := initSession()
	if err != nil {
		msg := "Error creating Cassandra session: " + err.Error()
		log.Printf(msg)
		io.WriteString(w, msg)
		//log.Fatalf(msg)
		return
	}

	defer thesession.Close()
	query := fmt.Sprintf(
			"INSERT INTO %s.%s (id, dataset_id, session_id, direction, fromst, Last_updt, Length, Lif_lat, Lit_lat, Lit_lon, Strheading, Tost, Traffic, Segmentid, Start_lon, Street) VALUES (%s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
			keyspace, table,
			gocql.TimeUUID(), gocql.TimeUUID() /* placeholder for dataset_id*/, message.SessionID,
			message.Direction, message.Fromst, message.Last_updt, message.Length, message.Lif_lat, message.Lit_lat, message.Lit_lon, message.Strheading, message.Tost, message.Traffic, message.Segmentid, message.Start_lon, message.Street )
	err = thesession.Query(query).Exec()
	if err != nil {
		msg := "ERROR: datasetentryCassandraWriter: Query: "+query+". Error writing to Cassandra " + err.Error()
		io.WriteString(w, msg)
		log.Printf(msg)
		//log.Fatalf(msg)
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
}

func sessionsCassandraWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, _body []byte ) {

	log.Printf("DEBUG: sessionsCassandraWriter: starting attempt to write to Cassandra POST body: " + string(_body) )

	var message sessionStruct
  if err := json.Unmarshal([]byte(_body), &message); err != nil {
		w.WriteHeader(http.StatusNotImplemented)
		errmsg := "ERROR: sessionsCassandraWriter: Could not decode body into sessionStruct with Unmarshal. " + " Error: " + err.Error()
		io.WriteString(w, errmsg)
    log.Fatalf(errmsg)
		return
  }

	err := initSession()
	if err != nil {
		msg := "Error creating session: " + err.Error()
		log.Printf(msg)
		io.WriteString(w, msg )
		return
	}
	defer thesession.Close()

	query := fmt.Sprintf(
			"INSERT INTO %s.%s (id, run_ts, topic, status, events_counter, last_updt_date) VALUES (%s, '%s', '%s', '%s', '%s', '%s')",
			keyspace,
			table,
			message.Id, message.RunTS, message.Topic, message.Status, message.Counter, message.LastUpdt )
	// send back to caller
	io.WriteString(w, query)

	err = thesession.Query(query).Exec()
	if err != nil {
		msg := "ERROR: sessionsCassandraWriter: Query: "+query+".  Error writing to Cassandra " + err.Error()
		io.WriteString(w, msg)
		log.Printf(msg)
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

}


func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "{\"alive\": true}" )
}


func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}
