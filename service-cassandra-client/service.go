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
)

func main() {

	datasetKeyspace = getENV("CASSANDRA_KEYSPACE")
	sUsername = getENV("CASSANDRA_UNAME")
	sPassword = getENV("CASSANDRA_UPASS")
	sHost = getENV("CASSANDRA_HOST")
	newrelicKey := getENV("NEWRELIC_KEY")

	//  newrelic part
	config := newrelic.NewConfig("cassandra-client-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}

	r := mux.NewRouter()
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/insert/{keyspace}/{table}", insertHandler))
	r.HandleFunc("/", homeHandler)
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
		Counter int `json:"counter"`
		LastUpdt string `json:"last_updt"`
		// dataset ID - to be populated by Cassandra Clent service
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func insertHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

	table := mux.Vars(r)["table"]
	kspace := mux.Vars(r)["keyspace"]
	// making assumptions here - service passing table and keyspace is aware and passing correct ones
	//  i.e. no error checking at this time (TODO)
	if table == "" || kspace == "" {
		log.Printf("ERROR:  Can't have table or keyspace empty...\n")
		return
	}
	if kspace == "northamerica" {

		switch table {
			case "datasetentry":
				datasetentryCassandraWriter(w, r, kspace, table, body)
				return

			case "catalog":
					log.Printf("ERROR:  Specified table is not supported...yet..  \n")
					return

			case "category":
					log.Printf("ERROR:  Specified table is not supported...yet..  \n")
					return

			case "dataset":
					log.Printf("ERROR:  Specified table is not supported...yet..  \n")
					return

			default:
					log.Printf("ERROR:  Specified table is not supported...\n")
					return
		}

	}else if kspace == "common" {

		switch table {
			case "sessions":
				sessionsCassandraWriter(w, r, kspace, table, body)
				return

			default:
					log.Printf("ERROR:  Specified table is not supported...\n")
					return
		}

	}else {
		log.Printf("ERROR:  Specified keyspace is not supported...\n")
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
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
		 cluster.NumConns = 16
	   cluster.Authenticator = gocql.PasswordAuthenticator{
	 		Username: sUsername,
	 		Password: sPassword,
	 	}
     cluster.Consistency = gocql.One
     cluster.Port = 9042   // default port
     return cluster
}


type publishEnvelope struct {
	Topic string  `json:"topic"`
	Data map[string]string `json:"data"`
}


func datasetentryCassandraWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, envelope_body []byte ) {

	// Unmarshal the envelope
	var message publishEnvelope
  if err := json.Unmarshal([]byte(envelope_body), &message); err != nil {
		w.WriteHeader(http.StatusNotImplemented)
		errmsg := "ERROR: Could not decode body into publishEnvelope with Unmarshal. " + " Error: " + err.Error()
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
			keyspace,
			table,
			gocql.TimeUUID(), gocql.TimeUUID() /* placeholder for dataset_id*/, message.Data["session_id"],
			message.Data["_direction"], message.Data["_fromst"], message.Data["_last_updt"], message.Data["_length"], message.Data["_lif_lat"], message.Data["_lit_lat"], message.Data["_lit_lon"], message.Data["_strheading"], message.Data["_tost"], message.Data["_traffic"], message.Data["segmentid"], message.Data["start_lon"], message.Data["street"] )
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

func sessionsCassandraWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, envelope_body []byte ) {

	log.Printf("DEBUG: sessionsCassandraWriter: starting attempt to write to Cassandra POST body: " + string(envelope_body) )

	var message publishEnvelope
  if err := json.Unmarshal([]byte(envelope_body), &message); err != nil {
		w.WriteHeader(http.StatusNotImplemented)
		errmsg := "ERROR: sessionsCassandraWriter: Could not decode body into publishEnvelope with Unmarshal. " + " Error: " + err.Error()
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
			message.Data["id"], message.Data["run_ts"], message.Data["topic"], message.Data["status"], message.Data["counter"], message.Data["last_updt"])
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
