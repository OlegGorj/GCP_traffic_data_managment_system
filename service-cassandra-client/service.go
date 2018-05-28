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
	config := newrelic.NewConfig("cassandra-service", newrelicKey)
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

			default:
					log.Printf("ERROR:  Specified table is not supported...\n")
					return
		}

	}else {
		log.Printf("ERROR:  Specified keyspace is not supported...\n")
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}")
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
     //cluster.Keyspace = datasetKeyspace
		 cluster.Timeout = 3 * time.Second
		 cluster.NumConns = 16
	   cluster.Authenticator = gocql.PasswordAuthenticator{
	 		Username: sUsername,
	 		Password: sPassword,
	 	}
     cluster.Consistency = gocql.One
     cluster.Port = 9042   // default port
     return cluster
}

func datasetentryCassandraWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, body []byte/*e datasetentryStruct*/ ) {

	var e datasetentryStruct
  if err := json.Unmarshal(body /*sDec*/, &e); err != nil {
		errmsg := "ERROR: Could not decode body into datasetentryStruct type with Unmarshal: " + string(body) + "\n\n"
    log.Printf(errmsg)
		io.WriteString(w, errmsg)
  }

	err := initSession()
	if err != nil {
		msg := "Error creating session: " + err.Error()
		log.Printf(msg)
		io.WriteString(w, "{\"status\":\"1\", \"message\":\""+ msg +"\"}")
		//log.Fatalf(msg)
		return
	}
	defer thesession.Close()

	err = thesession.Query(
		fmt.Sprintf(
			"INSERT INTO %s.%s (id, Direction, Fromst, Last_updt, Length, Lif_lat, Lit_lat, Lit_lon, Strheading, Tost, Traffic, Segmentid, Start_lon, Street) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			keyspace,
			table),
		gocql.TimeUUID(), e.Direction, e.Fromst, e.Last_updt, e.Length, e.Lif_lat, e.Lit_lat, e.Lit_lon, e.Strheading, e.Tost, e.Traffic, e.Segmentid, e.Start_lon, e.Street ).Exec()

	if err != nil {
		msg := "ERROR: Error writing to Cassandra " + err.Error()
		io.WriteString(w, msg)
		log.Printf(msg)
		//log.Fatalf(msg)
		return
	}

}

func sessionsCassandraWriter(w http.ResponseWriter, r *http.Request, keyspace string, table string, body []byte ) {

	var e sessionStruct
  if err := json.Unmarshal(body, &e); err != nil {
		errmsg := "ERROR: Could not decode body into sessionStruct type with Unmarshal: " + string(body) + "\n\n"
    log.Printf(errmsg)
		io.WriteString(w, errmsg)
		return
  }

	err := initSession()
	if err != nil {
		msg := "Error creating session: " + err.Error()
		log.Printf(msg)
		io.WriteString(w, "{\"status\":\"1\", \"message\":\""+ msg +"\"}")
		return
	}
	defer thesession.Close()

	query := fmt.Sprintf(
			"INSERT INTO %s.%s (id, run_ts, topic, status, events_counter) VALUES (%s, '%s', '%s', '%s', %d)\n",
			keyspace,
			table,
			e.Id, e.RunTS, e.Topic, e.Status, e.Counter)
	io.WriteString(w, query)
	err = thesession.Query(query).Exec()
	if err != nil {
		msg := "ERROR: Error writing to Cassandra " + err.Error() + "\n\n"
		io.WriteString(w, msg)
		log.Printf(msg)
		return
	}

}
