package main

import (
	"fmt"
	"log"
	"net/http"
  _ "io"
  "io/ioutil"
  "encoding/json"
  "time"
  "os"
  "sync"
  b64 "encoding/base64"
	"bytes"
	_ "strconv"
	_ "runtime/debug"

	"google.golang.org/appengine"
  "cloud.google.com/go/pubsub"
	"github.com/gorilla/mux"

	"github.com/newrelic/go-agent"
)

var (
	messagesMu sync.Mutex
  countMu sync.Mutex
	count   int
  subscription *pubsub.Subscription
	cassandraServiceUri string
	trafficTrackingTopic string
	sessionsTopic string
)

func main() {

	cassandraServiceUri = getENV("CASSANDRA_SERVICE")
	trafficTrackingTopic = getENV("TRAFFIC_TRACKER_TOPIC")
	sessionsTopic = getENV("SESSIONS_TOPIC")
	newrelicKey := getENV("NEWRELIC_KEY")

	config := newrelic.NewConfig("push-subscription-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}

	r := mux.NewRouter()
	// /_ah/push-handlers/ prefix
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler)).Methods("GET")
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/push/{fromtopic}/{backend}", pushHandler)).Methods("POST")
	r.HandleFunc("/", homeHandler).Methods("GET")
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

func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
	log.Print("health check called..")
}

func pushHandler(w http.ResponseWriter, r *http.Request) {
	backend := mux.Vars(r)["backend"]
	fromtopic := mux.Vars(r)["fromtopic"]
	if backend == "" || fromtopic == "" {
		log.Printf("ERROR:  Can't have 'backen' or 'fromtopic' empty...\n")
		return
	}

	switch backend {
		case "cassandra":
				pushBackendCassandraRouter(w, r, fromtopic)

		case "datastore":
				log.Printf("ERROR:  Specified backend is not supported...\n")
				return

		case "spanner":
				log.Printf("ERROR:  Specified backend is not supported...\n")
				return

		default:
				log.Printf("ERROR:  Specified backend is not supported...\n")
				return
	}
}

//type publishEnvelope struct {
//	Topic string  `json:"topic"`
//	Data map[string]string `json:"data"`
//}

func pushBackendCassandraRouter(w http.ResponseWriter, r *http.Request, fromtopic string) {

	time.Sleep(4 * time.Second)

	w.Header().Set("Content-Type", "application/json")

  if r.Body == nil {
      log.Print("ERROR: Request body missing\n")
      return
  }
  body, err := ioutil.ReadAll(r.Body)
  defer r.Body.Close()
  if err != nil {
		w.WriteHeader(http.StatusNotImplemented)
    log.Printf("INFO:  Can't read http body ioutil.ReadAll... ")
		return
	}
  var msg pushRequest
  if err := json.Unmarshal([]byte(body), &msg); err != nil {
		w.WriteHeader(http.StatusNotImplemented)
    log.Printf("ERROR: Could not decode body with Unmarshal: %s \n", string(body))
		return
  }

	schema := msg.Message.Attributes["schema"]
	pubsub_topic := msg.Message.Attributes["topic"]
	session_id := msg.Message.Attributes["session_id"]

	log.Print("DEBUG: pushBackendCassandraRouter: pubsub_topic: " + pubsub_topic+ " schema: " + schema + " session_id: " + session_id)


	sDec, _  := b64.StdEncoding.DecodeString( msg.Message.Data )
	// calling cassandra service
	callCassandraClientService( fromtopic, string(sDec), schema, session_id )

	w.WriteHeader(http.StatusOK)
}

func callCassandraClientService(topic string, sDec string, schema string, session_id string){

	c := &http.Client{
   Timeout: 60 * time.Second,
	}

	s_id := session_id
	if session_id == "" {
		s_id = "_"
	}
	serviceUri := cassandraServiceUri + "/" + topic + "/" + s_id + "?schema=" + schema

	log.Print("DEBUG: Calling Cassandra service at  " + serviceUri + "with the payload(base64): " + b64.StdEncoding.EncodeToString( []byte(sDec) ) )

		rsp, err := c.Post(serviceUri, "application/json", bytes.NewBufferString(sDec))
		defer rsp.Body.Close()
		body_byte, err := ioutil.ReadAll(rsp.Body)
		if err != nil { panic(err) }

	log.Print("DEBUG: Response from cassandra service ("+ serviceUri +"): " + string(body_byte) + "\n\n")

}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}


// eof
