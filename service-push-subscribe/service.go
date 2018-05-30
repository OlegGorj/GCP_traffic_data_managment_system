package main

import (
	"fmt"
	"log"
	"net/http"
  _ "io"
  "io/ioutil"
  "encoding/json"
  _ "strconv"
  "time"
  _ "html/template"
  "os"
  "sync"
  b64 "encoding/base64"
	"bytes"

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

type publishEnvelope struct {
	Topic string  `json:"topic"`
	Data map[string]string `json:"data"`
}

func pushBackendCassandraRouter(w http.ResponseWriter, r *http.Request, fromtopic string) {

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
  sDec, _  := b64.StdEncoding.DecodeString( msg.Message.Data )

	// Unmarshal the envelope
//	var msg_envelope publishEnvelope
//  if err := json.Unmarshal([]byte(sDec), &msg_envelope); err != nil {
//		w.WriteHeader(http.StatusNotImplemented)
//		errmsg := "ERROR: Could not decode body into publishEnvelope with Unmarshal. sDec: " + string( msg.Message.Data ) + " Error: " + err.Error()
//		//io.WriteString(w, errmsg)
//    log.Fatalf(errmsg)
//		return
//  }

	// calling cassandra service
//	callCassandraClientService( msg_envelope.Topic, string(msg_envelope.Data) )
	callCassandraClientService( fromtopic, string(sDec) )

	w.WriteHeader(http.StatusOK)
}

func callCassandraClientService(topic string, sDec string){

	c := &http.Client{
   Timeout: 60 * time.Second,
	}
	// Based on the name of the topic, determine which table record(s) should be sent to..
	// This part should be obtained from Config service
	serviceUri := cassandraServiceUri
	if topic == trafficTrackingTopic {
		serviceUri = cassandraServiceUri + "/northamerica/datasetentry"
	}else if topic == sessionsTopic {
		serviceUri = cassandraServiceUri + "/common/sessions"
	}else {
		log.Print("ERROR: Unsuppoerted value of key \"topic\" in envelope\n\n")
		return
	}

	log.Print("DEBUG: Calling pub service at  " + serviceUri + "with the payload(base64): " + b64.StdEncoding.EncodeToString( []byte(sDec) ) )

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

func createKeyValuePairsAsString(m map[string]string) string {
    b := new(bytes.Buffer)
    for key, value := range m {
        fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
    }
    return b.String()
}

// eof
