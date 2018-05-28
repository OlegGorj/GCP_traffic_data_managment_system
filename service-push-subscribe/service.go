package main

import (
	"fmt"
	"log"
	"net/http"
  "io"
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
	datastoreServiceUri string
)

func main() {
	datastoreServiceUri = getENV("CASSANDRA_SERVICE")
	newrelicKey := getENV("NEWRELIC_KEY")

	config := newrelic.NewConfig("subscription-push-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}
	//http.HandleFunc(newrelic.WrapHandleFunc(app, "/push/cassandra", pushCassandraHandler))
	//http.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))

	r := mux.NewRouter()
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/push/{backend}", pushHandler))
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

func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func pushHandler(w http.ResponseWriter, r *http.Request) {

	//vars := mux.Vars(r)
	backend := mux.Vars(r)["backend"]
	if backend == "" {
		log.Printf("ERROR:  Can't have {backend} empty...\n")
		return
	}
	switch backend {
		case "cassandra":
				pushCassandraHandler(w, r)

		case "datastore":
				log.Printf("ERROR:  Specified backend is not supported...\n")
				return

		default:
				log.Printf("ERROR:  Specified backend is not supported...\n")
				return
	}
}

func pushCassandraHandler(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

  if r.Body == nil {
      log.Print("ERROR: Please send a request body")
      return
  }
  body, err := ioutil.ReadAll(r.Body)
  defer r.Body.Close()
  if err != nil {
    log.Printf("INFO:  Can't read http body ioutil.ReadAll... ")
		return
	}
  var msg pushRequest
  if err := json.Unmarshal([]byte(body), &msg); err != nil {
    log.Printf("ERROR: Could not decode body with Unmarshal: %s \n", string(body))
  }
  sDec, _  := b64.StdEncoding.DecodeString( msg.Message.Data )
  //var data entityEntryJSONStruct
  //if err := json.Unmarshal(sDec, &data); err != nil {
  //  log.Printf("ERROR: Could not decode Message.Data into Entry type with Unmarshal: " + string(sDec) + "\n")
  //}

	func (){
		// calling cassandra service

		log.Print("DEBUG: Calling pub service at  " + datastoreServiceUri + "with the payload: \n" + string(sDec) + "\n")

		c := &http.Client{
	    Timeout: 60 * time.Second,
		}
		rsp, err := c.Post(datastoreServiceUri, "application/json", bytes.NewBuffer(sDec))
		defer rsp.Body.Close()
		body_byte, err := ioutil.ReadAll(rsp.Body)
		if err != nil { panic(err) }
		log.Print("DEBUG: Response from cassandra service ("+ datastoreServiceUri +"): " + string(body_byte) + "\n\n")
	}()

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


// eof
