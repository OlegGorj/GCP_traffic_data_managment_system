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
  _ "encoding/base64"
	"bytes"

	"google.golang.org/appengine"

  "cloud.google.com/go/pubsub"
	_ "google.golang.org/appengine"
  _ "golang.org/x/net/context"

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

	http.HandleFunc("/_ah/health", healthCheckHandler)

	config1 := newrelic.NewConfig("publish-service", "df553dd04a541579cffd9a3a60c7afa9ca692cc7")
	app1, err1 := newrelic.NewApplication(config1)
	if err1 != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}
	http.HandleFunc(newrelic.WrapHandleFunc(app1, "/push", pushHandler))

  http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "This is main entry for endpoints..")
	})

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


func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}


func pushHandler(w http.ResponseWriter, r *http.Request) {
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
  //sDec, _  := b64.StdEncoding.DecodeString( msg.Message.Data )
  //var data entityEntryJSONStruct
  //if err := json.Unmarshal(sDec, &data); err != nil {
  //  log.Printf("ERROR: Could not decode Message.Data into Entry type with Unmarshal: " + string(sDec) + "\n")
  //}

	func (){
		// calling cassandra service
		//log.Print("DEBUG: Calling pub service at  " + pubServiceUri + "with the payload: \n" + json_full + "\n")
		rsp, err := http.Post(datastoreServiceUri, "application/json", bytes.NewBuffer(body))
		defer rsp.Body.Close()
		body_byte, err := ioutil.ReadAll(rsp.Body)
		if err != nil { panic(err) }
		log.Print("INFO: Response from cassandra service ("+ datastoreServiceUri +"): " + string(body_byte) + "\n\n")
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
