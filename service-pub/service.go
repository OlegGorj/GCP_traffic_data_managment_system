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
  _ "sync"
  _  "encoding/base64"

  "cloud.google.com/go/pubsub"
  "golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/api/iterator"
)

var (
	projectName string
)

func main() {

	projectName = getENV("GOOGLE_CLOUD_PROJECT")

	http.HandleFunc("/_ah/health", healthCheckHandler)
	http.HandleFunc("/publish", publishHandler)
	log.Print("Starting service.....")
	appengine.Main()

}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}


type publishEnvelope struct {
	Topic string  `json:"topic"`
	Data map[string]string `json:"data"`
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := context.Background()

	switch {

	  case r.Method == "POST":
			if r.Body == nil {
					errormsg := "ERROR: Please send a request body"
			    log.Fatalf(errormsg + "%v", errormsg)
					io.WriteString(w, "{\"status\":\"1\", \"" + errormsg + "\":\"ok\"}" )
		      return
		  }
		  body, err := ioutil.ReadAll(r.Body)
		  defer r.Body.Close()
		  if err != nil {
				errormsg := "ERROR:  Can't read http body ioutil.ReadAll"
		    log.Fatalf(errormsg + "%v", err)
				io.WriteString(w, "{\"status\":\"1\", \"" + errormsg + "\":\"ok\"}" )
				return
			}
			var msg publishEnvelope
		  if err := json.Unmarshal([]byte(body), &msg); err != nil {
		    log.Fatalf("ERROR: Could not decode body into publishEnvelope with Unmarshal: %s \n", string(body))
		  }
			jsondata, _ := json.Marshal(msg.Data)
			//log.Printf("DEBUG: Topic from envelope: "    + msg.Topic + "\n Data-json from envelope"    + string(jsondata) + "\n")
			topicName := msg.Topic
			if topicName == "" {
				log.Fatalf("ERROR: Topic name is empty")
			}

			// publish to topic
			// log.Print("DEBUG: Calling PUB service at project " + projectName)
			client, err := pubsub.NewClient(ctx, projectName)
			if err != nil {
				log.Fatalf("Could not create pubsub Client:" + err.Error() + "for project" + projectName)
			}

			if err := publish(client, topicName, string(jsondata) ); err != nil {
				log.Fatalf("Failed to publish: %v. Topic name: %s\n", err, topicName)
			}

	  default:
	      http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}" )
}

func list(client *pubsub.Client) ([]*pubsub.Topic, error) {
	ctx := context.Background()
	var topics []*pubsub.Topic

	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done { break }
		if err != nil { return nil, err }
		topics = append(topics, topic)
	}

	return topics, nil
}

func publish(client *pubsub.Client, topic, msg string) error {
	ctx := context.Background()
	t := client.Topic(topic)
	result := t.Publish(ctx, &pubsub.Message{
	Data: []byte(msg),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil { return err }

	log.Print("Published a message; msg ID: " + id + "\n")

	return nil
}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}

// eof
