package main

import (
	"fmt"
  "log"
	"io"
	"os"
	"net/http"
	_ "encoding/json"
	_ "bytes"
	"io/ioutil"
	_ "time"
	_ "strconv"

	"google.golang.org/appengine"
	"github.com/gorilla/mux"
	"cloud.google.com/go/pubsub"
  "golang.org/x/net/context"
	_ "github.com/gocql/gocql"

	"github.com/newrelic/go-agent"
)

var (
	projectName string

	pubServiceUri string
	sourceSODAUri string

	publishTopic string
	sessionsTopic string
	controlsTopic string
)

type sessionStruct struct {
		Id string `json:"id"`
    RunTS string `json:"run_ts"`
		Topic string `json:"topic"`
		Status string `json:"status"`
		Counter string `json:"counter"`
		LastUpdt string `json:"last_updt"`
		// dataset ID - to be populated by Cassandra Clent service
}

func main() {

	pubServiceUri = getENV("PUBLISH_SERVICE")
	sourceSODAUri = getENV("DATASOURCE_SODA_URI")

	publishTopic = getENV("TRAFFIC_TRACKER_TOPIC")
	sessionsTopic = getENV("SESSIONS_TOPIC")
	controlsTopic = getENV("CONTROLS_TOPIC")

	projectName = getENV("GOOGLE_CLOUD_PROJECT")
	newrelicKey := getENV("NEWRELIC_KEY")

	//  newrelic part
	config := newrelic.NewConfig("publisher-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}

	r := mux.NewRouter()
	r.HandleFunc(newrelic.WrapHandleFunc(app,"/", homeHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/liveness_check", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/readiness_check", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/publish/{topic}", publishToTopicWEnvelopePOSTHandler)).Methods("POST")
	http.Handle("/", r)

	log.Print("Starting service.....")
	appengine.Main()

}


func publishToTopicWEnvelopePOSTHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	topic := mux.Vars(r)["topic"]
	if r.Body == nil {
			errormsg := "ERROR: Please send a request body"
			w.WriteHeader(http.StatusNotImplemented)
			io.WriteString(w, errormsg  )
			log.Fatalf(errormsg + "%v", errormsg)
     return
 	}
 	body, err := ioutil.ReadAll(r.Body)
 	defer r.Body.Close()
 	if err != nil {
		errormsg := "ERROR:  Can't read http body ioutil.ReadAll"
		w.WriteHeader(http.StatusNotImplemented)
		io.WriteString(w, errormsg  )
		log.Fatalf(errormsg + "%v", err)
		return
	}
	//go func() {
		if err := publishToTopicWEnvelope(projectName, topic, string(body) ); err != nil {
			w.WriteHeader(http.StatusNotImplemented)
			log.Fatalf("Failed to publish: %v. Topic name: %s\n", err, topic)
		}
	//}()

	w.WriteHeader(http.StatusOK)
	debug.FreeOSMemory()

}


func publishToTopicWEnvelope(projectName, topic, msg string) error {

	json_full := constructEnvelope(topic, msg)

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		log.Fatalf("Could not create pubsub Client:" + err.Error() + "for project" + projectName)
	}

	t := client.Topic(topic)
	result := t.Publish(ctx, &pubsub.Message{
	Data: []byte(json_full),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		log.Print("ERROR: could not get published message ID from PUBSUB: " + err.Error() + "\n")
		return err
	}

	log.Print("DEBUG: Published a message; msg ID: " + id + "\n")
	return nil
}

type publishEnvelope struct {
	Topic string  `json:"topic"`
	Data map[string]string `json:"data"`
}
func constructEnvelope(topic, data string) string {
	// USE publishEnvelope instead - TODO
	return fmt.Sprintf( "{\"data\": %s , \"topic\":\"%s\"}", data, topic)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Implemented endpoints:\n")
	fmt.Fprint(w, "POST /publish/{topic name}\n")
}

func scheduleHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
	log.Print("scheduleHandler called..")
}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}
