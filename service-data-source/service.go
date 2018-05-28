package main

import (
	"fmt"
	"github.com/SebastiaanKlippert/go-soda"
  "log"
	"io"
	"os"
	"net/http"
	"encoding/json"
	"bytes"
	"io/ioutil"
	"time"

	"google.golang.org/appengine"
	_ "google.golang.org/appengine/log"

	"cloud.google.com/go/pubsub"
  "golang.org/x/net/context"
	"github.com/gocql/gocql"

	"github.com/newrelic/go-agent"
)

var (
	pubServiceUri string
	sourceSODAUri string
	publishTopic string
	projectName string
	sessionsTopic string
)

type sessionStruct struct {
		Id string `json:"id"`
    RunTS string `json:"run_ts"`
		Topic string `json:"topic"`
		Status string `json:"status"`
		Counter int `json:"counter"`
}

//
// endpoint
// /us/il/chicago/Transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Segment/
//

func main() {

	pubServiceUri = getENV("PUBLISH_SERVICE")
	sourceSODAUri = getENV("DATASOURCE_SODA_URI")
	publishTopic = getENV("PUBSUB_TOPIC")
	sessionsTopic = getENV("SESSIONS_TOPIC")
	projectName = getENV("GOOGLE_CLOUD_PROJECT")
	newrelicKey := getENV("NEWRELIC_KEY")

	//  newrelic part
	config := newrelic.NewConfig("datasource-soda-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/soda_pull_service", pullSODADataHandler))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/schedule", scheduleHandler))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))

	//  newrelic part
	config1 := newrelic.NewConfig("publish-service", newrelicKey)
	app1, err1 := newrelic.NewApplication(config1)
	if err1 != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}
	http.HandleFunc(newrelic.WrapHandleFunc(app1, "/publish", publishHandler))

	log.Print("Starting service.....")
	appengine.Main()

}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func scheduleHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
	log.Print("scheduleHandler called..")
}


func recordSession(t time.Time, status string, topic string, counts int) int {
	session_id := gocql.TimeUUID()
	// construct session
	session := &sessionStruct{
		Id: session_id.String(),
		Status: status,
		RunTS: t.String(),
		Topic: topic,
		Counter: counts,
	}
	sessionStr, _ := json.Marshal(session)

	log.Print("DEBUG: sessionStr: " + string(sessionStr))

	// publish session
	go func() {
		log.Print("DEBUG: Calling PUB service at project " + projectName)

		ctx := context.Background()
		client, err := pubsub.NewClient(ctx, projectName)
		if err != nil {
			log.Fatalf("Could not create pubsub Client:" + err.Error() + "for project" + projectName)
			return
		}
		t := client.Topic(topic)
		result := t.Publish( ctx, &pubsub.Message{Data: []byte(sessionStr)} )
		id, err := result.Get(ctx)
		if err != nil {
			log.Print("ERROR: could not get published message ID from PUBSUB: " + err.Error() + "\n")
			return
		}
		log.Print("DEBUG: Published session; msg ID: " + id + "\n")
	}()

	return 0
}


func pullSODADataHandler(w http.ResponseWriter, r *http.Request) {
	var recordsCounter int
	//var succesFlag bool

	w.Header().Set("Content-Type", "application/json")

	//recordSession(time.Now(), "Ok", sessionsTopic, recordsCounter)

	sodareq := soda.NewGetRequest(sourceSODAUri, "")
	sodareq.Format = "json"
	//sodareq.Query.Limit = 10
  sodareq.Query.AddOrder("_last_updt", soda.DirAsc)

  ogr, err := soda.NewOffsetGetRequest(sodareq)
	if err != nil {
		errmsg := "{\"status\":\"1\", \"" + "SODA call failed: " + err.Error() + "\":\"ok\"}"
		recordSession(time.Now(), errmsg, sessionsTopic, 0)
		io.WriteString(w, errmsg )
		log.Fatalf("SODA call failed: %v",err)
		return
	}

  for i := 0; i < 14; i++ {
		ogr.Add(1)
		go func() {
			defer ogr.Done()

			for ;; {
				resp, err := ogr.Next(1)
				if err == soda.ErrDone { break }
				if err != nil { log.Fatal(err) }

				results := make([]map[string]interface{}, 0)
				err = json.NewDecoder(resp.Body).Decode(&results)
				resp.Body.Close()
				if err != nil { log.Fatalf("Can't Decode message from SODA call: %v",err) }

				//Process the data
        for _, r := range results {
					// increase counter
					recordsCounter++

					// contruct message envelope
					json_data := fmt.Sprintf(
						"{ \"_last_updt\": \"%s\", \"_direction\": \"%s\", \"_fromst\": \"%s\", \"_length\": \"%s\", \"_lif_lat\": \"%s\", \"_lit_lat\": \"%s\", \"_lit_lon\": \"%s\", \"_strheading\": \"%s\", \"_tost\": \"%s\" , \"_traffic\": \"%s\", \"segmentid\": \"%s\", \"start_lon\": \"%s\", \"street\": \"%s\" } \n",
						r["_last_updt"], r["_direction"], r["_fromst"], r["_length"], r["_lif_lat"], r["_lit_lat"], r["_lit_lon"], r["_strheading"], r["_tost"] , r["_traffic"], r["segmentid"], r["start_lon"], r["street"]  )
					json_full := constructEnvelope(publishTopic, json_data)
					//io.WriteString(w, json_full)

					// calling pub service
					//log.Print("DEBUG: Calling pub service at  " + pubServiceUri + "with the payload: \n" + json_full + "\n")
					rsp, err := http.Post(pubServiceUri, "application/json", bytes.NewBufferString(json_full))
					defer rsp.Body.Close()
					/*body_byte*/ _, err = ioutil.ReadAll(rsp.Body)
					if err != nil {
						panic(err)
					}
					//log.Print("INFO: Response from pub service ("+ pubServiceUri +"): " + string(body_byte) + "\n\n")
      	}

			}
		}()

	}
	ogr.Wait()

	recordSession(time.Now(), "Ok", sessionsTopic, recordsCounter)

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}" )

}

func constructEnvelope(topic, data string) string {
	return fmt.Sprintf( "{\"data\": %s , \"topic\":\"%s\"}", data, topic)
}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}


type publishEnvelope struct {
	Topic string  `json:"topic"`
	Data map[string]string `json:"data"`
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := context.Background()

	switch {
		case r.Method == "GET":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "{\"status\":\"0\", \"message\":\"method GET not supported\"}" )

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
				w.WriteHeader(http.StatusOK)
				msg := "ERROR: Could not decode body into publishEnvelope with Unmarshal: %s \n" + string(body) + "\n Error: " + err.Error()
				io.WriteString(w, msg)
		    log.Fatalf(msg)
				return
		  }
			jsondata, _ := json.Marshal(msg.Data)
			//log.Printf("DEBUG: Topic from envelope: "    + msg.Topic + "\n Data-json from envelope"    + string(jsondata) + "\n")
			topicName := msg.Topic
			if topicName == "" {
				log.Fatalf("ERROR: Topic name is empty")
			}
			go func() {
				// publish to topic
				//log.Print("DEBUG: Calling PUB service at project " + projectName)
				client, err := pubsub.NewClient(ctx, projectName)
				if err != nil {
					log.Fatalf("Could not create pubsub Client:" + err.Error() + "for project" + projectName)
				}
				if err := publish(client, topicName, string(jsondata) ); err != nil {
					log.Fatalf("Failed to publish: %v. Topic name: %s\n", err, topicName)
				}
			}()

	  default:
	      http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}" )
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
	if err != nil {
		log.Print("ERROR: could not get published message ID from PUBSUB: " + err.Error() + "\n")
		return err
	}

	log.Print("DEBUG: Published a message; msg ID: " + id + "\n")

	return nil
}
