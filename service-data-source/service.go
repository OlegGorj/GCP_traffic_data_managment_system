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
	"github.com/gorilla/mux"
	"cloud.google.com/go/pubsub"
  "golang.org/x/net/context"
	"github.com/gocql/gocql"

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
		Counter int `json:"counter"`
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
	config := newrelic.NewConfig("datasource-soda-service", newrelicKey)
	app, err := newrelic.NewApplication(config)
	if err != nil {
    log.Printf("ERROR: Issue with initializing newrelic application ")
	}

	r := mux.NewRouter()
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/schedule", scheduleHandler)).Methods("GET")
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/{country}/{state}/{city}/{catalog}/{category}/{dataset}", catalogHandler)).Methods("GET", "POST")
	r.HandleFunc(newrelic.WrapHandleFunc(app,"/", homeHandler))

	//  newrelic part
	//config1 := newrelic.NewConfig("publish-service", newrelicKey)
	//app1, err1 := newrelic.NewApplication(config1)
	//if err1 != nil {
 	//	log.Printf("ERROR: Issue with initializing newrelic application ")
	//}
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/publish/{topic}", publishToTopicWEnvelopePOSTHandler)).Methods("POST")
	http.Handle("/", r)

	log.Print("Starting service.....")
	appengine.Main()

}

func catalogHandler(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	country := mux.Vars(r)["country"]
	state := mux.Vars(r)["state"]
	city := mux.Vars(r)["city"]
	catalog := mux.Vars(r)["catalog"]
	category := mux.Vars(r)["category"]
	dataset := mux.Vars(r)["dataset"]

	// making assumptions here - service passing table and keyspace is aware and passing correct ones
	//  i.e. no error checking at this time (TODO)

	if country == "" || state == "" || city == "" || catalog == "" || category == "" || dataset == ""{
		io.WriteString(w, "ERROR:  Can't have {country}, {state}, {city}, {catalog}, {category} or {dataset} empty...\n")
		return
	}
	if category == "transportation" {

		switch dataset {
			case "Chicago-Traffic-Tracker-Congestion-Estimates-by-Segment":
				ChicagoTrafficTrackerCongestionEstimatesBySegment_DataHandler(w, r)

			default:
					io.WriteString(w,"ERROR:  Specified dataset is not supported...\n")
					return
		}

	} else if category == "environment" {
		switch dataset {
			case "Energy-Usage-2010":
					return

			default:
					io.WriteString(w, "ERROR:  Specified table is not supported...\n")
					return
		}

	}else {
		io.WriteString(w,"ERROR:  Specified keyspace is not supported...\n")
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}")
}




func ChicagoTrafficTrackerCongestionEstimatesBySegment_DataHandler(w http.ResponseWriter, r *http.Request) {

	var recordsCounter int
	var lastUpdt string
	status := "ok"
	session_id := gocql.TimeUUID()
	run_ts := time.Now().String()

	io.WriteString(w, "DEBUG: Starting session with ID: " +  session_id.String() )

	w.Header().Set("Content-Type", "application/json")
	//recordSession(time.Now(), "Ok", sessionsTopic, recordsCounter)

	sodareq := soda.NewGetRequest(sourceSODAUri, "")
	sodareq.Format = "json"
	//sodareq.Query.Limit = 10
  sodareq.Query.AddOrder("_last_updt", soda.DirAsc)
	//sodareq.Query.Where = "_last_updt> '2018-05-29 00:00:00.0'"

  ogr, err := soda.NewOffsetGetRequest(sodareq)
	if err != nil {
		errmsg := "{\"status\":\"1\", \"" + "SODA call failed: " + err.Error() + "\":\"ok\"}"
		err = recordSession( sessionStruct{
				Id: session_id.String(),
				Status: status,
				RunTS: run_ts,
				Topic: sessionsTopic,
				Counter: recordsCounter,
				LastUpdt: lastUpdt,
				} )
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
						"{ \"session_id\": \"%s\", \"_last_updt\": \"%s\", \"_direction\": \"%s\", \"_fromst\": \"%s\", \"_length\": \"%s\", \"_lif_lat\": \"%s\", \"_lit_lat\": \"%s\", \"_lit_lon\": \"%s\", \"_strheading\": \"%s\", \"_tost\": \"%s\" , \"_traffic\": \"%s\", \"segmentid\": \"%s\", \"start_lon\": \"%s\", \"street\": \"%s\" } \n",
						session_id.String(), r["_last_updt"], r["_direction"], r["_fromst"], r["_length"], r["_lif_lat"], r["_lit_lat"], r["_lit_lon"], r["_strheading"], r["_tost"] , r["_traffic"], r["segmentid"], r["start_lon"], r["street"]  )

					go callPublishService(publishTopic, []byte(json_data))

					//log.Print("INFO: Response from pub service ("+ pubServiceUri +"): " + string(body_byte) + "\n\n")
					lastUpdt = fmt.Sprintf("%s", r["_last_updt"])
      	} // end of range

			}
		}()

	}
	ogr.Wait()

	// publish session details
	err = recordSession( sessionStruct{
			Id: session_id.String(),
			Status: status,
			RunTS: run_ts,
			Topic: sessionsTopic,
			Counter: recordsCounter,
			LastUpdt: lastUpdt,
			} )

	// publish last updated date to control topic
	// TODO

	//w.WriteHeader(http.StatusOK)
	//io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}" )
}


func recordSession(session sessionStruct) error {
	sessionStr, _ := json.Marshal(session)

	log.Print("DEBUG: sessionStr: " + string(sessionStr))
	// publish session
	err := callPublishService(sessionsTopic, sessionStr)
	return err
}

func callPublishService(topic string, message []byte) error {

	log.Print("DEBUG: POST callPublishService: topic: " + topic)

	rsp, err := http.Post(pubServiceUri + "/" + topic, "application/json", bytes.NewBuffer(message) )
	defer rsp.Body.Close()
	if /*body_byte*/ _, err = ioutil.ReadAll(rsp.Body) ; err != nil {
		panic(err)
	}
	return nil
}



func publishToTopicWEnvelopePOSTHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch {

	  case r.Method == "POST":

			topic := mux.Vars(r)["topic"]

			if r.Body == nil {
					errormsg := "ERROR: Please send a request body"
					io.WriteString(w, errormsg  )
					log.Fatalf(errormsg + "%v", errormsg)
		      return
		  }
		  body, err := ioutil.ReadAll(r.Body)
		  defer r.Body.Close()
		  if err != nil {
				errormsg := "ERROR:  Can't read http body ioutil.ReadAll"
		    log.Fatalf(errormsg + "%v", err)
				io.WriteString(w, errormsg  )
				return
			}

			go func() {
				// publish to topic
				if err := publishToTopicWEnvelope(projectName, topic, string(body) ); err != nil {
					log.Fatalf("Failed to publish: %v. Topic name: %s\n", err, topic)
				}
			}()

	  default:
	      http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}" )
}


func publishToTopicWEnvelope(projectName, topic, msg string) error {

	//
	// Do envelope wrapping here
	//
	json_full := constructEnvelope(topic, msg)

	ctx := context.Background()
	//log.Print("DEBUG: Calling PUB service at project " + projectName)
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
	// USE publishEnvelope instead
	return fmt.Sprintf( "{\"data\": %s , \"topic\":\"%s\"}", data, topic)
}

func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}



func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}
func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func scheduleHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
	log.Print("scheduleHandler called..")
}

// { "ts":"2018-05-01 10:10:20", "topic":"us.chicago-city.transportation.traffic-tracker-congestion-estimates", "last_updated":"2008-05-24 14:21:04", "counter":90 }
// { "ts":"2018-05-02 10:10:20", "topic":"us.chicago-city.transportation.traffic-tracker-congestion-estimates", "last_updated":"2008-05-26 14:21:04", "counter":110 }
// { "ts":"2018-05-03 10:10:20", "topic":"us.chicago-city.transportation.traffic-tracker-congestion-estimates", "last_updated":"2008-05-25 14:21:04", "counter":110 }
// { "ts":"2018-05-04 10:10:20", "topic":"us.chicago-city.transportation.traffic-tracker-congestion-estimates", "last_updated":"2008-05-29 14:21:04", "counter":110 }
/*
type controlStruct struct {
		Ts string `json:"ts"`
		Topic string `json:"topic"`
		LastUpdated string `json:"last_updated"`
		Counter int `json:"counter"`
}

var (
	 prev_last_updated string
	 last_updated string
)

func getLastUpdatedDate(w http.ResponseWriter, r *http.Request){
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		fmt.Fprint(w, "Could not create pubsub Client:" + err.Error() + "for project" + projectName)
		log.Fatalf("Could not create pubsub Client:" + err.Error() + "for project" + projectName)
	}

	sub := client.Subscription("pull-common.controls")
	sub.ReceiveSettings.MaxExtension = 10 * time.Second
	defer client.Close()
	//var prev_last_updated time
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		last_updated = string(m.Data)
	 	log.Printf("Got message: %s", m.Data)

		var e controlStruct
	  if err := json.Unmarshal(m.Data , &e); err != nil {
			errmsg := "ERROR: Could not decode body into controlStruct type with Unmarshal: " + string(m.Data) + "\n\n"
	    log.Printf(errmsg)
			io.WriteString(w, errmsg)
	  }
		if e.Topic == publishTopic {

			log.Printf("The message related to topic : "+ publishTopic)

			m.Ack()
			last_updated = e.LastUpdated
			t_t1, _ := getTimeFromString(last_updated)
			t_t2, _ := getTimeFromString(prev_last_updated)
			if prev_last_updated == "" {
				prev_last_updated = last_updated
			} else if t_t1.After( t_t2 ) {
				// push to prev_last_updated to one the older
				prev_last_updated = last_updated
			}

			log.Printf("Between " + prev_last_updated+ " and "+last_updated +"... Oldest last_updated date is: "+ prev_last_updated + "\n\n")
		}
	 })
	 if err != context.Canceled {
		 return
	 }
}

func getDateNow() string {
	timenow := time.Now()
	changedtime := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", timenow.Year(), timenow.Month(),timenow.Day(), timenow.Hour(),timenow.Minute(),timenow.Second()  )
	return changedtime
}

func getTimeFromString(t string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", t )
}
*/
