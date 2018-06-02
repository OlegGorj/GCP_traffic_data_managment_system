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
	"strconv"
	"runtime/debug"
	"strings"

	"google.golang.org/appengine"
	"github.com/gorilla/mux"
	_ "cloud.google.com/go/pubsub"
  _ "golang.org/x/net/context"
	"github.com/gocql/gocql"

	"github.com/newrelic/go-agent"
)

var (
	projectName string

	pubServiceUri string
	sourceSODAUri_Chicago string

	publishTopic string
	sessionsTopic string
	controlsTopic string
	isSchemaDefined bool
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
	sourceSODAUri_Chicago = getENV("DATASOURCE_SODA_CHICAGO_URI")

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
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/liveness_check", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/readiness_check", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/_ah/health", healthCheckHandler))
	r.HandleFunc(newrelic.WrapHandleFunc(app, "/schedule", scheduleHandler)).Methods("GET")
	r.HandleFunc(newrelic.WrapHandleFunc(app,"/", homeHandler))

	r.HandleFunc(newrelic.WrapHandleFunc(app, "/{country}/{state}/{city}/{catalog}/{category}/{dataset}", cityRouterHandler)).Queries("schema", "{schema}").Methods("GET", "POST")

	err = r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err := route.GetPathTemplate()
		if err == nil {
			fmt.Println("ROUTE:", pathTemplate)
		}
		pathRegexp, err := route.GetPathRegexp()
		if err == nil {
			fmt.Println("Path regexp:", pathRegexp)
		}
		queriesTemplates, err := route.GetQueriesTemplates()
		if err == nil {
			fmt.Println("Queries templates:", strings.Join(queriesTemplates, ","))
		}
		queriesRegexps, err := route.GetQueriesRegexp()
		if err == nil {
			fmt.Println("Queries regexps:", strings.Join(queriesRegexps, ","))
		}
		methods, err := route.GetMethods()
		if err == nil {
			fmt.Println("Methods:", strings.Join(methods, ","))
		}
		fmt.Println()
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}

	http.Handle("/", r)

	log.Print("Starting service.....")
	appengine.Main()

}
func cityRouterHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	country := strings.ToLower(mux.Vars(r)["country"])
	state := strings.ToLower(mux.Vars(r)["state"])
	city := strings.ToLower(mux.Vars(r)["city"])
	if country == "" || state == "" || city == "" {
		io.WriteString(w, "ERROR:  Can't have {country}, {state}, {city}, {catalog}, {category} or {dataset} empty...\n")
		return
	}
	switch city {
	case "chicago":
			catalogChicagoHandler(w, r)

		default:
				io.WriteString(w,"ERROR:  Specified city " + city +" is not supported...\n")
				return
	}

	w.WriteHeader(http.StatusOK)
}

func catalogChicagoHandler(w http.ResponseWriter, r *http.Request) {

	catalog := strings.ToLower(mux.Vars(r)["catalog"])
	category := strings.ToLower(mux.Vars(r)["category"])
	dataset := strings.ToLower(mux.Vars(r)["dataset"])
	schema := strings.ToLower(mux.Vars(r)["schema"])

	if schema == "" || schema == "false" {
		log.Print( "DEBUG:  Assuming schema is not present..\n")
		isSchemaDefined = false
	}else{
		log.Print( "DEBUG: Use Schema to store "+ dataset + "\n")
		isSchemaDefined = true
	}

	// making assumptions here - service passing table and keyspace is aware and passing correct ones
	//  i.e. no error checking at this time (TODO)

	if catalog == "" || category == "" || dataset == ""{
		io.WriteString(w, "ERROR:  Can't have {country}, {state}, {city}, {catalog}, {category} or {dataset} empty...\n")
		return
	}
	if category == "transportation" {

		switch dataset {
		case "chicago-traffic-tracker-historical-congestion-estimates-by-segment-2018-current":
				ChicagoTrafficTrackerHistoricalCongestionEstimatesBySegment2018Current_DataHandler(w, r)
				return

			case "chicago-traffic-tracker-congestion-estimates-by-segment":
				ChicagoTrafficTrackerCongestionEstimatesBySegment_DataHandler(w, r)
				return

			default:
					io.WriteString(w,"ERROR:  Specified dataset is not supported...\n")
					return
		}

	} else if category == "environment" {
		switch dataset {
			case "energy-usage-2010":
					return

			default:
					io.WriteString(w, "ERROR:  Specified table is not supported...\n")
					return
		}

	}else {
		io.WriteString(w,"ERROR:  Specified category is not supported...\n")
		return
	}

	//io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}")
}


func ChicagoTrafficTrackerHistoricalCongestionEstimatesBySegment2018Current_DataHandler(w http.ResponseWriter, r *http.Request) {

}

func ChicagoTrafficTrackerCongestionEstimatesBySegment_DataHandler(w http.ResponseWriter, r *http.Request) {

	var recordsCounter int
	var lastUpdt string
	status := "ok"
	session_id := gocql.TimeUUID()
	run_ts := time.Now().String()

	w.Header().Set("Content-Type", "application/json")

	debug_msg := fmt.Sprintf("DEBUG: Ending session with ID: %s and count of records: %d \n",  session_id.String(), recordsCounter)
	io.WriteString(w, debug_msg)

	sodareq := soda.NewGetRequest(sourceSODAUri_Chicago, "")
	sodareq.Format = "json"
	//sodareq.Query.Limit = 10
  sodareq.Query.AddOrder("_last_updt", soda.DirAsc)
	//sodareq.Query.Where = "_last_updt> '2018-05-29 00:00:00.0'"

  ogr, err := soda.NewOffsetGetRequest(sodareq)
	if err != nil {
		errmsg := "SODA call failed: " + err.Error()
		err = recordSession( sessionStruct{
				Id: session_id.String(),
				Status: status,
				RunTS: run_ts,
				Topic: publishTopic,
				Counter: strconv.Itoa(recordsCounter),
				LastUpdt: lastUpdt,
				} )
		io.WriteString(w, errmsg )
		log.Fatalf("SODA call failed: %v",err)
		return
	}

  for i := 0; i < 4; i++ {
		ogr.Add(1)
		go func() {
			defer ogr.Done()

			for ;; {
				resp, err := ogr.Next(1)
				if err == soda.ErrDone { break }
				if err != nil { log.Fatal(err) }

				results := make([]map[string]interface{}, 0)
				err = json.NewDecoder(resp.Body).Decode(&results)
				defer resp.Body.Close()
				if err != nil { log.Fatalf("Can't Decode message from SODA call: %v",err) }

				//Process the data
        for _, r := range results {
					// increase counter
					recordsCounter++
					// contruct message envelope
					json_data := fmt.Sprintf(
						"{ \"session_id\": \"%s\", \"_last_updt\": \"%s\", \"_direction\": \"%s\", \"_fromst\": \"%s\", \"_length\": \"%s\", \"_lif_lat\": \"%s\", \"_lit_lat\": \"%s\", \"_lit_lon\": \"%s\", \"_strheading\": \"%s\", \"_tost\": \"%s\" , \"_traffic\": \"%s\", \"segmentid\": \"%s\", \"start_lon\": \"%s\", \"street\": \"%s\" } \n",
						session_id.String(), r["_last_updt"], r["_direction"], r["_fromst"], r["_length"], r["_lif_lat"], r["_lit_lat"], r["_lit_lon"], r["_strheading"], r["_tost"] , r["_traffic"], r["segmentid"], r["start_lon"], r["street"]  )

					callPublishService(publishTopic, []byte(json_data), session_id.String())

					//log.Print("INFO: Response from pub service ("+ pubServiceUri +"): " + string(body_byte) + "\n\n")
					lastUpdt = fmt.Sprintf("%s", r["_last_updt"])
      	} // end of range

			}
		}()

	}
	ogr.Wait()

	debug_msg = fmt.Sprintf("DEBUG: Ending session with ID: %s and count of records: %d \n",  session_id.String(), recordsCounter)
	io.WriteString(w, debug_msg)

	// publish session details
	err = recordSession( sessionStruct{
			Id: session_id.String(),
			Status: status,
			RunTS: run_ts,
			Topic: publishTopic,
			Counter: strconv.Itoa(recordsCounter),
			LastUpdt: lastUpdt,
			} )
	if err != nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	// publish last updated date to control topic
	// TODO

	w.WriteHeader(http.StatusOK)
	debug.FreeOSMemory()
}


func recordSession(session sessionStruct) error {
	sessionStr, _ := json.Marshal(session)
	log.Print("DEBUG: sessionStr: " + string(sessionStr))
	// publish session
	return callPublishService(sessionsTopic, sessionStr, session.Id )
}

func callPublishService(topic string, message []byte, session_id string) error {
	s_id := session_id
	if session_id == "" {
		s_id = "_"
	}

	log.Print("DEBUG: POST callPublishService: topic: " + topic)
	rsp, err := http.Post(pubServiceUri + "/" + topic + "/" + s_id + "?schema=" + strconv.FormatBool(isSchemaDefined), "application/json", bytes.NewBuffer(message) )
	defer rsp.Body.Close()
	_, err = ioutil.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	return nil
}


func getENV(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}



func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	//fmt.Fprint(w, "ok")
	fmt.Fprint(w, "{\"alive\": true}" )
}
func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Implemented endpoints:\n")
	fmt.Fprint(w, "GET /schedule\n")
	fmt.Fprint(w, "POST/GET /us/il/chicago/data/transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Segment  \n")
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
