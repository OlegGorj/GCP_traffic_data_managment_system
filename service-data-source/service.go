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

	"google.golang.org/appengine"
	_ "google.golang.org/appengine/log"
	_ "google.golang.org/appengine/datastore"
	_ "golang.org/x/net/context"

)

var (
	pubServiceUri string
	sourceSODAUri string
	publishTopic string
)
//{
//    "_direction": "WB",
//    "_fromst": "Lake Shore Dr",
//    "_last_updt": "2011-08-10 00:00:00.0",
//    "_length": "0.37",
//    "_lif_lat": "41.896936",
//    "_lit_lat": "41.896835",
//    "_lit_lon": "-87.624241",
//    "_strheading": "E",
//    "_tost": "Michigan",
//    "_traffic": "-1",
//    "segmentid": "1284",
//    "start_lon": "-87.617048",
//    "street": "Chicago"
//}

func main() {

	pubServiceUri = getENV("PUBLISH_SERVICE")
	sourceSODAUri = getENV("DATASOURCE_SODA_URI")
	publishTopic = getENV("PUBSUB_TOPIC")
	http.HandleFunc("/_ah/health", healthCheckHandler)
	http.HandleFunc("/soda_pull_service", pullSODADataHandler)
	log.Print("Starting service.....")
	appengine.Main()
	//if err := http.ListenAndServe(":8081", nil); err != nil {
	//	log.Fatal(err)
	//}

}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func pullSODADataHandler(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	log.Print("pullSODADataHandler Method called\n")

	sodareq := soda.NewGetRequest(sourceSODAUri, "")
	sodareq.Format = "json"
	sodareq.Query.Limit = 10
  sodareq.Query.AddOrder("_last_updt", soda.DirAsc)

  ogr, err := soda.NewOffsetGetRequest(sodareq)
	if err != nil {
		io.WriteString(w, "{\"status\":\"1\", \"" + "SODA call failed: " + err.Error() + "\":\"ok\"}" )
		log.Fatalf("SODA call failed: %v",err)
		return
	}

  for i := 0; i < 14; i++ {
		ogr.Add(1)
		go func() {
			  defer ogr.Done()

//			for ;; {
				resp, err := ogr.Next(1)
//				if err == soda.ErrDone { break }
				if err != nil { log.Fatal(err) }

				results := make([]map[string]interface{}, 0)
				err = json.NewDecoder(resp.Body).Decode(&results)
				resp.Body.Close()
				if err != nil { log.Fatalf("Can't Decode message from SODA call: %v",err) }

				//Process the data
        for _, r := range results {

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
					body_byte, err := ioutil.ReadAll(rsp.Body)
					if err != nil { panic(err) }
					log.Print("INFO: Response from pub service: " + string(body_byte) + "\n\n")
      	}

//			}
		}()

	}
	ogr.Wait()

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
