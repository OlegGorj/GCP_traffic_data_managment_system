package main

import (
	_ "bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"net/http"
	"encoding/json"
	_ "strconv"
	_ "bytes"
	"strings"
	_ "text/tabwriter"
	_ "log"

	"google.golang.org/appengine/log"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	_ "golang.org/x/net/context"
	_ "cloud.google.com/go/datastore"

)

// Structs
type Catalog struct {
	Country    	string    `datastore:"country,noindex"`
	Name    	string    	`datastore:"name"`
	State			string			`datastore:"state"`
	Content    	string    `datastore:"content"`
	Timezone 	string			`datastore:"timezone,noindex"`
}

type Category struct {
	Type    	string    	`datastore:"type,noindex"`
}

type Dataset struct {
	created    	string	`datastore:"created"`
	description string	`datastore:"description"`
	frequency   string	`datastore:"frequency"`
	name    		string	`datastore:"name"`
	pii    			string	`datastore:"pii"`
	updated    	string	`datastore:"updated"`
}

type Entry struct {
	_direction		string	`json:"_direction"`
	_fromst				string	`json:"_fromst"`
	_last_updt		string	`json:"_last_updt"`
	_length				string	`json:"_length"`
	_lif_lat			string	`json:"_lif_lat"`
	_lit_lat			string	`json:"_lit_lat"`
	_lit_lon			string	`json:"_lit_lon"`
	_strheading		string	`json:"_strheading"`
	_tost					string	`json:"_tost"`
	_traffic			string	`json:"_traffic"`
	segmentid			string	`json:"segmentid"`
	start_lon			string	`json:"start_lon"`
	street				string	`json:"street"`
}

var (
	datasetParentKey string
)

// main
func main() {
	datasetParentKey = getENV("DATASET_PARENT_KEY")
	http.HandleFunc("/_ah/health", healthCheckHandler)
	http.HandleFunc("/entry", handlerEntry)
	appengine.Main()
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func handlerEntry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx_default := appengine.NewContext(r)

	log.Infof(ctx_default, "Starting service.....")
	if r.Body == nil {
      log.Infof(ctx_default, "ERROR: Please send a request body")
      return
  }
  body, err := ioutil.ReadAll(r.Body)
  defer r.Body.Close()
  if err != nil {
    log.Errorf(ctx_default, "INFO:  Can't read http body ioutil.ReadAll... ")
		return
	}

	fmt.Fprintf(w, "DEBUG: recieved request body: " + string(body) + "\n")

	b := `{ "_last_updt": "2010-07-21 14:50:17.0", "_direction": "SB", "_fromst": "Montrose", "_length": "0.5", "_lif_lat": "41.9615926286", "_lit_lat": "41.9542914171", "_lit_lon": "-87.6692326667", "_strheading": "N", "_tost": "Irving Park" , "_traffic": "-1", "segmentid": "163", "start_lon": "-87.6694419633", "street": "Ashland" }`

	results := make([]map[string]interface{}, 0)
	if err = json.NewDecoder(strings.NewReader(b)).Decode(&results) ; err != nil {
		log.Errorf(ctx_default, "ERROR: Could not decode body into Entry type: " + string(body))
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Could not decode body into Entry type using Decode\"}")
	}
	for _, rr := range results {
		io.WriteString(w, fmt.Sprintf("_last_updt: %s \n", rr["_last_updt"] ) )
	}


	var entry Entry
	if err := json.Unmarshal([]byte(b), &entry); err != nil {
		log.Errorf(ctx_default, "ERROR: Could not decode body into Entry type: " + string(body))
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Could not decode body into Entry type using Unmarshal\"}")
	}
	fmt.Fprintf(w, "DEBUG: Entry.street:  " + entry.street + "\n")

	if err := json.NewDecoder(strings.NewReader(b)).Decode(&entry); err != nil {
		log.Errorf(ctx_default, "ERROR: Could not decode body into Entry type: " + string(body))
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Could not decode body into Entry type using Decode\"}")
	}
	fmt.Fprintf(w, "DEBUG: Entry.street:  " + entry.street + "\n")

	// switch context
	ctx, err := appengine.Namespace(ctx_default, "NorthAmerica")
	if err != nil {
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Can't switch to new Context\"}")
		return
	}

	parentkey, err := datastore.DecodeKey(datasetParentKey)
	if err != nil {
		log.Errorf(ctx, "ERROR: Could not decode Parent encoded key:" + string(body))
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Could not decode Parent encoded key: "+ datasetParentKey +"\"}")
		return
	}
	fmt.Fprintf(w, "{\"status\":\"0\", \"message\":\" parentkey: " + parentkey.String() +" \"}\n")
	fmt.Fprintf(w, "parentkey: " + parentkey.String() + ", parentkey.Namespace: " + parentkey.Namespace() + "\n")

	key := datastore.NewKey(ctx, "entry", "", 0, parentkey)
	fmt.Fprintf(w, "key: " + key.String() + ", key.Namespace: " + key.Namespace() + "\n")

	newkey, err := datastore.Put(ctx,
			key,
			entry)
	if err != nil {
		io.WriteString(w, "{\"status\":\"1\", \"message\":\"Can't execute operation datastore-Put\n\"}")
		log.Errorf(ctx,"ERROR: Can't execute operation Datastore PUT: " + err.Error() + "\n")
		return
	}
	log.Infof(ctx, "DEBUG: saved record Entry type with the key: " + newkey.String() + "\n")

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "{\"status\":\"0\", \"message\":\"ok\"}")
}

/*

func handlerCity(w http.ResponseWriter, r *http.Request) {

	ctx := appengine.NewContext(r)
	log.Infof(ctx, "Starting service.....")

	switch r.Method {

		case "POST":
			city, err := decodeCity(r.Body)
			if err != nil {
				log.Infof(ctx,"City error: %#v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				io.WriteString(w, "{"status":"error"}")
				return
			}
			city, err = city.save(ctx)
			if err != nil {
				log.Infof(ctx,"City error: %#v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				io.WriteString(w, "{"status":"error"}")
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "{"status":"ok"}")

		case "GET":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "GET Method called\n")
			getCity(ctx, w, "Chicago")

		default:
			log.Infof(ctx,"method not implemented")

	}

}

// decode
func decodeCity(r io.ReadCloser) (*City, error) {
	defer r.Close()
	var city City
	err := json.NewDecoder(r).Decode(&city)
	return &city, err
}
// Save
func (t *City) save(c context.Context) (*City, error) {
	log.Infof(c, "Storing entity of Kind(City): " + string(t.Name))
	_, err := datastore.Put(c, t.key(c), t)
	if err != nil {
		return nil, err
	}
//	t.Id = k.IntID()
	return t, nil
}

// key
func (city *City) key(c context.Context) *datastore.Key {
  // if there is no Id, we want to generate an "incomplete"
  // one and let datastore determine the key/Id for us
  if city.Name == "" {
    return datastore.NewIncompleteKey(c, "City", nil)
  }
  // if Id is already set, we'll just build the Key based
  // on the name provided.
  return datastore.NewKey(c, "City", city.Name, 0, nil)
}

type City struct {
	Desc    	string    	`datastore:"description,noindex"`
	Name    	string    	`datastore:"name"`
	State			string			`datastore:"state"`
	Type    	string    	`datastore:"type"`
	Timezone 	string			`datastore:"timezone,noindex"`
}

func defaultCityList(c context.Context) *datastore.Key {
	return datastore.NewKey(c, "City", "", 0, nil)
}

func getCity(c context.Context, w http.ResponseWriter, cityName string) ( string, error) {

//	SELECT * WHERE __key__ HAS ANCESTOR KEY(City, 'Chicago')
	var city City
	if err := datastore.Get(c, datastore.NewKey(c, "City", cityName, 0, nil), &city); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		io.WriteString(w, "Get failed to execute: " + err.Error() + "\n" )
		return "{\"status\":\"error\"}" ,err
	}
	io.WriteString(w, "Kind(City):"+ cityName +" Name: " + city.Name + "\n")

  return  nil, nil
}

*/

// AddTask adds a task with the given description to the datastore,
// returning the key of the newly created entity.
//func AddCity(ctx context.Context, client *datastore.Client, desc string, name string, timezone string) (*datastore.Key, error) {
//	city := &City{
//		Desc:    	desc,
//		Name:    	name,
//		Timezone: timezone,
//	}
//	//key := datastore.IncompleteKey("City", nil)
//	key := datastore.NameKey(
//	        "City", 		// Kind
//	        name, 			// String ID; empty means no string ID
//	        nil,        // Parent Key; nil means no parent
//	)
//	return client.Put(ctx, key, city)
//}

//func GetCity(ctx context.Context, id int64) (*City, error) {
//  var city City
//  city.Id = id
//
//  k := city.key(ctx)
//  if err := datastore.Get(ctx, k, &city); err != nil { return nil, err  }
//  city.Id = k.IntID()
//
//  return &city, nil
//}


func getENV(k string) string {
	v := os.Getenv(k)
	//if v == "" {
	//	log.Errorf(ctx,"Environment variable not set: "+ k)
	//}
	return v
}
