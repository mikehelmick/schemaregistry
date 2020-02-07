package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"

	"github.com/gorilla/mux"
)

var koPath string
var gcpProjectID string

func init() {
	koPath = os.Getenv("KO_DATA_PATH")
	if koPath == "" {
		koPath = "./kodata"
	}
	gcpProjectID = os.Getenv("GCP_PROJECT")
	if gcpProjectID == "" {
		log.Fatalf("Missing GCP_PROJECT environment variable")
		panic("Missing GCP_PROJECT environment variable")
	}
}

// Publish is the JSON API for publishing a schema
type Publish struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	Schema string `json:"schema"`
}

// EventSchema is the type that is written to datastore
type EventSchema struct {
	CreatedAt time.Time      `datastore:"createdat"`
	Type      string         `datastore:"cetype"`
	Source    string         `datastore:"cesource"`
	Schema    string         `datastore:"schema,noindex"`
	Public    bool           `datastore:"public"`
	K         *datastore.Key `datastore:"__key__"`
}

func getDatastore() (context.Context, *datastore.Client, error) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, gcpProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return ctx, client, err
}

func renderTemplate(w http.ResponseWriter, tmpl string, p *map[string]interface{}) {
	file := fmt.Sprintf("%s/templates/%s.html", koPath, tmpl)
	log.Println(file)
	t, _ := template.ParseFiles(file)
	if err := t.Execute(w, p); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error rendering html %v", err)
	}
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	msg := Publish{}
	err := decoder.Decode(&msg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unable to parse message"))
		return
	}

	ctx, client, err := getDatastore()
	if err == nil {
		defer client.Close()

		key := datastore.IncompleteKey("schema", nil)
		entity := new(EventSchema)
		entity.CreatedAt = time.Now()
		entity.Type = msg.Type
		entity.Source = msg.Source
		entity.Schema = msg.Schema

		if _, err := client.Put(ctx, key, entity); err != nil {
			log.Fatalf("Unable to write to datastore: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to connect to database"))
		}
		log.Printf("Accepted schema for type %v", msg.Type)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Unable to connect to database"))
	}
}

func removeDotJson(ceType string) string {
	if idx := strings.LastIndex(ceType, ".json"); idx > 0 {
		ceType = ceType[0:idx]
	}
	return ceType
}

func getSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ceType := removeDotJson(vars["type"])
	m := map[string]interface{}{}

	ctx, client, err := getDatastore()
	if err == nil {
		defer client.Close()

		q := datastore.NewQuery("schema").Filter("cetype = ", ceType).Limit(1)
		var schemas []EventSchema
		if _, err := client.GetAll(ctx, q, &schemas); err == nil {
			if len(schemas) == 0 {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(fmt.Sprintf("Schema not found for type: %s", ceType)))
				return
			}
			m["schema"] = schemas[0]
			renderTemplate(w, "get", &m)
		} else {
			log.Fatalf("Query error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		log.Fatalf("Failed to connect to datastore: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal error."))
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	m := map[string]interface{}{}

	ctx, client, err := getDatastore()
	if err == nil {
		defer client.Close()

		q := datastore.NewQuery("schema").Order("cetype")
		var schemas []EventSchema
		if _, err := client.GetAll(ctx, q, &schemas); err == nil {
			pubSchemas := make([]EventSchema, 0)
			for _, s := range schemas {
				if s.Public {
					pubSchemas = append(pubSchemas, s)
				}
			}
			m["schemas"] = pubSchemas
		} else {
			log.Fatalf("Query error: %v", err)
		}
		renderTemplate(w, "index", &m)
	} else {
		log.Fatalf("Failed to connect to datastore: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal error."))
	}
}

func downloadSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ceType := removeDotJson(vars["type"])

	ctx, client, err := getDatastore()
	if err == nil {
		defer client.Close()

		q := datastore.NewQuery("schema").Filter("cetype = ", ceType).Limit(1)
		var schemas []EventSchema
		if _, err := client.GetAll(ctx, q, &schemas); err == nil {
			if len(schemas) == 0 {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(fmt.Sprintf("Schema not found for type: %s", ceType)))
				return
			}

			w.Header().Add("Content-Type", "application/json")
			w.Write([]byte(schemas[0].Schema))

		} else {
			log.Fatalf("Query error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		log.Fatalf("Failed to connect to datastore: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal error."))
	}
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/publish", publishHandler).Methods("POST")
	router.HandleFunc("/download/{type}", downloadSchemaHandler).Methods("GET")
	router.HandleFunc("/schema/{type}", getSchemaHandler).Methods("GET")
	router.HandleFunc("/", indexHandler).Methods("GET")

	http.ListenAndServe(":8080", router)
}
