package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"

	elastic "gopkg.in/olivere/elastic.v3"
)

type Tweet struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(elasticsearchDemo()))
	})
	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), nil))
}

func elasticsearchDemo() string {
	var output string

	var options []elastic.ClientOptionFunc
	esUrls := os.Getenv("ES_URLS")
	if esUrls != "" {
		urls := strings.Split(esUrls, ",")
		options = append(options, elastic.SetURL(urls...))
	}
	esBasicAuth := os.Getenv("ES_BASIC_AUTH")
	if esBasicAuth != "" {
		basicAuth := strings.Split(esBasicAuth, ":")
		options = append(options, elastic.SetBasicAuth(basicAuth[0], basicAuth[1]))
	}
	options = append(options, elastic.SetSniff(false))

	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	// Create a client
	client, err := elastic.NewClient(options...)
	if err != nil {
		// Handle error
		panic(err)
	}

	// Create an index
	_, err = client.CreateIndex("twitter").Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// Add a document to the index
	tweet := Tweet{User: "olivere", Message: "Take Five"}
	_, err = client.Index().
		Index("twitter").
		Type("tweet").
		Id("1").
		BodyJson(tweet).
		Refresh(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// Search with a term query
	termQuery := elastic.NewTermQuery("user", "olivere")
	searchResult, err := client.Search().
		Index("twitter").   // search in index "twitter"
		Query(termQuery).   // specify the query
		Sort("user", true). // sort by "user" field, ascending
		From(0).Size(10).   // take documents 0-9
		Pretty(true).       // pretty print request and response JSON
		Do()                // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	output += fmt.Sprintf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ttyp Tweet
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		if t, ok := item.(Tweet); ok {
			output += fmt.Sprintf("Tweet by %s: %s\n", t.User, t.Message)
		}
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	output += fmt.Sprintf("Found a total of %d tweets\n", searchResult.TotalHits())

	// Here's how you iterate through results with full control over each step.
	if searchResult.Hits.TotalHits > 0 {
		output += fmt.Sprintf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)

		// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Tweet
			err := json.Unmarshal(*hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}

			// Work with tweet
			output += fmt.Sprintf("Tweet by %s: %s\n", t.User, t.Message)
		}
	} else {
		// No hits
		output += fmt.Sprintf("Found no tweets\n")
	}

	// Delete the index again
	_, err = client.DeleteIndex("twitter").Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	return output
}
