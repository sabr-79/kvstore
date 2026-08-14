package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

func TestKVStoreRobustRace(t *testing.T) {
	store := KVstore{}
	mux := http.NewServeMux()
	mux.HandleFunc("/put", putHandler(&store))
	mux.HandleFunc("/get", getHandler(&store))

	server := httptest.NewServer(mux)
	defer server.Close()

	contestedKey := "highly_contested_key"

	validPayloads := make(map[string]bool)
	var mapMu sync.Mutex
	var wg sync.WaitGroup
	concurrencyLimit := 100

	for i := 0; i < concurrencyLimit; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			uniqueValue := "PAYLOAD_VAL_" + strconv.Itoa(id) + "_"
			for len(uniqueValue) < 200 {
				uniqueValue += "A"
			}

			mapMu.Lock()
			validPayloads[uniqueValue] = true
			mapMu.Unlock()

			url := server.URL + "/put?key=" + contestedKey + "&val=" + uniqueValue
			resp, err := http.Post(url, "application/json", nil)
			if err != nil {
				t.Errorf("Concurrent PUT request failed: %v", err)
				return
			}
			resp.Body.Close()
		}(i)
	}

	wg.Wait()

	getURL := server.URL + "/get?key=" + contestedKey
	resp, err := http.Get(getURL)
	if err != nil {
		t.Fatalf("Failed to fetch final key value: %v", err)
	}
	defer resp.Body.Close()

	var responseData struct {
		Value string `json:"value"`
	}
	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {
		t.Fatalf("Failed to parse JSON response body: %v", err)
	}

	finalValue := responseData.Value
	if finalValue == "" {
		t.Errorf("Assertion Failed: The final stored value was completely empty")
	}

	if !validPayloads[finalValue] {
		t.Errorf("CORRUPTION DETECTED! The stored string was torn or corrupted.\nGot unexpected data string length (%d)", len(finalValue))
	} else {
		t.Logf("Success! Lock integrity held. Safe data victory for payload.")
	}
}
