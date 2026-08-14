package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
)

type KVstore struct {
	//Key   int
	//Value int
	mu   sync.RWMutex
	dict map[string]string
}

func get(store *KVstore, key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}
	val, ok := store.dict[key]
	if ok {
		return val, ok
	}
	return "", ok

}
func getHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		val, valid := get(store, key)

		if !valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message string `json:"message"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		}{
			Message: "Obtained key",
			Key:     key,
			Value:   val,
		})

	}

}
func put(store *KVstore, key string, val string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}
	store.dict[key] = val
}
func putHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")

		if key == "" || val == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		put(store, key, val)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message string `json:"message"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		}{
			Message: "Stored key",
			Key:     key,
			Value:   val,
		})

	}

}

func remove(store *KVstore, key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}

	_, ok := store.dict[key]
	if ok {
		delete(store.dict, key)

	}
	return ok

}

func removeHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		ok := remove(store, key)
		if ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(struct {
				Message string `json:"message"`
				Key     string `json:"key"`
			}{
				Message: "Removed key",
				Key:     key,
			})

		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			return

		}

	}

}
func Scan(store *KVstore, startKey string, endKey string) map[string]string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	keys := []string{}
	for k := range store.dict {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	res := map[string]string{}

	for _, x := range keys {
		if x >= startKey && x <= endKey {
			res[x] = store.dict[x]

		}

	}
	return res

}
func scanHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		skey := r.URL.Query().Get("startKey")
		ekey := r.URL.Query().Get("endKey")

		if skey == "" || ekey == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		res := Scan(store, skey, ekey)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message  string            `json:"message"`
			StartKey string            `json:"startkey"`
			EndKey   string            `json:"endkey"`
			Result   map[string]string `json:"result"`
		}{
			Message:  "Scanned keys",
			StartKey: skey,
			EndKey:   ekey,
			Result:   res,
		})

	}

}

func main() {
	mux := http.NewServeMux()
	myStore := KVstore{}
	put(&myStore, "7", "9")
	x, _ := get(&myStore, "7")
	y, _ := get(&myStore, "1")
	z := remove(&myStore, "9")
	w := Scan(&myStore, "0", "7")
	fmt.Println(w)
	fmt.Println(x)
	fmt.Println(y)
	fmt.Println(z)
	mux.HandleFunc("/get", getHandler(&myStore))
	mux.HandleFunc("/put", putHandler(&myStore))
	mux.HandleFunc("/remove", removeHandler(&myStore))
	mux.HandleFunc("/scan", scanHandler(&myStore))
	fmt.Println("Server starting on :8080...")
	http.ListenAndServe(":8080", mux)

}
