package main

import (
	"bytes"
	"encoding/binary"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)
//  XSend this json request to localhost:9000/wordcount endpoint using postman
//{
 //   "type": "request",
 //   "sentence": "Hello World India is the best India America America India food cat ball india " 
//}
//
//
//
//
//





var port string = ":9000"
var consul_host string = "http://34.122.227.175"
var consul_port string = ":80"
var consul_api_port string = ":8080"
var server_name string = "Server01"
var timeout = 6 * time.Second

var wg sync.WaitGroup


func healthCheck(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(http.StatusOK)

	res.Write([]byte("200"))

}

type request struct {
	Type     string `json:"type"`
	Sentence string `json:"sentence"`
}

type assign struct {
	Type    string            `json:"type"`
	Servers map[string]string `json:"serverlist"`
	Array   map[int]string    `json:array`
}

type data struct {
	WordCounter map[string]int `json:"wordcounter"`
	Max         string         `json:"max"`
}
type leader struct {
	Session string `json:"session"`
	Leader  string `json:"server"`
}

func wordCount(str string) data {
	wordList := strings.Fields(str)
	counts := make(map[string]int)
	for _, word := range wordList {
		_, ok := counts[word]
		if ok {
			counts[word]++
		} else {
			counts[word] = 1
		}
	}
	maxCounter := 0
	var maxWord string
	for key, value := range counts {
		if value >= maxCounter {
			maxCounter = value
			maxWord = key
		}

	}
	return data{counts, maxWord}
}

func consul() {

	defer wg.Done()
	var isLeader bool
	client, err := api.NewClient(&api.Config{
		Address: consul_host + consul_port,
		Scheme:  "http",
	})

	if err != nil {
		panic(err)
	}

	err = client.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Address: consul_host + consul_api_port,
		ID:      server_name, // Unique for each node
		Name:    server_name, // Can be service type
		Tags:    []string{"monitoring"},
		Check: &api.AgentServiceCheck{
			HTTP:     consul_host + consul_api_port + "/_health",
			Interval: "10s",
		},
	})

	if err != nil {
		panic(err)
	}

	sessionID, _, err := client.Session().Create(&api.SessionEntry{
		Name:     "leader", // distributed lock
		Behavior: "release",
		TTL:      "100s",
	}, nil)

	if err != nil {
		panic(err)
	}

	isLeader, _, err = client.KV().Acquire(&api.KVPair{
		Key:     "leader", // distributed lock
		Value:   []byte(server_name),
		Session: sessionID,
	}, nil)

	if err != nil {
		panic(err)
	}

	for isLeader == false {
		fmt.Println("Trying to aquire lock")
		time.Sleep(timeout)
		isLeader, _, err = client.KV().Acquire(&api.KVPair{
			Key:     "leader", // distributed lock
			Value:   []byte(server_name),
			Session: sessionID,
		}, nil)

		if err != nil {
			fmt.Println(err)
			fmt.Println("Lock Hangup")
		}

		if isLeader == true {
			fmt.Println("Lock aquired by " + server_name)
			fmt.Println(sessionID)
			break
		}

	}
	for isLeader == true {
		fmt.Println(server_name + " is leader")
		time.Sleep(timeout)
		isRelease, _, err := client.KV().Release(&api.KVPair{
			Key:     "leader", // distributed lock
			Value:   []byte(server_name),
			Session: sessionID,
		}, nil)
		if err != nil {
			panic(err)
			fmt.Println(isRelease)
		}
		fmt.Println(server_name + " Released the lock")
		time.Sleep(timeout)
		consul()
	}

}
func sendRequest(req request) []byte {
	serverlist, n := ListServers()

	req.Type = "response"
	wordList := strings.Fields(req.Sentence)
	var counter int = 0
	for err, _ := range wordList {
		counter++
		if counter < 0 {
			fmt.Println(err)
		}
	}
	str := wordList
	str_size := counter
	var str_counter int = 0
	data_arr := make(map[int]string)

	part_size := str_size / n
	if part_size == 0 {
		return []byte("Cannot make more parts. Line 192")
	}
	if part_size == 1 {
		part_size = part_size + 1
	}
	var word_group string = ""
	for i := 0; i < str_size; i++ {
		if i%part_size == 0 {
			data_arr[str_counter] = word_group
			str_counter = str_counter + 1
			word_group = ""
		}
		word_group = word_group + " " + str[i]

	}
	data_arr[str_counter] = word_group // for the last part to be added to the array
	leader := findleader()
	for n, k := range serverlist {
		if n == leader {
			var reqleader assign
			reqleader.Servers = serverlist
			reqleader.Type = "response"
			reqleader.Array = data_arr

			b, err := json.Marshal(reqleader)
			if err != nil {
				fmt.Println("error in creating leader payload!")
			}

			client := &http.Client{}

			re, err := http.NewRequest("POST", "http://"+k+"/leader", bytes.NewBuffer(b))
			resp, err := client.Do(re)
			
			if err != nil {

				fmt.Println("error hititng the leader", re)
				return []byte("404 Error")
			}
			body, _ := ioutil.ReadAll(resp.Body)

			return body

		}

	}
	return []byte("Error Sending request. Line 241")

}
func findleader() string {

	client, err := api.NewClient(&api.Config{
		Address: consul_host + consul_port,
		Scheme:  "http",
	})

	if err != nil {
		panic(err)
	}

	// Get a handle to the KV API
	kv := client.KV()
	pair, _, err := kv.Get("leader", nil)
	if err != nil {
		panic(err)
	}
	if pair == nil {
		fmt.Println("no ip got")
	}
	value := string(pair.Value)
	return value

}

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair


func (p PairList) Len() int           { return len(p) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }


func parseRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	var newRequest request
	err = json.Unmarshal(body, &newRequest)
	if err != nil {
		panic(err)
	}
	if newRequest.Type == "response" {

		response := wordCount(newRequest.Sentence)
		responseBytes, _ := json.Marshal(response)
		w.Write(responseBytes)

	} else if newRequest.Type == "request" {
		finalresp := sendRequest(newRequest)
	//	fmt.Println("+++++++++++++++++++++")
	//	fmt.Println(string(finalresp))
		w.Write(finalresp)
	} else {
		w.Write([]byte("400 Bad Request. Please choose between response or request"))
	}
}

func parseLeaderRequest(w http.ResponseWriter, r *http.Request) {
	//new_wordcount2 := make(map[string][]byte)
	w.Header().Set("Content-Type", "application/json")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	var newResp assign
	err = json.Unmarshal(body, &newResp)
	if err != nil {
		panic(err)
	}
	serverlstt := newResp.Servers
	data_arr := newResp.Array
	FinalMap := make(map[string]int)
	//var finalResponse data
	if newResp.Type == "response" {
		dataCounter := 1
		for sname, svalue := range serverlstt {
			response := makeRequest(svalue, data_arr,dataCounter)
			dataCounter++
			fmt.Println(sname+" Processed "+strconv.Itoa(binary.Size(response))+" chunkes of bytes!")
			var newResponse data
			json.Unmarshal(response,&newResponse)
			p := make(PairList, len(newResponse.WordCounter))
			i := 0
	for k, v := range newResponse.WordCounter {
		p[i] = Pair{k, v}
		i++
	}

	
	sort.Sort(p)
	//p is sorted
	for _, k := range p {
		val,ok := FinalMap[k.Key]
		 if !ok{
			 FinalMap[k.Key] = k.Value
		 }else {
			 
			 new_value := val+k.Value
			 FinalMap[k.Key] = new_value

		}
       
    }
	}
	} else {
		w.Write([]byte("400 Bad Request. Please choose between response or request"))
	}
	FinalMap2 := rankByWordCount(FinalMap)
	//Maxmap := make(map[string]int)
	//min_counter := 5
	fmt.Println(FinalMap2)
	TempData, err := json.Marshal(FinalMap2)   
    if err != nil {
        fmt.Println(err.Error())
        return
    }
	
	if err != nil {
		fmt.Println("error occured")
	}
	w.Write(TempData)
}

func rankByWordCount(wordFrequencies map[string]int) PairList{
	pl := make(PairList, len(wordFrequencies))
	i := 0
	for k, v := range wordFrequencies {
	  pl[i] = Pair{k, v}
	  i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
  }
  


 
func makeRequest(severaddress string, array map[int]string,counter int)[]byte {

	var newRequest2 request
	newRequest2.Sentence = array[counter]
	newRequest2.Type = "response"
	client := &http.Client{}

	b, err := json.Marshal(newRequest2)
	if err != nil {
		fmt.Println("error in creating leader payload!")
	}
	re, _ := http.NewRequest("POST", "http://"+severaddress+"/wordcount", bytes.NewBuffer(b))
	resp, err := client.Do(re)

	if err != nil {

		fmt.Println("error hititng the leader. Line number 359", re)


	}
	body, _ := ioutil.ReadAll(resp.Body)
	
	return body

}


func indexPage(res http.ResponseWriter, req *http.Request) {
	fmt.Fprint(res, "Hello: "+req.Host)

}

func setupRoutes() {
	defer wg.Done()
	http.HandleFunc("/leader", parseLeaderRequest)
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/wordcount", parseRequest)
	http.ListenAndServe(port, nil)
}

func RegisterService() {
	// Get a new client
	client, err := api.NewClient(&api.Config{
		Address: consul_host + consul_port,
		Scheme:  "http",
	})

	if err != nil {
		panic(err)
	}

	// Get a handle to the KV API
	kv := client.KV()

	// Lookup the pair
	pair, _, err := kv.Get("Servers/"+server_name, nil)

	if err != nil {
		panic(err)
	}
	if pair == nil {
		// PUT a new KV pair
		p := &api.KVPair{Key: "Servers/" + server_name, Value: []byte(getip())}
		_, err = kv.Put(p, nil)
		if err != nil {
			panic(err)
		}
	} else {
		p := &api.KVPair{Key: "Servers/" + server_name, Value: []byte(getip())}
		_, err = kv.Put(p, nil)
		if err != nil {
			panic(err)
		}
	}
}

func ListServers() (map[string]string, int) {
	client, err := api.NewClient(&api.Config{
		Address: consul_host + consul_port,
		Scheme:  "http",
	})

	if err != nil {
		panic(err)
	}

	// Get a handle to the KV API
	kv := client.KV()

	servers, _, err := kv.Keys("Servers/", "/", nil)
	if err != nil {
		panic(err)
	}
	serverlst := make(map[string]string)
	count := 0
	for server := range servers {
		str := servers[server]
		pair, _, err := kv.Get(str, nil)
		if err != nil {
			panic(err)
		}
		if pair == nil {
			fmt.Println("no ip got")
		}
		value := string(pair.Value)
		key := strings.Split(servers[server], "/")[1]
		serverlst[key] = value
		count++

	}
	return serverlst, count
}

func healthyServers() {
	defer wg.Done()
	client, err := api.NewClient(&api.Config{
		Address: consul_host + consul_port,
		Scheme:  "http",
	})

	if err != nil {
		panic(err)
	}

	// Get a handle to the KV API
	kv := client.KV()
	for {

		servers, _, err := kv.Keys("Servers/", "/", nil)
		if err != nil {
			panic(err)
		}
		for server := range servers {
			str := servers[server]
			pair, _, err := kv.Get(str, nil)
			if err != nil {
				panic(err)
			}
			if pair == nil {
				fmt.Println("no ip got")
			}
			value := string(pair.Value)
			_, err = http.Get("http://" + value + "/health")
			if err != nil {
				fmt.Println("Server Down! Removing 1 server")
				_, err = kv.Delete(servers[server], nil)
				if err != nil {
					fmt.Println("url removed")
				}

			}
		}
		time.Sleep(2 * time.Second)

	}

}

func getip() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		fmt.Println("error")
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	ip := localAddr[0:idx]
	if strings.HasPrefix(ip, "172") || strings.HasPrefix(ip, "10") || strings.HasPrefix(ip, "192") || strings.HasPrefix(ip, "169") || strings.HasPrefix(ip, "224") || strings.HasPrefix(ip, "127") {
		ip = "127.0.0.1"
		return ip + port
	}

	return ip + port

}

func main() {
	RegisterService()
	// add two goroutines to `wg` WaitGroup
	wg.Add(3)
	go consul()
	go setupRoutes()
	go healthyServers()

	// wait until WaitGroup is done
	wg.Wait()

}
