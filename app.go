package main

import (
	"github.com/drone/routes"
	"log"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"os"
	"strconv"
	"github.com/naoina/toml"
	"fmt"
	"github.com/mkilling/goejdb"
    "labix.org/v2/mgo/bson"
    "net"
    "net/rpc"

)

type Data struct {
  Email string `json:"email"`
  Zip   string `json:"zip"`
  Country string `json:"country"`
  Profession string `json:"profession"`
  Favorite_color string `json:"favorite_color"`
  Is_smoking string `json:"is_smoking"`
  Favorite_sport string `json:"favorite_sport"`
	Food FoodData `json:"food"`
	Music MusicData `json:"music"`
	Movie MovieData `json:"movie"`
	Travel TravelData `json:"travel"`
}

var Profile []Data

var config TomlConfig

type Listener int

type FoodData struct {
	Type string `json:"type"`
	Drink_alcohol string `json:"drink_alcohol"`
}

type MusicData struct {
	Spotify_user_id string `json:"spotify_user_id"`
}

type MovieData struct {
	Tv_shows []string `json:"tv_shows"`
	Movies []string `json:"movies"`
}

type TravelData struct {
	Flight FlightData `json:"flight"`
}

type FlightData struct {
	Seat string `json:"seat"`
}

type TomlConfig struct{

	Database struct{
		File_name string
		Port_num int
	}

	Replication struct{
		Rpc_server_port_num int
		Replica []string
	}
}

func main() {

	toml_config := os.Args[1]

	toml_file_name, err := os.Open(toml_config) // For read access.
	if err != nil {
	log.Fatal(err)
	}

	defer toml_file_name.Close()
	buf, err := ioutil.ReadAll(toml_file_name)
	if err != nil {
		panic(err)
	}


	if err := toml.Unmarshal(buf, &config); err != nil {
		panic(err)
	}

	fmt.Println(config.Database.Port_num)

	go replicationServer()

	mux := routes.New()

	mux.Get("/profile/:email", GetProfile)
	mux.Post("/profile", PostProfile)
	mux.Del("/profile/:email", DelProfile)
	mux.Put("/profile/:email", PutProfile)


	http.Handle("/", mux)
	log.Println("Listening...")
	http.ListenAndServe(":" + strconv.Itoa(config.Database.Port_num), nil)
}


func GetProfile(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	email := params.Get(":email")


	// Create a new database file and open it
    jb, err := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err != nil {
        os.Exit(1)
    }
    defer jb.Close()
    // Get or create collection 'contacts'
    coll, _ := jb.CreateColl("profile", nil)

	res, _ := coll.Find(fmt.Sprintf(`{"email" : %q}`, email))

	fmt.Println(res)

	if(len(res) <= 0){
    	w.WriteHeader(404)
		return
    }

    w.Write([]byte(`[`))

    for _, bsData := range res {
        var m map[string]interface{}
        bson.Unmarshal(bsData, &m)
        fmt.Println(m)
        getData, _ := json.Marshal(m)
        w.Write([]byte(getData))
    }

    w.Write([]byte(`]`))


	w.WriteHeader(200)

}

func PostProfile(w http.ResponseWriter, r *http.Request) {
		var saveData Data
		inputJson, err := ioutil.ReadAll(r.Body)
		err = json.Unmarshal(inputJson, &saveData)
		if err != nil{
			http.Error(w, "Unmarshalling Error", http.StatusBadRequest)
			return
		}

		// Create a new database file and open it
    	jb, _ := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT)

    	defer jb.Close()
    	// Get or create collection 'contacts'
    	coll, _ := jb.CreateColl("profile", nil)

    	res, _ := coll.Find(fmt.Sprintf(`{"email" : %q }`, saveData.Email))

    	if(len(res) > 0){
    		w.WriteHeader(422)
			return
    	}

		bsData, _ := bson.Marshal(saveData)
		for _,replicationServerAddress := range config.Replication.Replica{
			go replicationClient(replicationServerAddress, bsData, "POST","")
		}
    	coll.SaveBson(bsData)

		w.WriteHeader(http.StatusCreated)
}

func DelProfile(w http.ResponseWriter, r *http.Request) {
    params := r.URL.Query()
    email := params.Get(":email")

    // Create a new database file and open it
    	jb, _ := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT )

    	defer jb.Close()
    	// Get or create collection 'contacts'
    	coll, _ := jb.CreateColl("profile", nil)

    	coll.Update(fmt.Sprintf(`{"email" : %q , "$dropall" : true}`, email))

    	for _,replicationServerAddress := range config.Replication.Replica{
			go replicationClient(replicationServerAddress, []byte{0}, "DELETE",email)
		}

		w.WriteHeader(204)

}

func PutProfile(w http.ResponseWriter, r *http.Request) {
    params := r.URL.Query()
    email := params.Get(":email")

    jb, _ := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT)

    	defer jb.Close()
    	// Get or create collection 'contacts'
    	coll, _ := jb.CreateColl("profile", nil)


		var t map[string]interface{}
		receivedData, err := ioutil.ReadAll(r.Body)
		err = json.Unmarshal(receivedData, &t)

		if err != nil {
			http.Error(w,"Error Unmarshalling",http.StatusBadRequest)
			return
		}

	res, _ := coll.Find(fmt.Sprintf(`{"email" : %q}`, email))

	fmt.Println(res)

	if(len(res) <= 0){
    	w.WriteHeader(404)
		return
    }

    coll.Update(fmt.Sprintf(`{"email" : %q , "$dropall" : true}`, email))



		var map_profile map[string]interface{}

		for _,bs :=range res{
			bson.Unmarshal(bs,&map_profile)
		}


		map_profile = UpdateData(t,map_profile)

		bsData, _ := bson.Marshal(map_profile)
	for _,replicationServerAddress := range config.Replication.Replica{
			go replicationClient(replicationServerAddress, bsData, "PUT",email)
		}

		coll.SaveBson(bsData)
		w.WriteHeader(204)

}


func (l *Listener) DeleteData(email string, ack *bool) error {
        jb, _ := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT )

    defer jb.Close()


    // Get or create collection 'contacts'
    coll, _ := jb.CreateColl("profile", nil)
    coll.Update(fmt.Sprintf(`{"email" : %q, "$dropall" : true}`, email))


        return nil
}

func UpdateData(data map[string]interface{}, profile map[string]interface{}) map[string]interface{}{
		for i := range data {
			profile[i] = data[i]
		}
		return profile
}



func (l *Listener) PersistData(postData []byte, ack *bool) error {
        fmt.Println(string(postData))
        jb, _ := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER | goejdb.JBOCREAT )

    defer jb.Close()

    // Get or create collection 'contacts'
    coll, _ := jb.CreateColl("profile", nil)
    coll.SaveBson(postData)

        return nil
}

func replicationClient(replica string,replicationData []byte, callType string, email string) {
        replica = replica[7:]

        client, err := rpc.Dial("tcp", replica)
        if err != nil {
                log.Fatal(err)
        }

                var reply bool
                if (callType=="POST"){
                        err = client.Call("Listener.PersistData", replicationData, &reply)
                }else if (callType=="PUT"){
                        err = client.Call("Listener.DeleteData", email, &reply)
                        err = client.Call("Listener.PersistData", replicationData, &reply)
                }else{
                        err = client.Call("Listener.DeleteData", email, &reply)
                }
                if err != nil {
                        log.Fatal(err)
                }
}




func replicationServer() {
        addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+strconv.Itoa(config.Replication.Rpc_server_port_num))
        if err != nil {
                log.Fatal(err)
        }

        inbound, err := net.ListenTCP("tcp", addy)
        if err != nil {
                log.Fatal(err)
        }

        listener := new(Listener)
        rpc.Register(listener)
        rpc.Accept(inbound)
}
