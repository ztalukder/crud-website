//Author: Zami Talukder
package main

import (
    "fmt"
    "html/template"
    "strings"
    "github.com/kataras/iris"
    "github.com/kataras/iris/middleware/logger"
    "github.com/kataras/iris/middleware/recover"
    "os"
    "net"
    "encoding/json"
    "time"
    "strconv"
)

type Hero struct {
    Name string
    SuperheroName string
    From string
    Power string
}

type UpdateForm struct {
    Index string
    Hero Hero
}

//Used to pass into home page
type HomeTemplate struct{
    Data map[string]Hero
}


type DeleteForm struct {
    Key string
}

var receiver *json.Decoder
var sender *json.Encoder
var connection net.Conn
var receivedBeat time.Time
var backHostPort string
var heartbeatPort string
var backendPorts []string

func main() {
    app := iris.New()
    app.Logger().SetLevel("debug")

    app.Use(recover.New())
    app.Use(logger.New())

    app.Get("/", home)
    app.Get("/insertform", insertform)
    app.Get("/update/{key:string}", update)
    app.Post("/delete", deleteitem)
    app.Post("/", home)
    app.Post("/update/{key:string}", update)

    port := "8080"
    backHostPort = ":8090"
    backend := ""
    heartbeatPort = ":8091"
    for i:= 0; i < len(os.Args); i++{
        if os.Args[i] == "--listen" && i+1 < len(os.Args){
            port = os.Args[i+1]
        }
        if os.Args[i] == "--backend" && i+1 < len(os.Args){
            backend = os.Args[i+1]
        }
    }
    backendPorts = strings.Split(backend, ",")


    

    acceptConnections()


    app.Run(iris.Addr(":" + port))
  
}

func acceptConnections(){
    foundBackend := false
    for !foundBackend{
        for i:=0; i < len(backendPorts); i++{
            findLeaderConn, err := net.Dial("tcp", backendPorts[i])
            if err != nil{
                continue
            }
            findLeaderReceiver := json.NewDecoder(findLeaderConn)
            findLeaderSender := json.NewEncoder(findLeaderConn)
            findLeaderSender.Encode("LEADER?")
            
            var response string
            findLeaderReceiver.Decode(&response)
            if response == "YES"{
                connection = findLeaderConn
                receiver = findLeaderReceiver
                sender = findLeaderSender
                foundBackend = true
                backHostPort = backendPorts[i]
                break
            }else {
                findLeaderConn.Close()
            }
        }
    }
    fmt.Println("Backend Found")

    intPort, err := strconv.Atoi(backHostPort[strings.Index(backHostPort,":")+1:])
    if err != nil{
        fmt.Println("invalid port")
    }
    heartbeatPort = ":" + strconv.Itoa(intPort+1)

    myHeart, err := net.Dial("tcp", heartbeatPort)
    if err != nil{
        fmt.Println("database heartbeat failed to establish connection")
        return
    }
    go acceptHeart(&myHeart)
    receivedBeat = time.Now()
    go runTimer(backHostPort)
    
}


func acceptHeart(heart *net.Conn){
    acceptBeat := json.NewDecoder(*heart)
    for {
        var beat string
        err := acceptBeat.Decode(&beat)
        if err != nil{
            break
        }
        if beat == "alive"{
            receivedBeat = time.Now()
        } else{
            break
        }
    }
    (*heart).Close()
}

func runTimer(port string){
    for {
        if time.Now().Sub(receivedBeat)/time.Second > 6{
            fmt.Println("Backend Failed at", port, "at", time.Now())
            go acceptConnections()
            break
        }
    }

}


//"/" handler
func home(ctx iris.Context){
    //post request
    if (strings.Contains(ctx.GetCurrentRoute().Method(), "POST")){
        insertHero := Hero{}
        ctx.ReadForm(&insertHero)
        addData(insertHero)
    }
    template, err := template.ParseFiles("home.html")
    if err != nil{
        fmt.Println(err)
    }
    allData := getAllData()
    template.Execute(ctx, HomeTemplate{
            Data: allData,
            })
}


//"/insert" handler
func insertform(ctx iris.Context){
    template, _:= template.ParseFiles("insertform.html")
    template.Execute(ctx, "none")
}


//"/delete" handler
func deleteitem(ctx iris.Context){
    deleteForm := DeleteForm{}
    ctx.ReadForm(&deleteForm)
    deleteData(deleteForm.Key)
    ctx.Redirect("/")
}

//"/update/{name}" handler
func update(ctx iris.Context){
    if (strings.Contains(ctx.GetCurrentRoute().Method(), "POST")){
        updateForm := Hero{}
        ctx.ReadForm(&updateForm)
        key := ctx.Params().Get("key")
        updateData(key, updateForm)
        ctx.Redirect("/")
    }else{
        template, err := template.ParseFiles("updateform.html")
        if err != nil{
            fmt.Println(err)
        }
        key := ctx.Params().Get("key")
        myHero := getSingleData(key)
        template.Execute(ctx, UpdateForm{Hero: myHero, Index: key})
    }
}

//Get a single hero with specified id from backend
func getSingleData(index string) Hero {
    sender.Encode("GET")
    sender.Encode(index)
    var hero Hero
    receiver.Decode(&hero)
    return hero
}

//get the entire database from backend
func getAllData() map[string]Hero {
    sender.Encode("GET ALL")
    var heroes map[string]Hero
    receiver.Decode(&heroes)
    return heroes
}

//Adds new hero into database
func addData(hero Hero) {
    sender.Encode("ADD")
    sender.Encode(hero)
}

//Deletes list of ids from database
func deleteData(indices string) {
    sender.Encode("DELETE")
    sender.Encode(indices)
}

//Sends update message to server
//index is id
//hero is the updated version
func updateData(index string, hero Hero){
    sender.Encode("UPDATE")
    sender.Encode(index)
    sender.Encode(hero)
}