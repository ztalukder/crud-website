package main

import (
    "fmt"
    "encoding/json"
    "net"
    "os"
    "sync"
    "crypto/sha256"
    "encoding/base64"
    "time"
    "strings"
    "strconv"
    "math/rand"
)

type Hero struct {
    Name string
    SuperheroName string
    From string
    Power string
}

type BackendReplica struct {
    Connection net.Conn
    Sender *json.Encoder
    Receiver *json.Decoder
    Port string
}

var info map[string]Hero
var lock sync.RWMutex
var backendPortList chan string
var myPort string
var backendListenPort string
var heartbeatPort string

var backendConnections []BackendReplica
var backendConnectionsLock sync.Mutex
var backendConnectionsTalk []BackendReplica
var backendConnectionsTalkLock sync.Mutex

var totalNodes int
var myState int //1=follower, 2= candidate, 3= leader
var term int
var messageReceived time.Time
var messageReceivedLock sync.RWMutex

var commitedLog []interface{}
var uncommitedLog []interface{}
var uncommitedLogLock sync.Mutex



func main(){
    a := Hero{Name:"Clark Kent", SuperheroName:"Superman", From:"DC", Power:"Strength"}
    b := Hero{Name:"Bruce Wayne", SuperheroName:"Batman", From:"DC", Power:"Technology"}
    c := Hero{Name:"Toshinori Yagi", SuperheroName:"All Might", From:"My Hero Academia", Power:"One for All"}
    d := Hero{Name:"Yugi", SuperheroName:"Yu-Gi-Oh", From:"Yu-Gi-Oh", Power:"Cards"}


    info = make(map[string]Hero)
    info[getID(a)] = a
    info[getID(b)] = b
    info[getID(c)] = c
    info[getID(d)] = d

    myState = 1
    term = 0
    messageReceived = time.Now()

    //listen to specific port
    myPort = "8090"
    backend := ""
    for i:= 0; i < len(os.Args); i++{
        if os.Args[i] == "--listen" && i+1 < len(os.Args){
            myPort = os.Args[i+1]
        }
        if os.Args[i] == "--backend" && i+1 < len(os.Args){
            backend = os.Args[i+1]
        }
    }
    intPort, err := strconv.Atoi(myPort)
    if err != nil{
        fmt.Println("invalid port")
    }
    //establish the three ports to use
    heartbeatPort = ":" + strconv.Itoa(intPort+1)
    backendListenPort = ":" + strconv.Itoa(intPort+2)
    fmt.Println("My backend Port:", backendListenPort)


    backendPorts := strings.Split(backend, ",")
    totalNodes = len(backendPorts)
    backendPortList = make(chan string, len(backendPorts))
    for i:=0; i < len(backendPorts); i++{
        peerBackPort, _ := strconv.Atoi(backendPorts[i][1:])

        backendPortList <- ":" + strconv.Itoa(peerBackPort+2)
    }

    go dialBackends()
    go listenBackends()
    go peerHeartbeat()

    go listenFrontendHeartbeat()
    listener, _ := net.Listen("tcp", ":" + myPort)
    fmt.Println("Listening to frontend at:", myPort, " Frontend Heartbeat at:", heartbeatPort)
    defer listener.Close()
    for {
        conn, _ := listener.Accept()
        
        fmt.Println("Found new connection")
        enc := json.NewEncoder(conn)
        dec := json.NewDecoder(conn)
        go handleConnection(&conn, enc, dec)
    }

}

func listenFrontendHeartbeat(){
    heartbeater, _ := net.Listen("tcp", heartbeatPort)

    for{
        heart, _:= heartbeater.Accept()
        go acceptHeartbeater(&heart)
    }
    defer heartbeater.Close()

}


//Dials to other replicas
func dialBackends(){
    for {
        aPort := <- backendPortList
        replica, err := net.Dial("tcp", aPort)
        if err != nil{
            backendPortList <- aPort
        }else{
            fmt.Println(aPort + " found")
            backendConnectionsLock.Lock()
            buddy := BackendReplica{Connection:replica, Sender:json.NewEncoder(replica),
                Receiver:json.NewDecoder(replica), Port:aPort}
            backendConnections = append(backendConnections, buddy)
            backendConnectionsLock.Unlock()

            go peerTalk(buddy)

        }
    }
}

//Accepts connections from replica backends
func listenBackends(){
    listener, _ := net.Listen("tcp", backendListenPort)
    defer listener.Close()
    for {
        conn, _ := listener.Accept()
        buddy := BackendReplica{Connection:conn, Sender:json.NewEncoder(conn),
                Receiver:json.NewDecoder(conn), Port:"noy"}
        backendConnectionsTalkLock.Lock()
        backendConnectionsTalk = append(backendConnectionsTalk, buddy)
        backendConnectionsTalkLock.Unlock()
    }
}

//Handles individual replica connections
func peerTalk(buddy BackendReplica){
    for {
        var talk string
        err := buddy.Receiver.Decode(&talk)
        fmt.Println("query:", talk)
        if err != nil{
            break
        }
        messageReceivedLock.Lock()
        messageReceived = time.Now()
        messageReceivedLock.Unlock()
        interpretBuddy(talk, buddy)
    }
    fmt.Println("A buddy died")
    backendConnectionsLock.Lock()
    for i:=0; i < len(backendConnections); i++{
        if backendConnections[i].Port == buddy.Port{
            backendPortList <- buddy.Port
            backendConnections = append(backendConnections[:i], backendConnections[i+1:]...)
            break
        }
    }
    backendConnectionsLock.Unlock()
    defer buddy.Connection.Close()

}

//For decoding what replicas and leader says to node
func interpretBuddy(message string, buddy BackendReplica){
    //decode what leader is saying or peer is saying
    if len(message) == 0{
        return
    }

    if message == "ELECTION"{
        //Votes for the first node that asks for a vote in a new term
        var electionTerm int
        buddy.Receiver.Decode(&electionTerm)
        fmt.Println("Received Term:", electionTerm)
        if electionTerm > term{
            if myState == 3 {
                myState = 1
            }
            term = electionTerm
            buddy.Sender.Encode("VOTED")
        } else {
            buddy.Sender.Encode("ALREADY VOTED")
        }
    } else if message == "LEADER"{
        var leaderLog []interface{}
        buddy.Receiver.Decode(&leaderLog)
        uncommitedLog = leaderLog
        for i:=0; i < len(uncommitedLog); i++{
            fmt.Println("what is this:", uncommitedLog[i])
        }
        buddy.Sender.Encode("NOTED")
        var response string
        err := buddy.Receiver.Decode(&response)
        if err != nil {

        } else{
            commitTheLog()
        }
    } 
}

//Runs until a message has not been received by anyone
//Turns into candidate when no message is received
func peerHeartbeat(){
    seed := rand.NewSource(time.Now().UnixNano())
    random := rand.New(seed)
    waitTime := time.Duration(random.Intn(4) + 3)*time.Second
    fmt.Println(waitTime)
    for {
        messageReceivedLock.RLock()

        if time.Now().Sub(messageReceived) > waitTime{
            myState = 2
            term += 1
            messageReceivedLock.RUnlock()
            break
        }
        messageReceivedLock.RUnlock()
    }
    fmt.Println("i am now a candidate")
    startMyElection()
}

//Starts its own election process
func startMyElection(){
    backendConnectionsTalkLock.Lock()
    for i:=0; i < len(backendConnectionsTalk); i++{
        backendConnectionsTalk[i].Sender.Encode("ELECTION")
        backendConnectionsTalk[i].Sender.Encode(term)
    }
    backendConnectionsTalkLock.Unlock()
    myVotes := 1
    backendConnectionsTalkLock.Lock()
    i := 0
    for i < len(backendConnectionsTalk){
        var response string
        err := backendConnectionsTalk[i].Receiver.Decode(&response)
        if err != nil{
            backendConnectionsTalk = append(backendConnectionsTalk[:i], backendConnectionsTalk[i+1:]...)
            continue
        }
        fmt.Println("Response:", response)
        if response == "VOTED"{
            myVotes++
        }
        i++
    }
    backendConnectionsTalkLock.Unlock()

    fmt.Println("my votes:", myVotes)
    if myVotes > totalNodes/2 {
        fmt.Println("Election WON")
        myState = 3
        go leaderHeartbeat(false)
    } else{
        fmt.Println("my election Failed")
        myState = 1
        messageReceivedLock.Lock()
        messageReceived = time.Now()
        messageReceivedLock.Unlock()
        go peerHeartbeat()
    }
}

//send uncommited logs to followers, get response and wait
func leaderHeartbeat(runOnce bool){
    for {
        if myState != 3 {
            go peerHeartbeat()
            break
        }
        backendConnectionsTalkLock.Lock()
        for i:=0; i < len(backendConnectionsTalk); i++{
            backendConnectionsTalk[i].Sender.Encode("LEADER")
            backendConnectionsTalk[i].Sender.Encode(uncommitedLog)
        }
        backendConnectionsTalkLock.Unlock()

        backendConnectionsTalkLock.Lock()
        i:=0
        responseCount := 1
        for i < len(backendConnectionsTalk){
            var response string
            err := backendConnectionsTalk[i].Receiver.Decode(&response)
            if err != nil{
                backendConnectionsTalk = append(backendConnectionsTalk[:i], backendConnectionsTalk[i+1:]...)
                continue
            }
            i++
            responseCount++
        }
        backendConnectionsTalkLock.Unlock()
        if responseCount > totalNodes/2 {
            backendConnectionsTalkLock.Lock()
            for i:=0; i < len(backendConnectionsTalk); i++{
                backendConnectionsTalk[i].Sender.Encode("COMMIT")
            }
            backendConnectionsTalkLock.Unlock()
            commitTheLog()
        }
        fmt.Println("LEADER HEARTBEAT")
        if runOnce == true{
            break
        }
        time.Sleep(2*time.Second)
    }
}

//goes through the log and commits all the entries
func commitTheLog(){
    uncommitedLogLock.Lock()
    for i:=0; i < len(uncommitedLog);{
        if uncommitedLog[i].(string) == "DELETE"{
            i++
            index := uncommitedLog[i].(string)
            lock.Lock()
            delete(info, index)
            lock.Unlock()
        } else if uncommitedLog[i].(string) == "ADD" {
            i++
            hero := uncommitedLog[i].(Hero)
            lock.Lock()
            if _,ok := info[getID(hero)]; !ok{
                info[getID(hero)] = hero
            }
            lock.Unlock()
        } else if uncommitedLog[i].(string) == "UPDATE" {
            i++
            index := uncommitedLog[i].(string)
            i++
            hero := uncommitedLog[i].(Hero)
            lock.Lock()
            info[index] = hero
            lock.Unlock()
        }
        i++
    }
    commitedLog = append(commitedLog, uncommitedLog)
    uncommitedLog = nil
    uncommitedLogLock.Unlock()
}



//Seconds heartbeat to frontend at time intervals
func acceptHeartbeater(heart *net.Conn){
    beater := json.NewEncoder(*heart)
    for {
        //sleep
        err := beater.Encode("alive")
        if err != nil{
            break
        }
        time.Sleep(3*time.Second)
    }
    (*heart).Close()
}

//Thread to handle a new connection to the database
//connection is the current connection, encoder and decoder help send messages to frontend
func handleConnection(connection *net.Conn, encoder *json.Encoder, decoder *json.Decoder){
   
    for {
        fmt.Println("new connection")
        var query string
        err := decoder.Decode(&query)
        fmt.Println(query)
        if err != nil{
            break
        }
        processQuery(query, encoder, decoder)
        if myState == 3{
            leaderHeartbeat(true)
        }
    }
    fmt.Println("A connection closed.")
    (*connection).Close()
    connection = nil

}

func processQuery(query string, encoder *json.Encoder, decoder *json.Decoder){
    if len(query) == 0{
        return
    }
    if query == "LEADER?" {
        fmt.Println(query)
        if myState == 3{
            encoder.Encode("YES")
            
        } else {
            encoder.Encode("NO")
        }
    }
    uncommitedLogLock.Lock()
    if query == "GET ALL"{
        lock.RLock()
        data := info
        lock.RUnlock()
        encoder.Encode(data)

    } else if query == "DELETE" {
        uncommitedLog = append(uncommitedLog, query)
        var indices string
        decoder.Decode(&indices)
        uncommitedLog = append(uncommitedLog, indices)

    } else if query == "ADD" {
        var hero Hero
        decoder.Decode(&hero)
        lock.Lock()
        if _,ok := info[getID(hero)]; !ok{
            uncommitedLog = append(uncommitedLog, query)
            uncommitedLog = append(uncommitedLog, hero)
        }
        lock.Unlock()
      
    } else if query == "GET" {
        var index string
        decoder.Decode(&index)
        lock.RLock()
        var data Hero
        if _,ok := info[index]; ok{
            data = info[index]
        }
        lock.RUnlock()
        encoder.Encode(data)

    } else if query == "UPDATE" {
        uncommitedLog = append(uncommitedLog, query)
        var index string
        decoder.Decode(&index)
        var hero Hero
        decoder.Decode(&hero)
        uncommitedLog = append(uncommitedLog, index)
        uncommitedLog = append(uncommitedLog, hero)

    }
    uncommitedLogLock.Unlock()
}

//Function used to hash a hero to get their ID in the map
func getID(hero Hero) string{
    h := sha256.New()
    h.Write([]byte(hero.Name + hero.SuperheroName + hero.From + hero.Power))
    x := base64.URLEncoding.EncodeToString(h.Sum(nil))
    return x
}