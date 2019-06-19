Zami Talukder
Instructions:
go run backend.go --listen 8093 --backend :8090,:8096
go run backend.go --listen 8090 --backend :8093,:8096
go run backend.go --listen 8096 --backend :8090,:8093

go run frontend.go --backend :8090,:8093,:8096

run backend before frontend

Following arguments are required:
--backend

State of work:
Backend->frontend heartbeats use backend_port +1
Backends listen to each other at backend_port +2

Elections have been implemented and work correctly.
Leaders are reelected when the current leader fails.
The leader is found by the frontend everytime it tries to establish a connection.
The frontend asks all replicas until one tells it that it is the frontend.
The backend is allowed to reconnect when it comes back alive. 
Elections are stalemates when a quorem is not present. 
Frontend is still working as intended.
Leader is the only node talking to all frontends.
The leader tells all the nodes to commit. It tells them to commit whenever 
a write/delete/update is called by the frontend.


When a replica rejoins, the replica does not have the same information as the other nodes.
When nodes heal from a network partition and two leaders are present, the leader or 
candidate with the higher term is chosen and the other leader steps down.

Both of these could be dealt with by comparing the size of the committed log and giving the
replica with the smaller log all the data from the replica with the larger log. 

Differing logs of leaders from different network partitions is not resolved.


