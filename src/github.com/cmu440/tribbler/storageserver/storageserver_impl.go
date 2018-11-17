package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type Role int

const (
	MASTER Role = 0
	SLAVE  Role = 1
)

type storageServer struct {
	role       Role                       // Role of this storage server - MASTER or SLAVE
	registerChan chan storagerpc.Node     // Channel used for register servers
	mux			sync.Mutex				  // Lock for this storage server
	numNodes   int						  // Number of servers in the hash ring
	allServers map[string]storagerpc.Node // All the storage servers in the system
	virtualIDs []uint32                   // Virtual IDs assigned to this server
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	ss := new(storageServer)
	ss.virtualIDs = virtualIDs
	ss.numNodes = numNodes
	ss.allServers = make(map[string]storagerpc.Node)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	// Wrap the storageServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	// TODO: Own IP address?
	ownHostAddr := net.JoinHostPort("localhost", strconv.Itoa(port))
	// If the server is the MASTER server
	if len(masterServerHostPort) == 0 {
		ss.role = MASTER
		ss.registerChan = make(chan storagerpc.Node)

		// MASTER registers itself first
		ss.allServers[ownHostAddr] = storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}
		for {
			select {
			case newNode := <-ss.registerChan:
				ss.mux.Lock()
				if _, ok := ss.allServers[newNode.HostPort]; !ok {
					ss.allServers[newNode.HostPort] = newNode
					// All servers have registered
					if len(ss.allServers) == ss.numNodes {
						ss.mux.Unlock()
						break
					}
				}
				ss.mux.Unlock()
			}
		}
	} else {
		ss.role = SLAVE
		// SLAVE server ask the MASTER to add itself into the ring
		client, err := rpc.DialHTTP("tcp", net.JoinHostPort(masterServerHostPort, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}

		args := &storagerpc.RegisterArgs{ServerInfo: storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}}
		reply := &storagerpc.RegisterReply{}
		// Retry no more than 5 times
		for {
			err = client.Call("StorageServer.RegisterServer", args, reply)
			if err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.NotReady {
				time.Sleep(1 * time.Second)
			} else if reply.Status == storagerpc.OK {
				for _, nodeInfo := range reply.Servers {
					ss.allServers[nodeInfo.HostPort] = nodeInfo
				}
				break
			}
		}
	}

	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mux.Lock()
	if len(ss.allServers) == ss.numNodes {
		reply.Status = storagerpc.OK
		var registeredServers []storagerpc.Node
		for _, nodeInfo := range ss.allServers {
			registeredServers = append(registeredServers, nodeInfo)
		}
		reply.Servers = registeredServers
		ss.mux.Unlock()
	} else {
		ss.mux.Unlock()
		ss.registerChan <- args.ServerInfo
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
