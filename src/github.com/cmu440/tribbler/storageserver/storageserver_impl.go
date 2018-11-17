package storageserver

import (
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
	role         Role                       // Role of this storage server - MASTER or SLAVE
	registerChan chan storagerpc.Node       // Channel used for register servers
	mux          sync.Mutex                 // Lock for this storage server
	numNodes     int                        // Number of servers in the hash ring
	allServers   map[string]storagerpc.Node // All the storage servers in the system
	virtualIDs   []uint32                   // Virtual IDs assigned to this server

	// Hash tables for this server
	stringTable map[string]string
	listTable   map[string][]string

	// Channels
	getRequestChan        chan storagerpc.GetArgs
	getReplyChan          chan storagerpc.GetReply
	deleteRequestChan     chan storagerpc.DeleteArgs
	deleteReplyChan       chan storagerpc.DeleteReply
	getListRequestChan    chan storagerpc.GetArgs
	getListReplyChan      chan storagerpc.GetListReply
	putRequestChan        chan storagerpc.PutArgs
	putReplyChan          chan storagerpc.PutReply
	appendListRequestChan chan storagerpc.PutArgs
	appendListReplyChan   chan storagerpc.PutReply
	removeListRequestChan chan storagerpc.PutArgs
	removeListReplyChan   chan storagerpc.PutReply

	// TODO: add stuff related to lease for the final checkpoint
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
	ss.stringTable = make(map[string]string)
	ss.listTable = make(map[string][]string)
	// Init channels
	ss.getRequestChan = make(chan storagerpc.GetArgs)
	ss.getReplyChan = make(chan storagerpc.GetReply)
	ss.deleteRequestChan = make(chan storagerpc.DeleteArgs)
	ss.deleteReplyChan = make(chan storagerpc.DeleteReply)
	ss.getListRequestChan = make(chan storagerpc.GetArgs)
	ss.getListReplyChan = make(chan storagerpc.GetListReply)
	ss.putRequestChan = make(chan storagerpc.PutArgs)
	ss.putReplyChan = make(chan storagerpc.PutReply)
	ss.appendListRequestChan = make(chan storagerpc.PutArgs)
	ss.appendListReplyChan = make(chan storagerpc.PutReply)
	ss.removeListRequestChan = make(chan storagerpc.PutArgs)
	ss.removeListReplyChan = make(chan storagerpc.PutReply)

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

	ownHostAddr := net.JoinHostPort("localhost", strconv.Itoa(port))
	// If the server is the MASTER server
	if len(masterServerHostPort) == 0 {
		ss.role = MASTER
		ss.registerChan = make(chan storagerpc.Node)

		// MASTER registers itself first
		ss.allServers[ownHostAddr] = storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}
		if numNodes > 1 {
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
				ss.mux.Lock()
				for _, nodeInfo := range reply.Servers {
					ss.allServers[nodeInfo.HostPort] = nodeInfo
				}
				ss.mux.Unlock()
				break
			}
		}
	}

	// Start the main routine after setup
	go ss.MainRoutine()
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
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.getRequestChan <- *args
	*reply = <-ss.getReplyChan
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.deleteRequestChan <- *args
	*reply = <-ss.deleteReplyChan
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.getListRequestChan <- *args
	*reply = <-ss.getListReplyChan
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.putRequestChan <- *args
	*reply = <-ss.putReplyChan
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.appendListRequestChan <- *args
	*reply = <-ss.appendListReplyChan
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// TODO: Check if this is the correct server to serve this request
	ss.removeListRequestChan <- *args
	*reply = <-ss.removeListReplyChan
	return nil
}

//
// Main routine of the storage server that handles multiple storage requests
//
func (ss *storageServer) MainRoutine() {
	for {
		select {
		case args := <-ss.getRequestChan:
			reply := storagerpc.GetReply{}
			if val, ok := ss.stringTable[args.Key]; ok {
				reply.Status = storagerpc.OK
				reply.Value = val
				// TODO: Add lease stuff for the final checkpoint
			} else {
				reply.Status = storagerpc.KeyNotFound
			}
			ss.getReplyChan <- reply
		case args := <-ss.deleteRequestChan:
			reply := storagerpc.DeleteReply{}
			if _, ok := ss.stringTable[args.Key]; ok {
				reply.Status = storagerpc.OK
				delete(ss.stringTable, args.Key)
				// TODO: Add revoke for the final checkpoint
			} else {
				reply.Status = storagerpc.KeyNotFound
			}
			ss.deleteReplyChan <- reply
		case args := <-ss.getListRequestChan:
			reply := storagerpc.GetListReply{}
			if list, ok := ss.listTable[args.Key]; ok {
				reply.Status = storagerpc.OK
				// TODO: reference copy OK here?
				reply.Value = list
				// TODO: Add lease stuff for the final checkpoint
			} else {
				reply.Status = storagerpc.KeyNotFound
			}
			ss.getListReplyChan <- reply
		case args := <-ss.putRequestChan:
			reply := storagerpc.PutReply{}
			ss.stringTable[args.Key] = args.Value
			reply.Status = storagerpc.OK
			ss.putReplyChan <- reply
		case args := <-ss.appendListRequestChan:
			reply := storagerpc.PutReply{}
			// AppendToList will always be called after user existence check
			if _, ok := ss.listTable[args.Key]; !ok {
				ss.listTable[args.Key] = nil
			}
			existFlag := false
			for _, val := range ss.listTable[args.Key] {
				if val == args.Value {
					reply.Status = storagerpc.ItemExists
					ss.appendListReplyChan <- reply
					existFlag = true
					break
				}
			}
			// TODO: Add revoke for the final checkpoint
			if !existFlag {
				ss.listTable[args.Key] = append(ss.listTable[args.Key], args.Value)
				reply.Status = storagerpc.OK
				ss.appendListReplyChan <- reply
			}
		case args := <-ss.removeListRequestChan:
			reply := storagerpc.PutReply{}
			// TODO: Whether this choice is correct?
			if _, ok := ss.listTable[args.Key]; !ok {
				reply.Status = storagerpc.OK
				ss.removeListReplyChan <- reply
				continue
			}
			targetIdx := -1
			for idx, val := range ss.listTable[args.Key] {
				if val == args.Value {
					targetIdx = idx
					break
				}
			}
			if targetIdx == -1 {
				reply.Status = storagerpc.ItemNotFound
			} else {
				ss.listTable[args.Key] = append(ss.listTable[args.Key][:targetIdx],
					ss.listTable[args.Key][targetIdx+1:]...)
				reply.Status = storagerpc.OK
				// TODO: Add revoke for the final checkpoint
			}
			ss.removeListReplyChan <- reply
		}
	}
}
