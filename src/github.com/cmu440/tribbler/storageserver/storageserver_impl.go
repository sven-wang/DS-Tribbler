package storageserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
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
	role           Role                       // Role of this storage server - MASTER or SLAVE
	registerChan   chan storagerpc.Node       // Channel used for register servers
	mux            sync.Mutex                 // Lock for this storage server
	numNodes       int                        // Number of servers in the hash ring
	allServers     map[string]storagerpc.Node // All the storage servers in the system
	flattenServers []simpleNode               // For the ease of server validation
	myHostPort     string                     // Host address for this server

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

	// Data structures for caching and leasing
	keyToLeases      map[string]*leasesInfo
	beingRevokedKeys map[string]bool
}

// Simplified Node struct - Only store one virtual ID for a server
type simpleNode struct {
	hostPort  string // The host:port address of the storage server node.
	virtualID uint32 // One of the virtual IDs identifying this storage server node.
}

type leaseInfo struct {
	startTime int64
	validTime int
}

// All the leases for a given key
type leasesInfo struct {
	updateLock  sync.Mutex
	accessLock  sync.Mutex
	validLeases map[string]leaseInfo
}

//
// Flatten the slice of storagerpc.Node to slice of simpleNode, which is order by each virtual ID
//
func FlattenServers(servers map[string]storagerpc.Node) []simpleNode {
	var flattenedServers []simpleNode
	for _, serverInfo := range servers {
		curHostPort := serverInfo.HostPort
		for _, virtualID := range serverInfo.VirtualIDs {
			flattenedServers = append(flattenedServers, simpleNode{hostPort: curHostPort, virtualID: virtualID})
		}
	}
	// Sort the simpleNode list using virtualID in ascending order
	sort.Slice(flattenedServers, func(i, j int) bool {
		return flattenedServers[i].virtualID < flattenedServers[j].virtualID
	})
	return flattenedServers
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
	// Wrap the storageServer before registering it for RPC.
	err := rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}
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

	// Data fields for caching and leasing
	ss.keyToLeases = make(map[string]*leasesInfo)
	ss.beingRevokedKeys = make(map[string]bool)

	ownHostAddr := net.JoinHostPort("localhost", strconv.Itoa(port))
	ss.myHostPort = ownHostAddr

	// If the server is the MASTER server
	if len(masterServerHostPort) == 0 {
		err = StartListen(port)
		if err != nil {
			return nil, err
		}

		ss.role = MASTER
		ss.registerChan = make(chan storagerpc.Node)

		// MASTER registers itself first
		ss.mux.Lock()
		ss.allServers[ownHostAddr] = storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}
		ss.mux.Unlock()
		if numNodes > 1 {
			breakLoopFlag := false
			for !breakLoopFlag {
				select {
				case newNode := <-ss.registerChan:
					ss.mux.Lock()
					if _, ok := ss.allServers[newNode.HostPort]; !ok {
						ss.allServers[newNode.HostPort] = newNode
					}
					// All servers have received OK
					if len(ss.allServers) == ss.numNodes {
						ss.mux.Unlock()
						breakLoopFlag = true
						break
					}
					ss.mux.Unlock()
				}
			}
		}
	} else {
		ss.role = SLAVE
		// SLAVE server ask the MASTER to add itself into the ring
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)
		// Handle initialization dial error
		for err != nil {
			time.Sleep(10 * time.Millisecond)
			client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}

		args := &storagerpc.RegisterArgs{ServerInfo: storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}}
		for {
			reply := &storagerpc.RegisterReply{}
			err = client.Call("StorageServer.RegisterServer", args, reply)
			if err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.NotReady {
				time.Sleep(time.Second)
			} else if reply.Status == storagerpc.OK {
				ss.mux.Lock()
				for _, nodeInfo := range reply.Servers {
					ss.allServers[nodeInfo.HostPort] = nodeInfo
				}
				ss.mux.Unlock()
				break
			}
		}

		err = StartListen(port)
		if err != nil {
			return nil, err
		}
	}

	ss.flattenServers = FlattenServers(ss.allServers)
	// Start the main routine after setup
	go ss.MainRoutine()
	return ss, nil
}

func StartListen(port int) error {
	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mux.Lock()
	var registeredServers []storagerpc.Node
	for _, nodeInfo := range ss.allServers {
		registeredServers = append(registeredServers, nodeInfo)
	}
	if len(ss.allServers) == ss.numNodes {
		ss.mux.Unlock()
		reply.Status = storagerpc.OK
		reply.Servers = registeredServers
	} else {
		if _, ok := ss.allServers[args.ServerInfo.HostPort]; !ok && len(ss.allServers) == ss.numNodes-1 {
			registeredServers = append(registeredServers, args.ServerInfo)
			reply.Status = storagerpc.OK
			reply.Servers = registeredServers
		} else {
			reply.Status = storagerpc.NotReady
		}
		ss.mux.Unlock()
		ss.registerChan <- args.ServerInfo
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
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.getRequestChan <- *args
	*reply = <-ss.getReplyChan
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Check whether the key is being granted lease(s). If so, revoke all of the leases
	ss.mux.Lock()
	if leases, ok := ss.keyToLeases[args.Key]; ok {
		ss.mux.Unlock()
		leases.updateLock.Lock()
		leases.accessLock.Lock()
		if len(leases.validLeases) > 0 {
			leases.accessLock.Unlock()
			defer leases.updateLock.Unlock()
			ss.RevokeLease(args.Key, leases)
		} else {
			leases.accessLock.Unlock()
			leases.updateLock.Unlock()
		}
	} else {
		ss.mux.Unlock()
	}

	ss.deleteRequestChan <- *args
	*reply = <-ss.deleteReplyChan

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.getListRequestChan <- *args
	*reply = <-ss.getListReplyChan
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Check whether the key is being granted lease(s). If so, revoke all of the leases
	ss.mux.Lock()
	if leases, ok := ss.keyToLeases[args.Key]; ok {
		ss.mux.Unlock()
		leases.updateLock.Lock()
		leases.accessLock.Lock()
		if len(leases.validLeases) > 0 {
			leases.accessLock.Unlock()
			defer leases.updateLock.Unlock()
			ss.RevokeLease(args.Key, leases)
		} else {
			leases.accessLock.Unlock()
			leases.updateLock.Unlock()
		}
	} else {
		ss.mux.Unlock()
	}

	ss.putRequestChan <- *args
	*reply = <-ss.putReplyChan
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Check whether the key is being granted lease(s). If so, revoke all of the leases
	ss.mux.Lock()
	if leases, ok := ss.keyToLeases[args.Key]; ok {
		ss.mux.Unlock()
		leases.updateLock.Lock()
		leases.accessLock.Lock()
		if len(leases.validLeases) > 0 {
			leases.accessLock.Unlock()
			defer leases.updateLock.Unlock()
			ss.RevokeLease(args.Key, leases)
		} else {
			leases.accessLock.Unlock()
			leases.updateLock.Unlock()
		}
	} else {
		ss.mux.Unlock()
	}

	ss.appendListRequestChan <- *args
	*reply = <-ss.appendListReplyChan
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Check whether the key is being revoked. If not, revoke the key
	// Check whether the key is being granted lease(s). If so, revoke all of the leases
	ss.mux.Lock()
	if leases, ok := ss.keyToLeases[args.Key]; ok {
		ss.mux.Unlock()
		leases.updateLock.Lock()
		leases.accessLock.Lock()
		if len(leases.validLeases) > 0 {
			leases.accessLock.Unlock()
			defer leases.updateLock.Unlock()
			ss.RevokeLease(args.Key, leases)
		} else {
			leases.accessLock.Unlock()
			leases.updateLock.Unlock()
		}
	} else {
		ss.mux.Unlock()
	}

	ss.removeListRequestChan <- *args
	*reply = <-ss.removeListReplyChan
	return nil
}

//
// Check whether this server is the right server to handle the request
//
func (ss *storageServer) ValidateServer(key string) bool {
	hashedKey := libstore.StoreHash(key)

	var chosenServer = ""
	// Find the proper server to serve this request. Since the virtual IDs are sorted in the storageServers list, break
	// the loop once a virtual ID that is bigger than the hashed key is found
	for _, serverInfo := range ss.flattenServers {
		// Need to be <= rather than ==
		if hashedKey <= serverInfo.virtualID {
			chosenServer = serverInfo.hostPort
			break
		}
	}
	// If the hashed key is bigger than every virtual ID, use the first simpleNode to serve this request
	if len(chosenServer) == 0 {
		chosenServer = ss.flattenServers[0].hostPort
	}

	if chosenServer == ss.myHostPort {
		return true
	} else {
		return false
	}
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
				// If the Libstore instance wants a lease on the key
				if args.WantLease {
					ss.mux.Lock()
					// If the key is being revoked, stop granting
					if _, ok := ss.beingRevokedKeys[args.Key]; ok {
						ss.mux.Unlock()
						reply.Lease.Granted = false
					} else {
						grant := true
						if _, ok := ss.keyToLeases[args.Key]; !ok {
							ss.keyToLeases[args.Key] = &leasesInfo{}
							ss.keyToLeases[args.Key].validLeases = make(map[string]leaseInfo)
						}
						leases := ss.keyToLeases[args.Key]
						ss.mux.Unlock()

						leases.accessLock.Lock()
						if _, ok := leases.validLeases[args.HostPort]; ok {
							grant = false
							reply.Lease.Granted = false
						}

						if grant {
							reply.Lease.Granted = true
							reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
							timestamp := time.Now().UnixNano()
							lease := leaseInfo{startTime: timestamp, validTime: storagerpc.LeaseSeconds}
							leases.validLeases[args.HostPort] = lease
							go ss.TimeoutRevoke(args.HostPort, args.Key, timestamp, reply.Lease.ValidSeconds)
						}
						leases.accessLock.Unlock()
					}
				}
			} else {
				reply.Status = storagerpc.KeyNotFound
			}
			ss.getReplyChan <- reply
		case args := <-ss.deleteRequestChan:
			reply := storagerpc.DeleteReply{}
			if _, ok := ss.stringTable[args.Key]; ok {
				reply.Status = storagerpc.OK
				delete(ss.stringTable, args.Key)
			} else {
				reply.Status = storagerpc.KeyNotFound
			}
			ss.deleteReplyChan <- reply
		case args := <-ss.getListRequestChan:
			reply := storagerpc.GetListReply{}
			if list, ok := ss.listTable[args.Key]; ok {
				reply.Status = storagerpc.OK
				reply.Value = list
				// If the Libstore instance wants a lease on the key
				if args.WantLease {
					ss.mux.Lock()
					// If the key is being revoked, stop granting
					if _, ok := ss.beingRevokedKeys[args.Key]; ok {
						ss.mux.Unlock()
						reply.Lease.Granted = false
					} else {
						grant := true
						if _, ok := ss.keyToLeases[args.Key]; !ok {
							ss.keyToLeases[args.Key] = &leasesInfo{}
							ss.keyToLeases[args.Key].validLeases = make(map[string]leaseInfo)
						}
						leases := ss.keyToLeases[args.Key]
						ss.mux.Unlock()

						leases.accessLock.Lock()
						if _, ok := leases.validLeases[args.HostPort]; ok {
							grant = false
							reply.Lease.Granted = false
						}

						if grant {
							reply.Lease.Granted = true
							reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
							timestamp := time.Now().UnixNano()
							lease := leaseInfo{startTime: timestamp, validTime: storagerpc.LeaseSeconds}
							leases.validLeases[args.HostPort] = lease
							go ss.TimeoutRevoke(args.HostPort, args.Key, timestamp, reply.Lease.ValidSeconds)
						}
						leases.accessLock.Unlock()
					}
				}
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
			}
			ss.removeListReplyChan <- reply
		}
	}
}

//
// Handle timeout revoke for each granted lease
//
func (ss *storageServer) TimeoutRevoke(hostPort string, key string, startTime int64, validTime int) {
	timer := time.NewTimer(time.Duration(validTime+storagerpc.LeaseGuardSeconds) * time.Second)
	// Hang for a while
	<-timer.C
	ss.mux.Lock()
	if leases, ok := ss.keyToLeases[key]; ok {
		ss.mux.Unlock()
		leases.accessLock.Lock()
		if lease, ok := leases.validLeases[hostPort]; ok && lease.startTime == startTime {
			delete(leases.validLeases, hostPort)
		}
		leases.accessLock.Unlock()
	}
}

//
// Revoke all the leases associated with the same key. In the same time, block all the rpc calls that try to change the
// value of the key
//
func (ss *storageServer) RevokeLease(key string, leases *leasesInfo) {
	ss.mux.Lock()
	ss.beingRevokedKeys[key] = true
	ss.mux.Unlock()
	leases.accessLock.Lock()
	for hostPort, _ := range leases.validLeases {
		go SendRevokeLease(key, hostPort, leases)
	}
	leases.accessLock.Unlock()

	exitLoopFlag := false
	for !exitLoopFlag {
		leases.accessLock.Lock()
		curLen := len(leases.validLeases)
		leases.accessLock.Unlock()
		if curLen == 0 {
			exitLoopFlag = true
		}
		time.Sleep(time.Millisecond)
	}

	// Clear the lease metadata related to the key
	leases.accessLock.Lock()
	leases.validLeases = make(map[string]leaseInfo)
	leases.accessLock.Unlock()
	ss.mux.Lock()
	delete(ss.beingRevokedKeys, key)
	ss.mux.Unlock()
}

//
// RPC call remote RevokeLease
//
func SendRevokeLease(key string, hostPort string, leases *leasesInfo) {
	// TODO: May add cache for the client here
	client, err := rpc.DialHTTP("tcp", hostPort)
	if err != nil {
		// fmt.Println("Cannot contact the libstore!")
		return
	}

	args := &storagerpc.RevokeLeaseArgs{Key: key}
	reply := &storagerpc.RevokeLeaseReply{}
	err = client.Call("LeaseCallbacks.RevokeLease", args, reply)

	if err != nil {
		// fmt.Println("Error during RevokeLease RPC call!")
		return
	}

	if reply.Status != storagerpc.OK && reply.Status != storagerpc.KeyNotFound {
		fmt.Println("Unknown RevokeLease reply!")
	}

	leases.accessLock.Lock()
	delete(leases.validLeases, hostPort)
	leases.accessLock.Unlock()
}
