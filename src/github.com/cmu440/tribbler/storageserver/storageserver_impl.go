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
	role         Role                       // Role of this storage server - MASTER or SLAVE
	registerChan chan registerInfo          // Channel used for register servers
	mux          sync.Mutex                 // Lock for this storage server
	numNodes     int                        // Number of servers in the hash ring
	allServers   map[string]storagerpc.Node // All the storage servers in the system
	virtualIDs   []uint32                   // Virtual IDs assigned to this server
	myHostPort   string                     // Host address for this server

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

	keyToLibstore		map[string][]string		// Map from key to the Libstore instances that cache this key
	keyToLock		    map[string]*sync.Mutex	// Fine grained user-level lock
	revokingKey			map[string]int			// Map of keys that are being revoked
	leaseStartTime      map[string]int64	    // Each granted lease's latest start time
}

type registerInfo struct {
	serverInfo storagerpc.Node
	lenChan    chan int
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
	// Sort the virtual IDs owned by this storage server in ascending order so as to ease server validation
	sort.Slice(ss.virtualIDs, func(i, j int) bool {
		return ss.virtualIDs[i] < ss.virtualIDs[j]
	})
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
	ss.keyToLibstore = make(map[string][]string)
	ss.keyToLock = make(map[string]*sync.Mutex)
	ss.revokingKey = make(map[string]int)		// Used for stop granting lease for the key
	ss.leaseStartTime = make(map[string]int64)

	ownHostAddr := net.JoinHostPort("localhost", strconv.Itoa(port))
	ss.myHostPort = ownHostAddr
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

	// If the server is the MASTER server
	if len(masterServerHostPort) == 0 {
		ss.role = MASTER
		ss.registerChan = make(chan registerInfo)

		// MASTER registers itself first
		ss.mux.Lock()
		ss.allServers[ownHostAddr] = storagerpc.Node{HostPort: ownHostAddr, VirtualIDs: virtualIDs}
		ss.mux.Unlock()
		if numNodes > 1 {
			for {
				select {
				case registerInfo := <-ss.registerChan:
					newNode := registerInfo.serverInfo
					ss.mux.Lock()
					if _, ok := ss.allServers[newNode.HostPort]; !ok {
						ss.allServers[newNode.HostPort] = newNode
					}
					registerInfo.lenChan <- len(ss.allServers)
					// All servers have registered
					if len(ss.allServers) == ss.numNodes {
						ss.mux.Unlock()
						break
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
	lenChan := make(chan int)
	ss.registerChan <- registerInfo{lenChan: lenChan, serverInfo: args.ServerInfo}
	serverNum := <-lenChan

	if serverNum == ss.numNodes {
		reply.Status = storagerpc.OK
		var registeredServers []storagerpc.Node
		for _, nodeInfo := range ss.allServers {
			registeredServers = append(registeredServers, nodeInfo)
		}
		reply.Servers = registeredServers
	} else {
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

	// Check whether the key is being revoked. If not, revoke the key
	ss.RevokeLease(args.Key)

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

	// Check whether the key is being revoked. If not, revoke the key
	ss.RevokeLease(args.Key)

	ss.putRequestChan <- *args
	*reply = <-ss.putReplyChan
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.ValidateServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Check whether the key is being revoked. If not, revoke the key
	ss.RevokeLease(args.Key)

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
	ss.RevokeLease(args.Key)

	ss.removeListRequestChan <- *args
	*reply = <-ss.removeListReplyChan
	return nil
}

//
// Check whether this server is the right server to handle the request
//
func (ss *storageServer) ValidateServer(key string) bool {
	hashedKey := libstore.StoreHash(key)

	candVirtualID := ss.virtualIDs[0]
	for idx, curID := range ss.virtualIDs {
		if idx == 0 {
			continue
		}
		if curID > hashedKey {
			candVirtualID = curID
			break
		}
	}

	for _, serverInfo := range ss.allServers {
		if serverInfo.HostPort == ss.myHostPort {
			continue
		}
		for _, curID := range serverInfo.VirtualIDs {
			if curID > hashedKey && curID < candVirtualID {
				return false
			}
		}
	}

	return true
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
					if _, ok := ss.revokingKey[args.Key]; ok {
						reply.Lease.Granted = false
					} else {
						grant := true
						if hostPorts, ok := ss.keyToLibstore[args.Key]; ok {
							for _, hostPort := range hostPorts {
								if hostPort == args.HostPort {
									grant = false
									reply.Lease.Granted = false
									break
								}
							}
						} else {
							ss.keyToLibstore[args.Key] = nil
						}
						if grant {
							ss.keyToLibstore[args.Key] = append(ss.keyToLibstore[args.Key], args.HostPort)
							reply.Lease.Granted = true
							reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
							timestamp := time.Now().UnixNano()
							ss.leaseStartTime[args.Key] = timestamp
							go ss.TimeoutRevoke(args.Key, timestamp, reply.Lease.ValidSeconds)
						}
					}
					ss.mux.Unlock()
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
					if _, ok := ss.revokingKey[args.Key]; ok {
						reply.Lease.Granted = false
					} else {
						grant := true
						if hostPorts, ok := ss.keyToLibstore[args.Key]; ok {
							for _, hostPort := range hostPorts {
								if hostPort == args.HostPort {
									grant = false
									reply.Lease.Granted = false
									break
								}
							}
						} else {
							ss.keyToLibstore[args.Key] = nil
						}
						if grant {
							ss.keyToLibstore[args.Key] = append(ss.keyToLibstore[args.Key], args.HostPort)
							reply.Lease.Granted = true
							reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
							timestamp := time.Now().UnixNano()
							ss.leaseStartTime[args.Key] = timestamp
							go ss.TimeoutRevoke(args.Key, timestamp, reply.Lease.ValidSeconds)
						}
					}
					ss.mux.Unlock()
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
func (ss *storageServer) TimeoutRevoke(key string, startTime int64, validTime int) {
	timer := time.NewTimer(time.Duration(validTime + storagerpc.LeaseGuardSeconds) * time.Second)
	revokeLeaseFlag := false
	// Hang for a while
	for {
		select {
		case <-timer.C:
			ss.mux.Lock()
			if curStartTime, ok := ss.leaseStartTime[key]; ok {
				if curStartTime == startTime {
					revokeLeaseFlag = true
				}
			}
			ss.mux.Unlock()
			break
		}
	}
	if revokeLeaseFlag {
		ss.RevokeLease(key)
	}
}

//
// Revoke all the leases associated with the same key. In the same time, block all the rpc calls that try to change the
// value of the key
//
func (ss *storageServer) RevokeLease(key string) bool {
	var lockOnKey *sync.Mutex
	ss.mux.Lock()
	if lockOnKey, ok := ss.keyToLock[key]; !ok {
		lockOnKey = &sync.Mutex{}
		ss.keyToLock[key] = lockOnKey
	}
	if _, ok := ss.revokingKey[key]; !ok {
		ss.revokingKey[key] = 1
	} else {
		ss.revokingKey[key]++
	}
	ss.mux.Unlock()

	revokeReplyChan := make(chan bool)
	// Lock the key when revoke lease, so other modification RPC calls cannot proceed until all revoke lease finished
	lockOnKey.Lock()
	defer lockOnKey.Unlock()

	totalCnt := 0
	ss.mux.Lock()
	if hostPorts, ok := ss.keyToLibstore[key]; ok {
		for _, hostPort := range hostPorts {
			totalCnt++
			go SendRevokeLease(key, hostPort, revokeReplyChan)
		}
	}
	ss.mux.Unlock()

	currentCnt := 0
	if totalCnt > 0 {
		for {
			select {
			case <-revokeReplyChan:
				currentCnt++
				if currentCnt == totalCnt {
					break
				}
			}
		}
	}

	ss.mux.Lock()
	if ss.revokingKey[key] == 1 {
		delete(ss.revokingKey, key)
	} else {
		ss.revokingKey[key]--
	}
	// Always clear the keyToLibstore[key]
	ss.keyToLibstore[key] = nil
	ss.mux.Unlock()
	return true
}

func SendRevokeLease(key string, hostPort string, revokeReplyChan chan bool) {
	// TODO: May add cache for the client here
	client, err := rpc.DialHTTP("tcp", hostPort)
	if err != nil {
		fmt.Println("Cannot contact the libstore!")
		revokeReplyChan <- true
		return
	}

	args := &storagerpc.RevokeLeaseArgs{Key: key}
	reply := &storagerpc.RevokeLeaseReply{}
	err = client.Call("LeaseCallbacks.RevokeLease", args, reply)

	if err != nil {
		fmt.Println("Error during RevokeLease RPC call!")
		revokeReplyChan <- true
		return
	}

	if reply.Status != storagerpc.OK && reply.Status != storagerpc.KeyNotFound {
		fmt.Println("Unknown RevokeLease reply!")
	}
	revokeReplyChan <- true
}
