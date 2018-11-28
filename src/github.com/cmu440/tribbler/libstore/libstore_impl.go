package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

// The Libstore instance uses an 'rpc.Client' in order to perform RPCs to the
// Master StorageServer.
type libstore struct {
	mux            sync.Mutex             // Lock for the libstore instance
	msClient       *rpc.Client            // RPC client to the Master StorageServer
	storageServers []simpleNode           // List of storage servers that are in the hash ring
	mode           LeaseMode              // Debugging flag
	myHostPort     string                 // IP address of the libstore instance
	hostToClient   map[string]*rpc.Client // Cache for the seen connections' clients

	// Cache for the Libstore instance
	stringCache map[string]string		  // Data cache for string value
	listCache map[string][]string		  // Data cache for string slice value
	leaseStartTime map[string]int64	      // Cache for each cached data's entry time
	leaseValidTime map[string]int		  // Cache for each cached data's lease time
	requestTimeWindow	   map[string][]int64	  // Cache for each query's count
}

// Simplified Node struct - Only store one virtual ID for a server
type simpleNode struct {
	hostPort  string // The host:port address of the storage server node.
	virtualID uint32 // One of the virtual IDs identifying this storage server node.
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := new(libstore)
	// Libstore instance registers itself for RevokeLease RPC
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	ls.mode = mode
	ls.myHostPort = myHostPort
	ls.hostToClient = make(map[string]*rpc.Client)
	if mode != Never {
		ls.stringCache = make(map[string]string)
		ls.listCache = make(map[string][]string)
		ls.leaseStartTime = make(map[string]int64)
		ls.leaseValidTime = make(map[string]int)
		ls.requestTimeWindow = make(map[string][]int64)
	}

	// Contact the master server to know a list of available storage servers via GetServers RPC
	client, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	ls.msClient = client
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	// Retry no more than 5 times
	retryCnt := 0
	for retryCnt < 6 {
		err = client.Call("StorageServer.GetServers", args, reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.NotReady {
			time.Sleep(1 * time.Second)
		} else if reply.Status == storagerpc.OK {
			ls.storageServers = FlattenServers(reply.Servers)
			break
		}
		retryCnt++
	}

	if retryCnt == 5 {
		return nil, errors.New("storage servers took too long to get ready")
	}

	return ls, nil
}

//
// Flatten the slice of storagerpc.Node to slice of simpleNode, which is order by each virtual ID
//
func FlattenServers(servers []storagerpc.Node) []simpleNode {
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

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	reply := &storagerpc.GetReply{}
	// Request route
	client, requestLease, err := ls.RouteServer(key)
	if err != nil {
		return "", err
	}

	// If lease is requested
	if requestLease {
		args.WantLease = true
	} else {
		// If the libstore instance is caching the value
		ls.mux.Lock()
		if val, ok := ls.stringCache[key]; ok {
			ls.mux.Unlock()
			return val, nil
		}
		ls.mux.Unlock()
	}

	err = client.Call("StorageServer.Get", args, reply)
	if err != nil {
		return "", err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return "", errors.New("KeyNotFound")
	} else if reply.Status != storagerpc.OK {
		return "", errors.New("get fails")
	}

	// If the lease request is submitted and granted
	if requestLease && reply.Lease.Granted {
		ls.mux.Lock()
		defer ls.mux.Unlock()

		ls.stringCache[key] = reply.Value
		ls.leaseStartTime[key] = time.Now().UnixNano()
		ls.leaseValidTime[key] = reply.Lease.ValidSeconds
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	reply := &storagerpc.PutReply{}
	// Request route
	client, _, err := ls.RouteServer(key)
	if err != nil {
		return err
	}
	err = client.Call("StorageServer.Put", args, reply)
	if err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("put fails")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	reply := &storagerpc.DeleteReply{}
	// Request route
	client, _, err := ls.RouteServer(key)
	if err != nil {
		return err
	}
	err = client.Call("StorageServer.Delete", args, reply)
	if err != nil {
		return err
	}

	if reply.Status == storagerpc.KeyNotFound {
		return errors.New("KeyNotFound")
	} else if reply.Status != storagerpc.OK {
		return errors.New("delete fails")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	reply := &storagerpc.GetListReply{}
	// Request route
	client, requestLease, err := ls.RouteServer(key)
	if err != nil {
		return nil, err
	}

	// If lease is requested
	if requestLease {
		args.WantLease = true
	} else {
		// If the libstore instance is caching the value
		ls.mux.Lock()
		if val, ok := ls.listCache[key]; ok {
			ls.mux.Unlock()
			return val, nil
		}
		ls.mux.Unlock()
	}

	err = client.Call("StorageServer.GetList", args, reply)
	if err != nil {
		return nil, err
	}

	if reply.Status != storagerpc.KeyNotFound && reply.Status != storagerpc.OK {
		return nil, errors.New("get list fails")
	} else {
		// If the lease request is submitted and granted
		if requestLease && reply.Lease.Granted {
			ls.mux.Lock()
			defer ls.mux.Unlock()

			ls.listCache[key] = reply.Value
			ls.leaseStartTime[key] = time.Now().UnixNano()
			ls.leaseValidTime[key] = reply.Lease.ValidSeconds
		}
		return reply.Value, nil
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	reply := &storagerpc.PutReply{}
	// Request route
	client, _, err := ls.RouteServer(key)
	if err != nil {
		return err
	}
	err = client.Call("StorageServer.RemoveFromList", args, reply)
	if err != nil {
		return err
	}

	if reply.Status == storagerpc.ItemNotFound {
		return errors.New("ItemNotFound")
	} else if reply.Status != storagerpc.OK {
		return errors.New("remove from list fails")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	reply := &storagerpc.PutReply{}
	// Request route
	client, _, err := ls.RouteServer(key)
	if err != nil {
		return err
	}
	err = client.Call("StorageServer.AppendToList", args, reply)
	if err != nil {
		return err
	}

	if reply.Status == storagerpc.ItemExists {
		return errors.New("ItemExists")
	} else if reply.Status != storagerpc.OK {
		return errors.New("append to list fails")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	if _, ok := ls.leaseValidTime[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		ls.ClearCache(args.Key)
		reply.Status = storagerpc.OK
	}
	return nil
}

//
// Handle timeout revoke for each granted lease
//
func (ls *libstore) TimeoutRevoke(key string, startTime int64, validTime int) {
	timer := time.NewTimer(time.Duration(validTime + storagerpc.LeaseGuardSeconds) * time.Second)
	revokeLeaseFlag := false
	// Hang for a while
	for {
		select {
		case <-timer.C:
			ls.mux.Lock()
			if curStartTime, ok := ls.leaseStartTime[key]; ok {
				if curStartTime == startTime {
					revokeLeaseFlag = true
				}
			}
			ls.mux.Unlock()
			break
		}
	}
	if revokeLeaseFlag {
		ls.mux.Lock()
		defer ls.mux.Unlock()
		ls.ClearCache(key)
	}
}

//
// Given a key (not the hashed key), find the correct server to handle the request
//
func (ls *libstore) RouteServer(key string) (*rpc.Client, bool, error) {
	hashedKey := StoreHash(key)
	var chosenServer = ""

	// Find the proper server to serve this request. Since the virtual IDs are sorted in the storageServers list, break
	// the loop once a virtual ID that is bigger than the hashed key is found
	for _, serverInfo := range ls.storageServers {
		// Need to be <= rather than ==
		if hashedKey <= serverInfo.virtualID {
			chosenServer = serverInfo.hostPort
			break
		}
	}

	// If the hashed key is bigger than every virtual ID, use the first simpleNode to serve this request
	if len(chosenServer) == 0 {
		chosenServer = ls.storageServers[0].hostPort
	}

	ls.mux.Lock()
	defer ls.mux.Unlock()

	// Check whether lease is required
	requestLease := false
	if ls.mode != Never {
		curTimeStamp := time.Now().UnixNano()
		checkLeaseRequirement := true
		// If this key has been cached, check whether it is still valid
		if startTime, ok := ls.leaseStartTime[key]; ok {
			// If the cache is outdated
			// TODO: > or >= here
			if int(curTimeStamp - startTime) > ls.leaseValidTime[key] {
				// Delete the outdated cache
				ls.ClearCache(key)
			} else {
				// If the cache is still valid
				checkLeaseRequirement = false
			}
		}
		// If it is necessary to check the requirement of lease
		if checkLeaseRequirement {
			if ls.mode == Always {
				requestLease = true
			} else if ls.mode == Normal {
				if times, ok := ls.requestTimeWindow[key]; ok {
					// Clear the outdated query in the query sliding window
					idx := 0
					for idx < len(times) {
						// TODO: > or >= here
						if times[idx] + storagerpc.QueryCacheSeconds > curTimeStamp {
							break
						}
						idx++
					}
					ls.requestTimeWindow[key] = times[idx:]
				} else {
					ls.requestTimeWindow[key] = nil
				}
				// If there are enough queries in the past QueryCacheSeconds, request the lease
				if len(ls.requestTimeWindow[key]) >= storagerpc.QueryCacheThresh {
					requestLease = true
				}
				ls.requestTimeWindow[key] = append(ls.requestTimeWindow[key], curTimeStamp)
			}
		} else if ls.mode == Normal {
			if _, ok := ls.requestTimeWindow[key]; !ok {
				ls.requestTimeWindow[key] = nil
			}
			// Do not clear the outdated entry until the cache is invalid
			ls.requestTimeWindow[key] = append(ls.requestTimeWindow[key], curTimeStamp)
		}
	}

	// If this server is connected for the first time, build the connection and cache the client
	if client, ok := ls.hostToClient[chosenServer]; !ok {
		client, err := rpc.DialHTTP("tcp", chosenServer)
		if err != nil {
			return nil, requestLease, err
		} else {
			ls.hostToClient[chosenServer] = client
			return client, requestLease, nil
		}
	} else {
		return client, requestLease, nil
	}
}

//
// Clear the cache of a given key
//
func (ls *libstore) ClearCache(key string) {
	delete(ls.stringCache, key)
	delete(ls.listCache, key)
	delete(ls.leaseStartTime, key)
	delete(ls.leaseValidTime, key)
}
