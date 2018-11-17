package libstore

import (
	"errors"
	"net"
	"net/rpc"
	"strings"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

// The Libstore instance uses an 'rpc.Client' in order to perform RPCs to the
// Master StorageServer.
type libstore struct {
	// TODO: Add hash table as the cache here for final checkpoint
	msClient       *rpc.Client       // RPC client to the Master StorageServer
	storageServers []storagerpc.Node // List of storage servers that are in the hash ring
	mode           LeaseMode         // Debugging flag
	myHostPort     string
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
	ls.mode = mode
	ls.myHostPort = myHostPort

	// Contact the master server to know a list of available storage servers via GetServers RPC
	serverPort := strings.Split(myHostPort, ":")[1]
	client, err := rpc.DialHTTP("tcp", net.JoinHostPort(masterServerHostPort, serverPort))
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
			ls.storageServers = reply.Servers
			break
		}
		retryCnt++
	}

	if retryCnt == 5 {
		return nil, errors.New("storage servers took too long to get ready")
	}

	// TODO: anything else?
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	reply := &storagerpc.GetReply{}
	err := ls.msClient.Call("StorageServer.Get", args, reply)
	if err != nil {
		return "", err
	}
	if reply.Status == storagerpc.KeyNotFound {
		err = errors.New("KeyNotFound")
		return "", err
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	return errors.New("not implemented")
}

func (ls *libstore) Delete(key string) error {
	return errors.New("not implemented")
}

func (ls *libstore) GetList(key string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
