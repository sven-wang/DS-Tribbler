package tribserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	// TODO: implement this!
	myLibstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tribServer := new(tribServer)
	// TODO: Change lease level for final checkpoint
	libstoreInstance, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}
	tribServer.myLibstore = libstoreInstance

	addr := strings.Split(myHostPort, ":") // addr = (ip, port)
	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", addr[1]))
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

//
// Check whether the user exists or not
//
func (ts *tribServer) checkUserExistence(userID string) (bool, error) {
	// Format user key
	userKey := util.FormatUserKey(userID)
	// Do a Get on the Libstore instance to check duplicate key
	val, err := ts.myLibstore.Get(userKey)
	if err != nil {
		return false, err
	}
	exist := false
	// TODO: How to determine if a key exists?
	if len(val) > 0 {
		exist = true
	}
	return exist, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	exist, err := ts.checkUserExistence(args.UserID)
	if err != nil {
		return err
	}
	// If the user exists
	if exist {
		reply.Status = tribrpc.Exists
		return nil
	}
	// TODO: Check what parameters are supposed to put there
	err = ts.myLibstore.Put(args.UserID, args.UserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	exist, err := ts.checkUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	exist, err = ts.checkUserExistence(args.TargetUserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// Format the fromUser's subscription key
	userSubListKey := util.FormatSubListKey(args.UserID)
	err = ts.myLibstore.AppendToList(userSubListKey, args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	exist, err := ts.checkUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	exist, err = ts.checkUserExistence(args.TargetUserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// Format the fromUser's subscription key
	userSubListKey := util.FormatSubListKey(args.UserID)
	err = ts.myLibstore.RemoveFromList(userSubListKey, args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	exist, err := ts.checkUserExistence(args.UserID)
	if err != nil {
		return err
	}
	// If the user does not exist
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Format the user's subscription key
	userSubListKey := util.FormatSubListKey(args.UserID)
	subList, err := ts.myLibstore.GetList(userSubListKey)
	if err != nil {
		return err
	}
	var friendList []string
	for _, followingUserID := range subList {
		// Check whether the user is also subscribed by his or her subscribed user
		followingUserSubListKey := util.FormatSubListKey(followingUserID)
		followingUserSubList, err := ts.myLibstore.GetList(followingUserSubListKey)
		if err != nil {
			return err
		}
		if checkExistence(followingUserSubList, args.UserID) {
			friendList = append(friendList, followingUserID)
		}
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = friendList
	return nil
}

//
// Check whether a user is in a given subscription list
//
func checkExistence(subList []string, targetUser string) bool {
	for _, userID := range subList {
		if userID == targetUser {
			return true
		}
	}
	return false
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	exist, err := ts.checkUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	timestamp := time.Now().UnixNano()
	tribListKey := util.FormatTribListKey(args.UserID)
	postKey := util.FormatPostKey(args.UserID, timestamp)

	return errors.New("not implemented")
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
