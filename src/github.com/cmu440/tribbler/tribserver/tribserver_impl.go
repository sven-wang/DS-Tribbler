package tribserver

import (
	"encoding/json"
	"github.com/cmu440/tribbler/util"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	myLibstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := new(tribServer)
	libStoreInstance, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}
	ts.myLibstore = libStoreInstance

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ts, nil
}

//
// Check whether the user exists or not
//
func (ts *tribServer) CheckUserExistence(userID string) (bool, error) {
	// Format user key
	userKey := util.FormatUserKey(userID)
	// Do a Get on the Libstore instance to check duplicate key
	_, err := ts.myLibstore.Get(userKey)
	if err != nil {
		// TODO: How to determine if a key exists?
		if err.Error() != "KeyNotFound" {
			return false, err
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	// If the user exists
	if exist {
		reply.Status = tribrpc.Exists
		return nil
	}

	userKey := util.FormatUserKey(args.UserID)
	err = ts.myLibstore.Put(userKey, args.UserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	exist, err = ts.CheckUserExistence(args.TargetUserID)
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
		if err.Error() != "ItemExists" {
			return err
		} else {
			reply.Status = tribrpc.Exists
		}
	} else {
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	exist, err = ts.CheckUserExistence(args.TargetUserID)
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
		if err.Error() != "ItemNotFound" {
			return err
		} else {
			reply.Status = tribrpc.NoSuchTargetUser
		}
	} else {
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
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
		if CheckExistence(followingUserSubList, args.UserID) {
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
func CheckExistence(subList []string, targetUser string) bool {
	for _, userID := range subList {
		if userID == targetUser {
			return true
		}
	}
	return false
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	timestamp := time.Now()
	tribListKey := util.FormatTribListKey(args.UserID)
	postKey := util.FormatPostKey(args.UserID, timestamp.UnixNano())

	tribble := tribrpc.Tribble{UserID: args.UserID, Contents: args.Contents, Posted: timestamp}
	// Storage design:
	// userPostKey -> tribbleIDList (list of postKeys)
	// postKey -> Tribble
	bytes, _ := json.Marshal(tribble)
	err = ts.myLibstore.Put(postKey, string(bytes))
	if err != nil {
		return err
	}
	err = ts.myLibstore.AppendToList(tribListKey, postKey)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	reply.PostKey = postKey
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Check whether the post exists or not
	_, err = ts.myLibstore.Get(args.PostKey)
	if err != nil {
		if err.Error() != "KeyNotFound" {
			return err
		} else {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}
	}

	err = ts.myLibstore.Delete(args.PostKey)
	if err != nil {
		if err.Error() != "KeyNotFound" {
			return err
		} else {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}
	}

	userTribListKey := util.FormatTribListKey(args.UserID)
	err = ts.myLibstore.RemoveFromList(userTribListKey, args.PostKey)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userTribListKey := util.FormatTribListKey(args.UserID)
	tribList, err := ts.myLibstore.GetList(userTribListKey)
	if err != nil {
		return err
	}

	reply.Tribbles = ts.GetTribblesByList(args.UserID, tribList)
	reply.Status = tribrpc.OK
	return nil
}

//
// Get the most recent 100 Tribbles from a given Tribble list
//
func (ts *tribServer) GetTribblesByList(userID string, tribList []string) []tribrpc.Tribble {
	var returnList []tribrpc.Tribble
	for i := len(tribList) - 1; i >= 0; i-- {
		curPostKey := tribList[i]
		data, err := ts.myLibstore.Get(curPostKey)
		// If you get a non-nil error when you call Get to get some particular Tribble, you can
		// ignore this Tribble
		if err != nil {
			continue
		}

		// Build and append the Tribble to the reply list
		tribble := tribrpc.Tribble{}
		json.Unmarshal([]byte(data), &tribble)
		returnList = append(returnList, tribble)
	}

	// Sort the returnList based on post timestamp (reverse chronological order)
	sort.Slice(returnList, func(i, j int) bool {
		return returnList[i].Posted.After(returnList[j].Posted)
	})
	// Retrieve a list of at most 100 Tribbles
	if len(returnList) > 0 {
		return returnList[:int(math.Min(float64(len(returnList)), 100))]
	} else {
		return nil
	}
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	exist, err := ts.CheckUserExistence(args.UserID)
	if err != nil {
		return err
	}
	if !exist {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(args.UserID)
	subList, err := ts.myLibstore.GetList(userSubListKey)
	if err != nil {
		return err
	}

	var returnList []tribrpc.Tribble
	for _, followingUserID := range subList {
		followingUserTribListKey := util.FormatTribListKey(followingUserID)
		curTribList, err := ts.myLibstore.GetList(followingUserTribListKey)
		// If you get a non-nil error when you call GetList to get some TribbleList for a particular
		// user who is subscribed by the given user, you can ignore this user and proceed to the next
		// one.
		if err != nil {
			continue
		}
		returnList = append(returnList, ts.GetTribblesByList(followingUserID, curTribList)...)
	}

	reply.Status = tribrpc.OK
	if len(returnList) > 0 {
		// Sort the returnList based on post timestamp (reverse chronological order)
		sort.Slice(returnList, func(i, j int) bool {
			return returnList[i].Posted.After(returnList[j].Posted)
		})
		reply.Tribbles = returnList[:int(math.Min(float64(len(returnList)), 100))]
	}
	return nil
}
