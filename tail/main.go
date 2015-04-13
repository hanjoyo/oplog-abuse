package main

import (
	"fmt"

	"github.com/ianschenck/envflag"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Oplog an individual document from the oplog.rs collection
type Oplog struct {
	Timestamp    bson.MongoTimestamp `bson:"ts"`
	HistoryID    int64               `bson:"h"`
	MongoVersion int                 `bson:"v"`
	Operation    string              `bson:"op"`
	Namespace    string              `bson:"ns"`
	Object       bson.M              `bson:"o"`
	QueryObject  bson.M              `bson:"o2"`
}

var (
	mongoURL = envflag.String("MONGO_URL", "mongodb://localhost", "mongodb url to connect to")
)

// LatestOplog returns the most recent oplog from the database
func latestOplog(sess *mgo.Session) (Oplog, error) {
	var oplog Oplog
	err := sess.DB("local").C("oplog.rs").Find(nil).Sort("-$natural").One(&oplog)
	return oplog, err
}

func main() {
	envflag.Parse()
	sess, err := mgo.Dial(*mongoURL)
	if err != nil {
		panic(err)
	}

	// need last oplog timestamp to make tailing query
	lo, err := latestOplog(sess)
	if err != nil {
		panic(err)
	}

	iter := sess.DB("local").
		C("oplog.rs").
		Find(bson.M{"ts": bson.M{"$gte": lo.Timestamp}}). // can filter the query even more: certain ns or operations
		Sort("$natural").
		LogReplay().
		Tail(-1) // tail forever

	var oplog Oplog
	for iter.Next(&oplog) {
		fmt.Printf("%+v\n", oplog)
	}
	err = iter.Err()
	if err != nil {
		panic(err)
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}
}
