package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/gonum/stat"
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

type Datapoint struct {
	At    time.Time `bson:"at"`
	Value float64   `bson:"value"`
}

type Raw struct {
	Key    string      `bson:"key"`
	At     int64       `bson:"at"`
	Values []Datapoint `bson:"values"`
}

// http://en.wikipedia.org/wiki/Seven-number_summary
type Summary struct {
	Key string  `bson:"key"`
	At  int64   `bson:"at"`
	Min float64 `bson:"min"`
	Max float64 `bson:"max"`
	P2  float64 `bson:"p2"`
	P9  float64 `bson:"p9"`
	P25 float64 `bson:"p25"`
	P50 float64 `bson:"p50"`
	P75 float64 `bson:"p75"`
	P91 float64 `bson:"p91"`
	P98 float64 `bson:"p98"`
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

func oplogCh(sess *mgo.Session, query bson.M) (<-chan Oplog, <-chan error) {
	out := make(chan Oplog)
	errc := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			errc <- err
			close(errc)
		}()
		defer close(out)
		iter := sess.DB("local").
			C("oplog.rs").
			Find(query).
			Sort("$natural").
			LogReplay().
			Tail(-1) // tail forever
		var oplog Oplog
		for iter.Next(&oplog) {
			out <- oplog
		}
		err = iter.Err()
		if err != nil {
			return
		}
		err = iter.Close()
	}()
	return out, errc
}

// assumes the oplog will be modifying a default "_id" field that is an
// ObjectID type. Returns the string representation of oplog ObjectIDs being
// either inserted or updated.
func oidCh(in <-chan Oplog) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		for o := range in {
			if o.Operation == "i" {
				if id, ok := o.Object["_id"]; ok {
					if boid, ok := id.(bson.ObjectId); ok {
						out <- boid.Hex()
					}
				}
			}
			if o.Operation == "u" {
				if id, ok := o.QueryObject["_id"]; ok {
					if boid, ok := id.(bson.ObjectId); ok {
						out <- boid.Hex()
					}
				}
			}
		}
	}()
	return out
}

func rawToSummary(raw Raw) (summary Summary) {
	summary.Key = raw.Key
	summary.At = raw.At
	values := make([]float64, len(raw.Values), len(raw.Values))
	for i, value := range raw.Values {
		values[i] = value.Value
	}
	sort.Float64s(values)
	summary.Min = stat.Quantile(0, stat.Empirical, values, nil)
	summary.Max = stat.Quantile(1, stat.Empirical, values, nil)
	summary.P2 = stat.Quantile(0.02, stat.Empirical, values, nil)
	summary.P9 = stat.Quantile(0.09, stat.Empirical, values, nil)
	summary.P25 = stat.Quantile(0.25, stat.Empirical, values, nil)
	summary.P50 = stat.Quantile(0.50, stat.Empirical, values, nil)
	summary.P75 = stat.Quantile(0.75, stat.Empirical, values, nil)
	summary.P91 = stat.Quantile(0.91, stat.Empirical, values, nil)
	summary.P98 = stat.Quantile(0.98, stat.Empirical, values, nil)
	return
}

func stats(sess *mgo.Session, oid string) error {
	// get raw object
	var raw Raw
	err := sess.DB("metrics").C("raw").Find(bson.M{"_id": bson.ObjectIdHex(oid)}).One(&raw)
	if err != nil {
		return err
	}
	summary := rawToSummary(raw)
	selector := bson.M{"key": summary.Key, "at": summary.At}
	// update := bson.M{"min": summary.Min, "max": summary.Max, "p95": summary.P95}
	_, err = sess.DB("metrics").C("summary").Upsert(selector, summary)
	fmt.Printf("%+v\n", raw)
	return err
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

	query := bson.M{
		"ts": bson.M{
			"$gt": lo.Timestamp,
		},
		"ns": "metrics.raw",
		"op": bson.M{
			"$in": []string{"i", "u"},
		},
	}
	och, errCh := oplogCh(sess, query)
	oidch := oidCh(och)
	for oid := range oidch {
		fmt.Printf("got oid: %s\n", oid)
		err = stats(sess, oid)
		if err != nil {
			panic(err)
		}
	}
	err = <-errCh
	if err != nil {
		panic(err)
	}
}
