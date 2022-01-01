package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	DB_Name = "test"
)

const maxBulkSizeAllowed = 1000 // max bulk size allowed by server
type Option struct {
	Sort   []string
	Limit  *int
	Offset *int
	Select bson.M
}

var globalSession *mgo.Session
var exportSession *mgo.Session
var warningMongoQueryDuration = time.Millisecond * 500
var initMutex sync.Mutex

func Init() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if globalSession != nil {
		return
	}
	mongdbUrl := "127.0.0.1"
	session, err := mgo.Dial(mongdbUrl)
	if err != nil {
		panic(err)
	}
	globalSession = session
}

func NewSession() *mgo.Session {
	if globalSession == nil {
		Init()
	}
	return globalSession.Copy()
}

func NewExportSession() *mgo.Session {
	if exportSession == nil {
		exportSession = NewSession()
		exportSession.SetMode(mgo.Monotonic, true)
	}
	return exportSession.Copy()
}

type Session struct {
	*mgo.Session
	c context.Context
}

func Get(c context.Context) *Session {
	if globalSession == nil {
		Init()
	}
	return &Session{
		globalSession.Copy(),
		c,
	}
}

func GetExport(c context.Context) *Session {
	return &Session{
		NewExportSession(),
		c,
	}
}

func (s *Session) C(name string) *mgo.Collection {
	return s.DB(DB_Name).C(name)
}

func (s *Session) Find(collection string, query interface{}, result interface{}) error {
	w := watchFind(s.c, collection, query)
	defer w.Done()
	return s.C(collection).Find(query).All(result)
}

func (s *Session) MustFind(collection string, query interface{}, result interface{}) {
	if err := s.Find(collection, query, result); err != nil {
		panic(err)
	}
}

func (s *Session) FindIter(collection string, query interface{}) *mgo.Iter {
	return s.C(collection).Find(query).Iter()
}

func (s *Session) FindIterSort(collection string, query interface{}, sort []string) *mgo.Iter {
	return s.C(collection).Find(query).Sort(sort...).Iter()
}

func (s *Session) FindId(collectionName string, id interface{}, result interface{}) error {
	w := watchFindId(s.c, collectionName, id)
	defer w.Done()
	return s.C(collectionName).FindId(id).One(result)
}

func (s *Session) MustFindId(collection string, query interface{}, result interface{}) {
	if err := s.FindId(collection, query, result); err != nil {
		panic(err)
	}
}

func (s *Session) FindIdOne(collectionName string, id interface{}, result interface{}) bool {
	w := watchFindId(s.c, collectionName, id)
	defer w.Done()
	if err := s.C(collectionName).FindId(id).One(result); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}

		return false
	}

	return true
}

func (s *Session) FindOne(collectionName string, query interface{}, result interface{}) bool {
	w := watchFind(s.c, collectionName, query)
	defer w.Done()
	if err := s.C(collectionName).Find(query).One(result); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}

		return false
	}

	return true
}

func (s *Session) FindAll(collectionName string, query interface{}, result interface{}) {
	w := watchFind(s.c, collectionName, query)
	defer w.Done()
	if err := s.C(collectionName).Find(query).All(result); err != nil {
		panic(err)
	}
}

func (s *Session) FindCount(collectionName string, query interface{}) int {
	w := watchFind(s.c, collectionName, query)
	defer w.Done()
	if n, err := s.C(collectionName).Find(query).Count(); err != nil {
		panic(err)
	} else {
		return n
	}
}

func (s *Session) FindIdApply(collectionName string, id interface{}, change mgo.Change, result interface{}) (*mgo.ChangeInfo, bool) {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	if info, err := s.C(collectionName).FindId(id).Apply(change, result); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}

		return nil, false
	} else {
		return info, true
	}
}

func (s *Session) MustFindIdApply(collectionName string, id interface{}, change mgo.Change, result interface{}) *mgo.ChangeInfo {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	if info, err := s.C(collectionName).FindId(id).Apply(change, result); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}

		panic("record not found")
	} else {
		return info
	}
}

func (s *Session) FindApply(collectionName string, query interface{}, change mgo.Change, result interface{}) (*mgo.ChangeInfo, bool) {
	w := watchUpdate(s.c, collectionName, query)
	defer w.Done()
	if info, err := s.C(collectionName).Find(query).Apply(change, result); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}

		return nil, false
	} else {
		return info, true
	}
}

func (s *Session) FindWithOptions(collection string, query interface{}, options Option, result interface{}) error {
	w := watchFind(s.c, collection, query)
	defer w.Done()
	q := s.C(collection).Find(query)
	if len(options.Sort) > 0 {
		q = q.Sort(options.Sort...)
	}
	if options.Offset != nil {
		q = q.Skip(*options.Offset)
	}
	if options.Limit != nil {
		q = q.Limit(*options.Limit)
	}
	if len(options.Select) != 0 {
		q = q.Select(options.Select)
	}
	return q.All(result)
}

func (s *Session) MustFindWithOptions(collection string, query interface{}, options Option, result interface{}) {
	if err := s.FindWithOptions(collection, query, options, result); err != nil {
		panic(err)
	}
}

func (s *Session) FindDistinct(collection string, query interface{}, key string, result interface{}) error {
	w := watchFind(s.c, collection, query)
	defer w.Done()
	return s.C(collection).Find(query).Distinct(key, result)
}

func (s *Session) MustFindDistinct(collection string, query interface{}, key string, result interface{}) {
	if err := s.FindDistinct(collection, query, key, result); err != nil {
		panic(err)
	}
}

func (s *Session) Insert(collectionName string, docs ...interface{}) error {
	w := watchInsert(s.c, collectionName)
	defer w.Done()
	return s.C(collectionName).Insert(docs...)
}

func (s *Session) MustInsert(collectionName string, docs ...interface{}) {
	if err := s.Insert(collectionName, docs...); err != nil {
		panic(err)
	}
}

func (s *Session) UpdateId(collectionName string, id interface{}, update interface{}) error {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	return s.C(collectionName).UpdateId(id, update)
}

func (s *Session) MustUpdateId(collectionName string, id interface{}, update interface{}) {
	if err := s.C(collectionName).UpdateId(id, update); err != nil {
		panic(err)
	}
}

func (s *Session) MustUpdate(collectionName string, query interface{}, update interface{}) {
	if err := s.C(collectionName).Update(query, update); err != nil {
		panic(err)
	}
}

func (s *Session) Update(collectionName string, query interface{}, update interface{}) error {
	if err := s.C(collectionName).Update(query, update); err != nil {
		return err
	}

	return nil
}



func (s *Session) Upsert(collectionName string, query interface{}, update interface{}) error {
	w := watchUpdate(s.c, collectionName, query)
	defer w.Done()
	_, err := s.C(collectionName).Upsert(query, update)
	return err
}

func (s *Session) MustUpsert(collectionName string, query interface{}, update interface{}) {
	if err := s.Upsert(collectionName, query, update); err != nil {
		panic(err)
	}
}

func (s *Session) UpsertId(collectionName string, id interface{}, update interface{}) error {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	_, err := s.C(collectionName).UpsertId(id, update)
	return err
}

func (s *Session) MustUpsertId(collectionName string, id interface{}, update interface{}) {
	if err := s.UpsertId(collectionName, id, update); err != nil {
		panic(err)
	}
}

func (s *Session) UpdateAll(collectionName string, query interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	w := watchUpdate(s.c, collectionName, query)
	defer w.Done()
	return s.C(collectionName).UpdateAll(query, update)
}

func (s *Session) MustUpdateAll(collectionName string, query interface{}, update interface{}) *mgo.ChangeInfo {
	if res, err := s.UpdateAll(collectionName, query, update); err != nil {
		panic(err)
	} else {
		return res
	}
}

func (s *Session) PartialUpdateId(collectionName string, id interface{}, update interface{}) error {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	return s.C(collectionName).UpdateId(id, bson.M{"$set": update})
}

func (s *Session) MustPartialUpdateId(collectionName string, id interface{}, update interface{}) {
	if err := s.PartialUpdateId(collectionName, id, update); err != nil {
		panic(err)
	}
}

func (s *Session) RemoveId(collectionName string, id interface{}) {
	w := watchRemoveId(s.c, collectionName, id)
	defer w.Done()
	if err := s.C(collectionName).RemoveId(id); err != nil {
		panic(err)
	}
}

func (s *Session) Remove(collectionName string, selector interface{}) {
	w := watchRemove(s.c, collectionName, selector)
	defer w.Done()
	if err := s.C(collectionName).Remove(selector); err != nil {
		if err != mgo.ErrNotFound {
			panic(err)
		}
	}
}

func (s *Session) RemoveAll(collectionName string, selector interface{}) (info *mgo.ChangeInfo, err error) {
	w := watchRemove(s.c, collectionName, selector)
	defer w.Done()
	return s.C(collectionName).RemoveAll(selector)
}

func (s *Session) MustRemoveAll(collectionName string, selector interface{}) *mgo.ChangeInfo {
	if info, err := s.RemoveAll(collectionName, selector); err != nil {
		panic(err)
	} else {
		return info
	}
}


func (s *Session) UnarchiveId(collectionName string, id interface{}) error {
	w := watchUpdateId(s.c, collectionName, id)
	defer w.Done()
	update := bson.M{
		"$set": bson.M{"deletedTime": nil},
	}
	return s.UpdateId(collectionName, id, update)
}

func (s *Session) MustUnarchiveId(collectionName string, id interface{}) {
	if err := s.UnarchiveId(collectionName, id); err != nil {
		panic(err)
	}
}

func (s *Session) PipeOne(collection string, pipeline []bson.M, result interface{}) error {
	w := watchPipe(s.c, collection, pipeline)
	defer w.Done()
	return s.C(collection).Pipe(pipeline).One(result)
}

func (s *Session) MustPipeOne(collection string, pipeline []bson.M, result interface{}) {
	if err := s.PipeOne(collection, pipeline, result); err != nil {
		panic(err)
	}
}

func (s *Session) PipeAll(collection string, pipeline []bson.M, result interface{}) error {
	w := watchPipe(s.c, collection, pipeline)
	defer w.Done()
	return s.C(collection).Pipe(pipeline).All(result)
}

func (s *Session) MustPipeAll(collection string, pipeline []bson.M, result interface{}) {
	if err := s.PipeAll(collection, pipeline, result); err != nil {
		panic(err)
	}
}

func (s *Session) MustDeleteId(collection string, id interface{}) {
	if err := s.DeleteId(collection, id); err != nil {
		panic(err)
	}
}

func (s *Session) Delete(collection string, query interface{}) (info *mgo.ChangeInfo, err error) {
	w := watchRemove(s.c, collection, query)
	defer w.Done()
	return s.C(collection).RemoveAll(query)
}

func (s *Session) DeleteId(collection string, id interface{}) error {
	w := watchRemoveId(s.c, collection, id)
	defer w.Done()
	return s.C(collection).RemoveId(id)
}

func (s *Session) Count(collection string, query interface{}) (int, error) {
	return s.C(collection).Find(query).Count()
}

func (s *Session) MustCount(collection string, query interface{}) int {
	if result, err := s.Count(collection, query); err != nil {
		panic(err)
	} else {
		return result
	}
}

func (s *Session) IterAll(collection string, query interface{}, iterFunc interface{}) {
	v := reflect.ValueOf(iterFunc)
	if v.Kind() != reflect.Func {
		panic("iterFunc is not a function")
	}
	fType := reflect.TypeOf(iterFunc)

	if fType.NumIn() == 0 || fType.In(0).Kind() != reflect.Ptr {
		panic("first argument of iterFunc is not pointer")
	}

	row := reflect.New(fType.In(0).Elem())
	iter := s.FindIter(collection, query)
	defer iter.Close()
	for {
		ok := iter.Next(row.Interface())
		if !ok {
			if err := iter.Err(); err != nil {
				panic(err)
			}
			break
		}
		v.Call([]reflect.Value{row})
	}
}


func watchFind(c context.Context, collection string, query interface{}) *mongoWatch {
	return newWatch(c, collection, "find").Query(query)
}

func watchFindId(c context.Context, collection string, id interface{}) *mongoWatch {
	return newWatch(c, collection, "findById").Id(id)
}

func watchUpdateId(c context.Context, collection string, id interface{}) *mongoWatch {
	return newWatch(c, collection, "updateById").Id(id)
}

func watchUpdate(c context.Context, collection string, query interface{}) *mongoWatch {
	return newWatch(c, collection, "update").Query(query)
}

func watchInsert(c context.Context, collection string) *mongoWatch {
	return newWatch(c, collection, "insert")
}

func watchBulkUpdate(c context.Context, collection string, count int) *mongoWatch {
	return newWatch(c, collection, "bulkUpdate").BulkCount(count)
}

func watchBulkUpsert(c context.Context, collection string, count int) *mongoWatch {
	return newWatch(c, collection, "bulkUpsert").BulkCount(count)
}

func watchRemoveId(c context.Context, collection string, id interface{}) *mongoWatch {
	return newWatch(c, collection, "removeById").Id(id)
}

func watchRemove(c context.Context, collection string, query interface{}) *mongoWatch {
	return newWatch(c, collection, "remove").Query(query)
}

func watchPipe(c context.Context, collection string, query interface{}) *mongoWatch {
	return newWatch(c, collection, "pipe").Query(query)
}

type mongoWatch struct {
	c          context.Context
	collection string
	operation  string
	params     map[string]interface{}
	start      time.Time
}

func newWatch(c context.Context, collection string, operation string) *mongoWatch {
	return &mongoWatch{
		c:          c,
		collection: collection,
		operation:  operation,
		start:      time.Now(),
		params:     map[string]interface{}{},
	}
}

func (w *mongoWatch) Query(query interface{}) *mongoWatch {
	content, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}
	w.params["query"] = string(content)
	return w
}

func (w *mongoWatch) Id(id interface{}) *mongoWatch {
	return w.Query(bson.M{"_id": id})
}

func (w *mongoWatch) BulkCount(count int) *mongoWatch {
	w.params["bulkCount"] = count
	return w
}

func (w *mongoWatch) Done() {
	duration := time.Now().Sub(w.start)
	if duration >= warningMongoQueryDuration {
		params := []string{}
		for k, v := range w.params {
			params = append(params, fmt.Sprintf("%s=%v", k, v))
		}
	}
}

