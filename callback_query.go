package gorm

import (
	"errors"
	"fmt"
	"reflect"
	"crypto/md5"
	"encoding/json"
)

// Define callbacks for querying
func init() {
	DefaultCallback.Query().Register("gorm:query", queryCallback)
	DefaultCallback.Query().Register("gorm:preload", preloadCallback)
	DefaultCallback.Query().Register("gorm:after_query", afterQueryCallback)
}

type SubObject struct {
	isCache bool
	Id      string
}

// queryCallback used to query data from database
func queryCallback(scope *Scope) {
	defer scope.trace(NowFunc())

	var (
		isSlice, isPtr bool
		resultType     reflect.Type
		results        = scope.IndirectValue()
	)

	if orderBy, ok := scope.Get("gorm:order_by_primary_key"); ok {
		if primaryField := scope.PrimaryField(); primaryField != nil {
			scope.Search.Order(fmt.Sprintf("%v.%v %v", scope.QuotedTableName(), scope.Quote(primaryField.DBName), orderBy))
		}
	}

	if value, ok := scope.Get("gorm:query_destination"); ok {
		results = indirect(reflect.ValueOf(value))
	}

	if kind := results.Kind(); kind == reflect.Slice {
		isSlice = true
		resultType = results.Type().Elem()
		results.Set(reflect.MakeSlice(results.Type(), 0, 0))

		if resultType.Kind() == reflect.Ptr {
			isPtr = true
			resultType = resultType.Elem()
		}
	} else if kind != reflect.Struct {
		scope.Err(errors.New("unsupported destination, should be slice or struct"))
		return
	}

	scope.prepareQuerySQL()

	if !scope.HasError() {
		scope.db.RowsAffected = 0
		if str, ok := scope.Get("gorm:query_option"); ok {
			scope.SQL += addExtraSpaceIfExist(fmt.Sprint(str))
		}
		iscache := reflect.ValueOf(scope.Value).Elem().FieldByName("isCache")
		haskey := md5.Sum([]byte(scope.SQL))
		haskeystr := fmt.Sprintf("%x", haskey) //将[]byte转成16进制
		//iscache存在且其值为真，则调用redis缓存逻辑
		if (iscache.IsValid() && iscache.Bool()) {
			//is value exist?
			if (Rds.HExists(scope.TableName(), haskeystr).Val()) {
				//get values from redis
				redisValue, _ := Rds.HGet(scope.TableName(), haskeystr).Bytes()
				json.Unmarshal(redisValue, scope.Value)
				return
			}
		}

		if rows, err := scope.SQLDB().Query(scope.SQL, scope.SQLVars...); scope.Err(err) == nil {
			defer rows.Close()

			columns, _ := rows.Columns()
			for rows.Next() {
				scope.db.RowsAffected++

				elem := results
				if isSlice {
					elem = reflect.New(resultType).Elem()
				}

				scope.scan(rows, columns, scope.New(elem.Addr().Interface()).Fields())

				if isSlice {
					if isPtr {
						results.Set(reflect.Append(results, elem.Addr()))
					} else {
						results.Set(reflect.Append(results, elem))
					}
				}
			}
			if (iscache.IsValid() && iscache.Bool()) {
				//set redis value
				jsonValue, _ := json.Marshal(scope.Value)
				Rds.HSet(scope.TableName(), haskeystr, jsonValue)
				if err := rows.Err(); err != nil {
					scope.Err(err)
				} else if scope.db.RowsAffected == 0 && !isSlice {
					scope.Err(ErrRecordNotFound)
				}
			}
		}
	}
}

// afterQueryCallback will invoke `AfterFind` method after querying
func afterQueryCallback(scope *Scope) {
	if !scope.HasError() {
		scope.CallMethod("AfterFind")
	}
}
