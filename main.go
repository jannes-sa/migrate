package migrate

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"reflect"
	"strings"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
)

// DoMigrate ...
func DoMigrate(tableName string, status string, connection string, st interface{}) {
	db, err := sql.Open("voltdb", connection)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if status == "migrate" {
		stateTableExists := true

		ckTableQry := `select 1 from ` + tableName + ` limit 1`
		stmt, _ := db.Prepare(ckTableQry)
		_, err := stmt.Query()
		if err != nil {
			stateTableExists = false
		}

		if !stateTableExists {
			prependQueryMigrate(connection, status, tableName, st)
		} else {
			log.Println(`TABLE ` + tableName + ` ALREADY EXISTS CAN'T MIGRATE`)
		}
	} else if status == "update" {
		stateTableExists := false

		ckTableQry := `select 1 from ` + tableName + ` limit 1`
		stmt, _ := db.Prepare(ckTableQry)
		_, err := stmt.Query()
		if err == nil {
			stateTableExists = true
		}

		if stateTableExists {
			prependQueryUpdate(connection, status, tableName, st)
		} else {
			log.Println(`TABLE ` + tableName + ` NOT EXISTS CAN'T UPDATE`)
		}
	} else if status == "drop" {
		dropTable(connection, tableName)
	}
}

////////////////// UPDATE //////////////////////////
func prependQueryUpdate(connection, status string, tableName string, st interface{}) {
	conn, err := voltdbclient.OpenConn(connection)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	v := reflect.ValueOf(st)
	vt := v.Type()

	if status == "update" {
		for i, n := 0, v.NumField(); i < n; i++ {
			var uq string
			ft := vt.Field(i)

			field := ft.Tag.Get("field")
			types := ft.Tag.Get("type")
			defaultd := ft.Tag.Get("default")

			attr := ft.Tag.Get("attr")
			attrs := strings.Split(attr, ",")

			containPer(attrs, "uq", field, &uq)

			qry := `alter table ` + tableName + ` alter column ` + field + ` ` + types
			if defaultd != "" {
				qry += ` set default ` + defaultd
			}

			_, err := conn.Exec("@AdHoc", []driver.Value{qry})
			if err != nil {
				log.Println("Failed Updating Alter Table")
				log.Fatal(err)
			} else {
				log.Println(`alter updating ` + field)
				log.Println(qry)
				log.Println("########")
			}

			if uq != "" {
				stateUnique := createUniqueIndexIgnore(connection, tableName, uq)
				if stateUnique {
					log.Println(".........................")
					log.Println("INDEX UNIQUE ====> " + uq)
					log.Println(".........................")
				}
			}
		}
	} else {
		log.Fatal("FAILED TRACK CHECK 2")
	}
}

////////////////// UPDATE //////////////

////////////////// MIGRATE ////////////////////////////
func prependQueryMigrate(connection, status string, tableName string, st interface{}) {
	conn, err := voltdbclient.OpenConn(connection)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	v := reflect.ValueOf(st)
	vt := v.Type()

	if status == "migrate" {
		var query string

		var arrQry []string

		var pk []string
		var uq []string

		for i, n := 0, v.NumField(); i < n; i++ {
			ft := vt.Field(i)

			field := ft.Tag.Get("field")
			types := ft.Tag.Get("type")
			defaultd := ft.Tag.Get("default")

			attr := ft.Tag.Get("attr")
			attrs := strings.Split(attr, ",")

			contain(attrs, "pk", field, &pk)
			contain(attrs, "uq", field, &uq)

			pendQuery := field + ` ` + types

			if defaultd != "" {
				pendQuery += ` DEFAULT ` + defaultd
			}

			pendQuery += ` NOT NULL`

			arrQry = append(arrQry, pendQuery)
		}

		if len(pk) > 0 {
			strJoinPk := strings.Join(pk, ",")
			pkQry := `PRIMARY KEY(` + strJoinPk + `)`
			arrQry = append(arrQry, pkQry)
		}

		qry := `CREATE TABLE ` + tableName + ` (` + strings.Join(arrQry, ",") + `)`
		query = qry
		// log.Println(qry)

		if query != "" {
			_, err := conn.Exec("@AdHoc", []driver.Value{query})
			if err != nil {
				log.Fatal(err)
			} else {
				if len(uq) > 0 {
					stateUnique := createUniqueIndex(connection, tableName, uq)
					if !stateUnique {
						dropTable(connection, tableName)
					} else {
						printRows(tableName, arrQry, pk, uq)
					}
				} else {
					printRows(tableName, arrQry, pk, uq)
				}
			}
		} else {
			log.Println("No Migrate Affected")
		}
	} else {
		log.Fatal("FAILED TRACK CHECK 1")
	}
}

func printRows(tableName string, arrQry []string, pk []string, uq []string) {
	log.Println("MIGRATE " + tableName)
	for _, v := range arrQry {
		log.Println(`=======>` + v)
	}

	if len(pk) > 0 {
		log.Println("PRIMARY KEY")
		for _, v := range pk {
			log.Println(`=======>` + v)
		}
	}

	if len(uq) > 0 {
		log.Println("INDEX UNIQUE")
		for _, v := range uq {
			log.Println(`=======>` + v)
		}
	}
}

////////////////// MIGRATE ////////////////////////////

func createUniqueIndex(connection string, tableName string, field []string) bool {
	stateUnique := false

	conn, err := voltdbclient.OpenConn(connection)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	strJoinField := strings.Join(field, ",")
	nmIndex := strings.Join(field, "_") + "_idx"

	qry := `create unique index ` + nmIndex + ` ON ` + tableName + ` (` + strJoinField + `)`
	_, err = conn.Exec("@AdHoc", []driver.Value{qry})
	if err != nil {
		log.Println(`Failed Create Unique Index ` + strJoinField)
		log.Fatal(err)
	} else {
		stateUnique = true
	}

	return stateUnique
}

func createUniqueIndexIgnore(connection string, tableName string, field string) bool {
	stateUnique := false

	conn, err := voltdbclient.OpenConn(connection)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	nmIndex := field + `_idx`
	qry := `create unique index ` + nmIndex + ` ON ` + tableName + ` (` + field + `)`
	_, err = conn.Exec("@AdHoc", []driver.Value{qry})
	if err != nil {
		log.Println(`Failed Create Unique Index ` + field)
		log.Println(err)
	} else {
		stateUnique = true
	}

	return stateUnique
}

func dropTable(connection string, tableName string) {
	conn, err := voltdbclient.OpenConn(connection)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec("@AdHoc", []driver.Value{`drop table ` + tableName})
	if err != nil {
		log.Println(`Failed Drop Table ` + tableName)
		log.Fatal(err)
	} else {
		log.Println(`drop table ` + tableName)
	}
}

func contain(attrs []string, vals string, fieldName string, assignAttrs *[]string) {
	for _, val := range attrs {
		if val == vals {
			*assignAttrs = append(*assignAttrs, fieldName)
		}
	}
}

func containPer(attrs []string, vals string, fieldName string, assignAttrs *string) {
	for _, val := range attrs {
		if val == vals {
			*assignAttrs = fieldName
		}
	}
}
