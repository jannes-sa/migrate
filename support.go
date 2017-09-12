package migrate

import (
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/astaxie/beego"
)

// GetMaterialProc ...
func GetMaterialProc(structs interface{}) {
	beego.Warn("#####################################################")
	v := reflect.ValueOf(structs)

	var javaTyping []string
	var staticTyping []string
	for i, n := 0, v.NumField(); i < n; i++ {
		types := v.Type().Field(i).Tag.Get("type")
		nmVal := v.Type().Field(i).Tag.Get("json")

		javaTyping = append(javaTyping, GetContainTypeJava(types, nmVal))
		staticTyping = append(staticTyping, nmVal)
	}

	beego.Debug(`NumField ` + strconv.Itoa(v.NumField()))
	beego.Debug(strings.Join(javaTyping, ","))
	log.Println("===========================")
	beego.Debug(strings.Join(staticTyping, ","))
	beego.Warn("#####################################################")
}

// GetContainTypeJava ...
func GetContainTypeJava(types string, nmVal string) string {
	var returnVal string
	if strings.Index(types, "varchar") > -1 {
		returnVal = "String"
	} else if types == "int" {
		returnVal = "int"
	} else if types == "bigint" || types == "timestamp" {
		returnVal = "long"
	} else if types == "float" {
		returnVal = "double"
	} else if types == "tinyint" {
		returnVal = "byte"
	}

	returnVal = returnVal + " " + nmVal
	return returnVal
}
