package migrate

import (
	"log"
	"os"
	"os/exec"
	"time"
)

var gopath = os.Getenv("GOPATH")
var procPath = gopath + `/src/txn/models/voltdb/proc/`
var compilerPath = gopath + `/src/txn/thirdparty/voltdb/voltdb/*`
var voltdb = gopath + `/src/txn/thirdparty/voltdb/`

// PROCInitStruct ...
type PROCInitStruct struct {
	Procedure string
	Table     string
	Partition string
}

// InitProcedure ...
func InitProcedure(proc []PROCInitStruct) {
	if len(proc) > 0 {
		time.Sleep(2 * time.Second)
		initProcInTable(proc)
		initProc(proc)
	}
}

func initProcInTable(proc []PROCInitStruct) {
	for _, v := range proc {
		var com string
		if v.Partition != "" {
			com = `echo "PARTITION TABLE ` + v.Table + ` ON COLUMN ` + v.Partition + `;" | ` + voltdb + `/bin/sqlcmd`
			out, err := exec.Command("sh", "-c", com).Output()
			log.Println(string(out))
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func initProc(proc []PROCInitStruct) {
	for _, v := range proc {
		var com string
		if v.Partition == "" {
			com = `echo "create procedure from class ` + v.Procedure + `;" | ` + voltdb + `/bin/sqlcmd`
		} else if v.Partition != "" {
			com = `echo "create procedure partition on table ` + v.Table + ` column ` + v.Partition + ` from class ` + v.Procedure + `;" | ` + voltdb + `/bin/sqlcmd`
		}

		if com != "" {
			out, err := exec.Command("sh", "-c", com).Output()
			log.Println(string(out))
			if err != nil {
				log.Println(err)
			}
		} else {
			panic("ERROR PROCEDURE INIT COMMAND EMPTY")
		}
	}
	log.Println("PROCEDURE INITIALIZE")
}

// CompileProc ...
func CompileProc() {
	rmComm := `rm -rf ` + procPath + `*.class`
	_, err := exec.Command("sh", "-c", rmComm).Output()
	if err != nil {
		log.Println("Failed Remove Class File")
		panic(err)
	}

	compileComm := `javac -cp "$CLASSPATH:` + compilerPath + `" ` + procPath + `*.java`
	_, err = exec.Command("sh", "-c", compileComm).Output()
	if err != nil {
		log.Println("Failed Compile Java")
		panic(err)
	}

	cpComm := `cp -a ` + procPath + `*.class ` + gopath + `/src/txn/`
	_, err = exec.Command("sh", "-c", cpComm).Output()
	if err != nil {
		log.Println("Failed Copy Class")
		panic(err)
	}

	jarComm := `jar cvf proc.jar *.class`
	_, err = exec.Command("sh", "-c", jarComm).Output()
	if err != nil {
		log.Println("Failed Compile Jar File")
		panic(err)
	}

	rmComm2 := `rm -rf ` + gopath + `/src/txn/*.class`
	_, err = exec.Command("sh", "-c", rmComm2).Output()
	if err != nil {
		log.Println("Failed Remove Class File2")
		panic(err)
	}

	loadComm := `echo "load classes ` + gopath + `/src/txn/proc.jar;" | ` + voltdb + `/bin/sqlcmd`
	_, err = exec.Command("sh", "-c", loadComm).Output()
	if err != nil {
		log.Println("Failed Ingest Compiler")
		panic(err)
	}

	rmComm3 := `rm -rf ` + gopath + `/src/txn/*.jar`
	_, err = exec.Command("sh", "-c", rmComm3).Output()
	if err != nil {
		log.Println("Failed Remove Class File3")
		panic(err)
	}
}
