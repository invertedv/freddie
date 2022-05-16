package main

import (
	"flag"
	"fmt"
	_ "github.com/mailru/go-clickhouse/v2"
	"io/ioutil"
	"log"
)
import "github.com/invertedv/chutils"

func main() {
	host := flag.String("host", "127.0.0.1", "string")
	user := flag.String("user", "", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	flag.Parse()
	con, err := chutils.NewConnect("http", *host, *user, *password)
	if err != nil {
		log.Fatalln(err)
	}
	con.DB.Ping()
	dir, err := ioutil.ReadDir(*srcDir)
	if err == nil {
		for _, f := range dir {
			fmt.Println(f)
		}
	}

}
