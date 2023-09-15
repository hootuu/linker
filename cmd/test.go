package main

import (
	"fmt"
	"github.com/hootuu/tail"
	"os"
	"time"
)

func main() {
	err := tail.Init("./data")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	go func() {
		_, err := tail.Tail("vn001", "chain001", "20210201")
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		fmt.Println("保存成功")
	}()
	go func() {
		for i := 0; i < 3; i++ {
			err := tail.Ack("xxx")
			if err != nil {
				fmt.Println(err)
			}
			time.Sleep(3 * time.Second)
		}
	}()
	time.Sleep(10 * time.Minute)
}
