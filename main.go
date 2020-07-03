package main

import (
	"fmt"
)

func main() {
	d := NewDB("test1.db")
	d.Write("key2", "efgh")
	d.Write("key1", "abcd")
	fmt.Println(d.Read("key2"))
	fmt.Println(d.Read("key1"))
	d.Write("Abcd", "1234")
	d.Write("key1", "3456")
	d.Write("key", "34678")
	fmt.Println(d.Read("Abcd"))
	fmt.Println(d.Read("key1"))
	fmt.Println(d.Read("key"))
	d.Write("A", "5678")
	fmt.Println(d.Read("A"))
	fmt.Println(d.Read("B"))

}
