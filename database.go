package main

import (
	"fmt"
	"os"
)

type DB struct {
	f *os.File
}

func NewDB(filename string) *DB {
	// -- code --
	f1, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0770)
	if err != nil {
		return nil
	}
	return &DB{
		f: f1,
	}
	// -- end --
}

func (d *DB) Write(key string, value string) error {
	// -- code --
	s := PaddedString(key, 100)
	t := PaddedString(value, 1000)
	fi, err := d.f.Stat()
	if err != nil {
		panic(err)

	}
	//l is the length of the file
	l := fi.Size()
	// m is the no. of keys present
	m := l / 1100
	//a will give the position where we need to write the key and b will tell whether the key is present or not
	a, b := d.binarysearch(string(s), 0, int(m-1))
	//if key is present
	if b == 1 {
		d.f.Seek(int64(1100*a+100), 0)
		_, err := d.f.Write(t)
		return err
	}
	//if key is not present
	data := append(s, t...)
	if int(m) == a {
		d.f.Seek(0, 2)
		_, err := d.f.Write(data)
		return err
	}
	//A new file to copy
	f1, err := os.OpenFile("Copy-File.db", os.O_CREATE|os.O_RDWR, 0770)
	if err != nil {

		panic(err)

	}
	d.f.Seek(0, 0)
	snh := make([]byte, 1100*a)
	_, err1 := d.f.Read(snh)
	if err1 != nil {
		panic(err)

	}
	f1.Seek(0, 0)
	_, err = f1.Write(snh)
	if err != nil {
		panic(err)

	}
	f1.Seek(int64(1100*a), 0)
	f1.Write(data)
	d.f.Seek(int64(a*1100), 0)
	snh1 := make([]byte, (int(l) - 1100*a))
	_, err = d.f.Read(snh1)
	if err != nil {
		panic(err)

	}
	f1.Seek(int64(1100*(a+1)), 0)
	_, err = f1.Write(snh1)
	if err != nil {
		panic(err)

	}
	f1.Seek(0, 0)
	snh2 := make([]byte, int(l+1100))
	_, err = f1.Read(snh2)
	if err != nil {
		panic(err)

	}
	d.f.Seek(0, 0)
	_, err = d.f.Write(snh2)
	f1.Close()
	err3 := os.Remove("Copy-file.db")
	if err3 != nil {
		panic(err3)

	}
	return err
	// -- end --
}

func (d *DB) Read(key string) (string, error) {
	// -- code --
	t := PaddedString(key, 100)
	fi, err := d.f.Stat()
	if err != nil {
		panic(err)
	}
	l := fi.Size() / 1100
	a, c := d.binarysearch(string(t), 0, int(l-1))
	if c == 1 {
		d.f.Seek(int64(a*1100+100), 0)
		b := make([]byte, 1000)
		_, err := d.f.Read(b)
		s := ReadStringUntilZero(b)
		return s, err
	}
	return "Key Not Found", nil
	// -- end --
}

func (d *DB) Close() error {
	// -- code --
	err := d.f.Close()
	return err
	// -- end --
}

// Return string padded with 0 bytes.
func PaddedString(k string, L int) []byte {
	// -- code --
	byteS := make([]byte, L-len(k))
	k = k + string(byteS)
	return []byte(k)
	// -- end --
}
func ReadStringUntilZero(b []byte) string {
	// -- code --
	s := ""
	i := 0
	for {
		if b[i] == 0 || i == len(b) {
			break
		}
		s = s + string(b[i])
		i++
	}
	return s
	// -- end --
}

// Return the offset of key in the file. Return -1 if key does not exist.
func (d *DB) offset(key string) int {
	// -- code --

	b := make([]byte, 100)
	c := 0
	for {
		d.f.Seek(int64(c), 0)
		_, err := d.f.Read(b)
		if err != nil {
			break
		}
		if key == string(b) {
			return c
		}
		c += 1100
	}
	return -1
	// -- end --
}
func (d *DB) binarysearch(key string, low int, high int) (int, int) {
	//if file is empty
	if high < low {
		return 0, -1
	}

	mid := low + (high-low)/2
	d.f.Seek(int64(mid*1100), 0)
	b := make([]byte, 100)
	_, err := d.f.Read(b)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if string(b) == key {
		return mid, 1
	}
	if high == low {
		if key > string(b) {
			return high + 1, -1
		}
		return high, -1
	}

	if key > string(b) {
		return d.binarysearch(key, mid+1, high)
	}
	return d.binarysearch(key, low, mid-1)
}
