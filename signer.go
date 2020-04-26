package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const TH = 5
const showOutput = false
func crc32HashGen(i int, data string, stack chan map[int]string, wg *sync.WaitGroup){
	defer wg.Done()
	var hash = map[int]string{}
	hash[i] = DataSignerCrc32(data)
	stack <- hash
}

var SingleHash = func(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	crc32Chan := make(chan map[int]string, 100)
	crc32md5Chan := make(chan map[int]string, 100)
	var arrayNum []int
	for fibNum := range in {
		md5 := DataSignerMd5(strconv.Itoa(fibNum.(int)))
		if (showOutput) {
			fmt.Printf("%s SingleHash md5(data) %s\n", strconv.Itoa(fibNum.(int)), md5)
		}
		wg.Add(1)
		go crc32HashGen(fibNum.(int), strconv.Itoa(fibNum.(int)), crc32Chan, wg)
		wg.Add(1)
		go crc32HashGen(fibNum.(int), md5, crc32md5Chan, wg)
		arrayNum = append(arrayNum, fibNum.(int))
	}
	wg.Wait()
	close(crc32Chan)
	close(crc32md5Chan)

	var crc32md5Map = map[int]string{}
	for crc32md5 := range crc32md5Chan {
		for i, item := range crc32md5 {
			crc32md5Map[i] = item
		}
	}
	var crc32Map = map[int]string{}
	for crc32 := range crc32Chan {
		for i, item := range crc32 {
			crc32Map[i] = item
		}
	}
	for _, num:=range arrayNum {
		out <- crc32Map[num] + "~" + crc32md5Map[num]
		if (showOutput) {
			fmt.Printf("%d | SingleHash %s\n", num, crc32Map[num] + "~" + crc32md5Map[num])
		}
	}
}

var MultiHash = func(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	crc32Chan := make(chan map[int]string, 100)

	k := 0 
	for stackHash := range in {
		for i := 0; i <= TH; i++ {
			wg.Add(1)
			go crc32HashGen(k * 100 + i, strconv.Itoa(i) + stackHash.(string), crc32Chan, wg)
		}
		k++
	}
	wg.Wait()
	close(crc32Chan)
	
	var crc32Map = map[int]string{}
	for crc32 := range crc32Chan {
		for i, item := range crc32 {
			crc32Map[i] = item
		}
	}
	for i:=0;i<k; i++ {
		var totalHash string
		for j := 0; j <= TH; j++ { 
			totalHash += crc32Map[i*100 + j]
			if (showOutput) {
				fmt.Printf("%d | MultiHash: crc32(%d+%d)) %s\n", j, j, i*100 + j, crc32Map[i*100 + j])
			}
		}
		out <- totalHash
	}
}

var CombineResults = func(in, out chan interface{}) {
	var sortList []string
	for crc32Map := range in {
		sortList = append(sortList, crc32Map.(string))
	}
	sort.Strings(sortList)
	if (showOutput) {
		fmt.Printf("CombineResults \n%s\n", strings.Join(sortList, "_"))
	}
	out <- strings.Join(sortList, "_")
}

func runJob(waiter *sync.WaitGroup, jobEntity job, in, out chan interface{}) {
	defer waiter.Done()
	defer close(out)
	jobEntity(in, out)
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{}, 100)
	for _, j := range jobs {
		out := make(chan interface{}, 100)
		wg.Add(1)
		go runJob(wg, j, in, out)
		in = out
	}
	wg.Wait()
}