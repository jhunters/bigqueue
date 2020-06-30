package bigqueue

import (
	"fmt"
	"testing"
)

//在当前目录下，运行以及命令
//go test -bench=. -benchtime=3s -run=^$
// 输出如下
// BenchmarkSprintf-8      50000000               109 ns/op
// 看到函数后面的-8了吗？这个表示运行时对应的GOMAXPROCS的值。
// 接着的20000000表示运行for循环的次数也就是调用被测试代码的次数
// 最后的117 ns/op表示每次需要话费117纳秒。(执行一次操作话费的时间)
//如果查看内存分配配置，命令如下 加-benchmem
// go.exe test -bench=. -benchmem -run=^$
func Benchmark_EnqueueOnly(b *testing.B) {

	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	// init enqueue
	bb := []byte("hello xiemalin! welcome to our world!")

	b.ResetTimer()
	t := &testing.T{}
	enqueue(queue, bb, b.N, t)
	b.StopTimer()

}

func Benchmark_DequeueOnly(b *testing.B) {

	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	bb := []byte("hello xiemalin! welcome to our world!")
	t := &testing.T{}
	enqueue(queue, bb, b.N, t)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queue.Dequeue()
	}
	b.StopTimer()

}

func Benchmark_EnqueueDequeue(b *testing.B) {

	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	bb := []byte("hello xiemalin! welcome to our world!")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queue.Enqueue(bb)
		queue.Dequeue()
	}
	b.StopTimer()
}

func Benchmark_ParallelEnqueueDequeue(b *testing.B) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()
	bb := []byte("hello xiemalin! welcome to our world!")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Enqueue(bb)
			queue.Dequeue()
		}
	})
}
