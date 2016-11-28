package main


import (
	"github.com/sgjp/LindaExperimentServer/server"

	"github.com/sgjp/LindaExperimentServer/tupleSpace"
	"reflect"
	"log"
)

func main() {

	//testTupleSpace()

	server.StartServer()
}

func testTupleSpace(){
	var space tupleSpace.TupleSpace

	space = tupleSpace.NewSpace()

	//tupleModel := tupleSpace.New(600,1,2)
	//space.Write(tupleModel)

	recv1 := space.Take(tupleSpace.New(0, 1))
	t1 := <-recv1

	if !reflect.DeepEqual(t1.Values(), []interface{}{1, 2}) {
		log.Print(`failed to Read from TupleSpace.`)
	}

	log.Printf("Read%v", t1)

	recv2 := space.Take(tupleSpace.New(0, 1))
	t2 := <-recv2

	if !reflect.DeepEqual(t2.Values(), []interface{}{1, 2}) {
		log.Print(`failed to Read from TupleSpace.`)
	}

	log.Printf("Read%v", t2)

	recv3 := space.Read(tupleSpace.New(0, 1))
	t3 := <-recv3

	if !reflect.DeepEqual(t3.Values(), []interface{}{1, 2}) {
		log.Print(`failed to Read from TupleSpace.`)
	}

	log.Printf("Read%v", t3)


	/*recv2 := space.Take(tupleSpace.New(0, `foo`))

if t2 := <-recv2; !reflect.DeepEqual(t2.Values(), []interface{}{`foo`, `bar`}) {
	log.Print(`failed to Take from TupleSpace.`)
}

if space.Len() > 0 {
	log.Print(`remove tuple from Take method is failed.`)
}

*/
}
