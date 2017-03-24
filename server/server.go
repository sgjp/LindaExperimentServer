package server

import (//"reflect"
	"github.com/sgjp/LindaExperimentServer/tupleSpace"
	"log"
	"github.com/sgjp/go-coap"
	"strings"
	"fmt"
	//"time"
	"net"

	"strconv"
	"os"
	"encoding/csv"
	"time"
	"reflect"
)
var taskDurationFile = "/Users/jsanchez/workspace/src/github.com/sgjp/LindaExperimentServer/TaskDuration.csv"

var primeNumsQty int

var startTime time.Time

var space tupleSpace.TupleSpace

var flag = true

func StartServer() {

	primeNumsQty = 50

	space = tupleSpace.NewSpace()
	log.Fatal(coap.ListenAndServeMulticast("udp", "224.0.1.187:5683",
		coap.FuncHandler(func(l *net.UDPConn, a *net.UDPAddr, m *coap.Message) *coap.Message {
			//log.Printf("Got message path=%q: PayLoad: %#v from %v Code: %v", m.Path(), string(m.Payload), a, m.Code)
			if len(m.Path()) > 0 {

				switch m.Path()[0] {

				case "in":
					res := inTuple(m)
					return res

				case "rd":
					res := rdTuple(m)
					return res

				case "out":
					res := outTuple(m)
					return res
				case "eval":
					res := evalTuple(m)
					return res
				default:
					res := notFoundHandler(m)
					return res

				}
			} else {
				res := notFoundHandler(m)
				return res
			}
			return nil
		}),"en1"))


}
func inTuple(m *coap.Message) *coap.Message {



	tupleData := payloadToTuple(m.Payload)
	//log.Printf("Searching tuple %v",tupleData)

	recv1 := space.Take(tupleSpace.NewJS(0, tupleData))
	t1 := <- recv1

	log.Printf("Tuple found: %v TOTAL tuples: %v",t1, space.Len())
	payload := tupleToPayload(t1)

	res := &coap.Message{
		Type:      coap.Acknowledgement,
		Code:      coap.Content,
		MessageID: m.MessageID,
		Token:     m.Token,
		Payload:   payload,
	}
	res.SetOption(coap.ContentFormat, coap.TextPlain)
	return res
}
func rdTuple(m *coap.Message) *coap.Message {


	tupleData := payloadToTuple(m.Payload)
	//log.Printf("Searching tuple %v",tupleData)

	recv1 := space.Read(tupleSpace.NewJS(0, tupleData))
	t1 := <- recv1

	//log.Printf("Tuple found: %v", t1)
	payload := tupleToPayload(t1)

	res := &coap.Message{
		Type:      coap.Acknowledgement,
		Code:      coap.Content,
		MessageID: m.MessageID,
		Token:     m.Token,
		Payload:   payload,
	}
	res.SetOption(coap.ContentFormat, coap.TextPlain)
	return res
}
func outTuple(m *coap.Message) *coap.Message {
	tupleData := payloadToTuple(m.Payload)
	tuple := tupleSpace.NewJS(600, tupleData)

	//Start counting time when the first W tuple comes
	if flag && space.Len()==0 && !reflect.DeepEqual(tuple.Values(), []interface{}{`W`}) {
		startTime = time.Now()
		log.Println("First W tuple arrived!")
		flag = false
	}
	space.Write(tuple)
	log.Printf("Outing tuple: %v. TOTAL tuples: %v",tuple,space.Len())
	if space.Len()==primeNumsQty && !reflect.DeepEqual(tuple.Values(), []interface{}{`R`}) {
		elapsed := time.Since(startTime)
		saveTaskDuration(int64(elapsed/time.Millisecond),primeNumsQty)
		log.Println("Last R tuple arrived!")
	}

	res := &coap.Message{
		Type:      coap.Acknowledgement,
		Code:      coap.Created,
		MessageID: m.MessageID,
		Token:     m.Token,
		Payload:   []byte(string("1")),
	}
	res.SetOption(coap.ContentFormat, coap.TextPlain)
	return res
}
func evalTuple(m *coap.Message) *coap.Message {


	res := &coap.Message{
		Type:      coap.Acknowledgement,
		Code:      coap.NotImplemented,
		MessageID: m.MessageID,
		Token:     m.Token,
		Payload:   []byte("5.01"),
	}
	res.SetOption(coap.ContentFormat, coap.TextPlain)
	return res
}

func payloadToTuple(payload []byte) []string{
	var data []string
	payloadString := string(payload)

	data = strings.Split(payloadString,",")

	for i:=0; i < len(data) ; i++{
		data[i] = strings.Replace(data[i],"\"","",1)
	}
	return data

}

func tupleToPayload(tuple interface{}) []byte{
	var data []byte
	tupleString := fmt.Sprintf("%v",tuple)
	startIndex := strings.Index(tupleString,"[")
	finishIndex := strings.LastIndex(tupleString,"]")
	tupleData := tupleString[startIndex+1:finishIndex]

	tupleSlice := strings.Replace(tupleData," ",",",-1)
	data = []byte(tupleSlice)
	return data

}


func notFoundHandler(m *coap.Message) *coap.Message {

	res := &coap.Message{
		Type:      coap.Acknowledgement,
		Code:      coap.NotFound,
		MessageID: m.MessageID,
		Token:     m.Token,
		Payload:   []byte("4.05"),
	}
	res.SetOption(coap.ContentFormat, coap.TextPlain)
	return res

}


func saveTaskDuration(elapsed int64, qty int){
	record := []string{
		strconv.Itoa(qty), strconv.FormatInt(elapsed,10)}

	file, er := os.OpenFile(taskDurationFile, os.O_RDWR|os.O_APPEND, 0666)

	if er != nil {
		log.Fatal(er)
	}
	defer file.Close()
	writer := csv.NewWriter(file)

	err := writer.Write(record)


	if err != nil {
		log.Fatal(er)
	}

	defer writer.Flush()
}
