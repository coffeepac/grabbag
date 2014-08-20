package main

import (
  "os"
  "fmt"
  "log"
  "sort"
  "flag"
  "time"
  "bytes"
  "strings"
  ioutil "io/ioutil"
  json "encoding/json"
  http "net/http"
  amqp "github.com/streadway/amqp"
)

var (
  eshost = "http://kibana.mtnsatcloud.com:9200/logstash-PROVIDEPYLONS/_search?fields=@timestamp,event,shipName"
  esindexdate = flag.String("indexdate", "", "date to query elasticsearch")
  esquery_doc = `{"query":{"filtered":{"query":{"bool":{"must":[{"query_string":{"query":"Switching"}}]}},"filter":{"bool":{"must":[{"fquery":{"query":{"query_string":{"query":"@type:zeroimpact-rabbitmq-input"}}}},{"fquery":{"query":{"query_string":{"query":"type:event or rmevent"}}}},{"fquery":{"query":{"query_string":{"query":"shipName:PROVIDEPYLONS"}}}}]}}}},"size":500}`
  shipname = flag.String("shipname", "", "name of ship according to value set by intelligent router")
  spool_dir = flag.String("spool_dir", "/mnt/spooled_messages", "name of ship according to value set by intelligent router")
  uri          = flag.String("uri", "amqp://datahouse:d4t4m4ngl3@localhost:5672/%2Fdatahouse", "AMQP URI")
  exchange     = flag.String("exchange", "netflow_dummy", "Durable AMQP exchange name")
  exchangeType = flag.String("exchangeType", "direct", "Exchange type - direct|fanout|topic|x-custom")
  routingKey   = flag.String("routingKey", "netflow_dummy", "AMQP routing key")
)

type RabbitChanWriter struct {
  amqpchan *amqp.Channel
  amqpconn *amqp.Connection
}

func (rcw *RabbitChanWriter) Close() (err error) {
  return rcw.amqpconn.Close()
}

func (rcw *RabbitChanWriter) Write(b []byte) (n int, err error) {
  if err = rcw.amqpchan.Publish(
    *exchange,
    *routingKey,
    false,
    false,
    amqp.Publishing{
      Headers:         amqp.Table{},
      ContentType:     "text/plain",
      ContentEncoding: "",
      Body:            b[0:len(b)],
      DeliveryMode:    amqp.Persistent,
    },
  ); err != nil {
    log.Printf("Exchange Publish: %s", err)
    return -1, err
  }

  return len(b), nil
}


/*
**  build the connection to the rabbitmq server
*/
func buildChannel() (rChan *RabbitChanWriter) {
  connection, err := amqp.Dial(*uri)
  if err != nil {
    log.Printf("Dial: %s", err)
    return nil
  }

  channel, err := connection.Channel()
  if err != nil {
    log.Printf("Channel: %s", err)
    return nil
  }

  //  build the exchange
  if err := channel.ExchangeDeclare(
    *exchange,     // name
    *exchangeType, // type
    true,         // durable
    false,        // auto-deleted
    false,        // internal
    false,        // noWait
    nil,          // arguments
  ); err != nil {
    log.Fatalf("Exchange Declare: %s", err)
  }

  //  create a queue with the routing key and bind to it
  if _, err := channel.QueueDeclare(
    *routingKey,    //  name
    true,           //  durable
    false,          //  autoDelete
    false,          //  exclusive
    false,          //  noWait
    nil,            //  args
  ); err != nil {
    log.Fatalf("Queue Declare: %s", err)
  }

  if err := channel.QueueBind(
    *routingKey,    //  name
    *routingKey,    //  key
    *exchange,      //  exchange
    false,          //  noWait
    nil,            //  args
  ); err != nil {
    log.Fatalf("Queue Bind: %s", err)
  }


  rChan = &RabbitChanWriter{channel, connection}
  return
}


/*
**  writes a message on the internal 'messages' chan to rabbitmq
*/
func writeToRabbit(rabbitWriter *RabbitChanWriter, message []byte){
  _, err := rabbitWriter.Write(message)
  if err != nil {
    log.Printf("got an error of: %s", err)
    //  close it up, have a nap, try again
    rabbitWriter.Close()
    time.Sleep(1 * time.Second)
    for {
      rabbitWriter = buildChannel()
      if rabbitWriter != nil {
        break
      } else {
        log.Println("failed to connect to rabbitmq server, nap and try again")
        time.Sleep(10*time.Second)
      }
    }
  }
}


type ByTime []Hits

func (a ByTime) Len() int               { return len(a) }
func (a ByTime) Swap(i, j int)          { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool     { return a[i].Fields.Timestamp[0] < a[j].Fields.Timestamp[0] }

type kbResp struct {
  Hits struct {
    Hits []Hits `json:'hits'`
  }
}

type Hits struct {
  Fields Fields `json:'fields'`
}

type Fields struct{
  Timestamp []string `json:"@timestamp"`
  Event []string `json:"event"`
  ShipName []string `json:"shipName"`
}

type BatsRange struct {
  StartTime string
  EndTime string
}

type FileInfoDir struct {
  FileInfo os.FileInfo
  Directory string
}

func (b BatsRange) ToString() {
  fmt.Println("Start time is: ", b.StartTime, " and End time is: ", b.EndTime)
}

func createBatsTimeline(shipname, esindexdate string) (BatsRanges []BatsRange) {
  //  build a client
  client := &http.Client{}
  resolved_esquery_doc := strings.Replace(esquery_doc, "PROVIDEPYLONS", shipname, 1)
  resolved_eshost := strings.Replace(eshost, "PROVIDEPYLONS", esindexdate, 1)
  readeresdoc := bytes.NewBufferString(resolved_esquery_doc)
  seriesReq, err := http.NewRequest("GET", resolved_eshost, readeresdoc)
  seriesReq.Header.Add("Content-Type", "Application/json")

  resp, err := client.Do(seriesReq)
  if err != nil {
    log.Println(err)
  } else {
    var kibanaRespons kbResp
    decoder := json.NewDecoder(resp.Body)
    err = decoder.Decode(&kibanaRespons)
    if err != nil {
      log.Println("Failed get!")
      log.Println(err)
    } else {
      if len(kibanaRespons.Hits.Hits) == 0 {
        log.Println("No hits!  Eject!")
      } else {
        sort.Sort(ByTime(kibanaRespons.Hits.Hits))
        // start pairing process.  if first record is VSAT, create a BATS record to start the day
        messages_only := kibanaRespons.Hits.Hits
        if strings.Contains(strings.ToLower(messages_only[0].Fields.Event[0]), "vsat") {
          startMoment := Hits{Fields{Timestamp:[]string{strings.Replace(esindexdate, ".", "-", 2)+"T00:00:00Z"}}}
          messages_only = append([]Hits{startMoment}, messages_only...)
        }
        if strings.Contains(strings.ToLower(messages_only[len(messages_only)-1].Fields.Event[0]), "bats") {
          endMoment := Hits{Fields{Timestamp:[]string{strings.Replace(esindexdate, ".", "-", 2)+"T23:59:59Z"}}}
          messages_only = append(messages_only, endMoment)
        }
        BatsRanges = make([]BatsRange, len(messages_only)/2, len(messages_only)/2)
        for idx, v := range messages_only {
          if idx % 2 == 0 {
            BatsRanges[idx/2].StartTime = v.Fields.Timestamp[0]
          } else {
            BatsRanges[idx/2].EndTime = v.Fields.Timestamp[0]
          }
        }
      }
    }
  }

  return BatsRanges
}

func createArchiveFilelist(directory string) (filelist []FileInfoDir) {
  filelist = make([]FileInfoDir, 0, 100)
  files, err := ioutil.ReadDir(directory)
  if err != nil {
    log.Println(err)
  } else {
    for _, v := range files {
      if v.IsDir() {
        filelist = append(filelist, createArchiveFilelist(directory + "/" + v.Name())...)
      } else {
        filelist = append(filelist, FileInfoDir{FileInfo: v, Directory:directory})
      }
    }
  }
  return filelist
}

func filterArchiveFiles(batRanges []BatsRange, fileList []FileInfoDir) (filteredList []string) {
  filteredList = make([]string, 0, 100)
  for _, file := range fileList {
    for _, batsrange := range batRanges {
      if batsrange.EndTime != "" && batsrange.StartTime != "" {
        if file.FileInfo.ModTime().UTC().Format(time.RFC3339) > batsrange.StartTime && file.FileInfo.ModTime().UTC().Format(time.RFC3339) < batsrange.EndTime {
          filteredList = append(filteredList, file.Directory + "/" + file.FileInfo.Name())
          continue
        }
      }
    }
  }
  return filteredList
}

func writeToLocalBroker(filesToSend []string, rabbitWriter *RabbitChanWriter) {
  for _, v := range filesToSend {
    message_data, err := ioutil.ReadFile(v)
    if err != nil {
      log.Println("Failed to read file ", v)
    } else {
      rabbitWriter.Write(message_data)
    }
  }
}

func main() {
  flag.Parse()
  batRanges := createBatsTimeline(*shipname, *esindexdate)
  for _, g := range batRanges {
    log.Println(g)
  }

  fileList := createArchiveFilelist(*spool_dir)
  filteredList := filterArchiveFiles(batRanges, fileList)


  var rabbitWriter *RabbitChanWriter
  for {
    rabbitWriter = buildChannel()
    if rabbitWriter != nil {
      break
    } else {
      log.Println("failed to connect to rabbitmq server, nap and try again")
      time.Sleep(10*time.Second)
    }
  }

  writeToLocalBroker(filteredList, rabbitWriter)
  log.Println("filtered list:")
  for _, v := range filteredList {
    log.Println(v)
  }
}
