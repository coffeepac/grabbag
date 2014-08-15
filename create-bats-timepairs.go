package main

import (
  "os"
  "fmt"
  "sort"
  "flag"
  "time"
  "bytes"
  "strings"
  ioutil "io/ioutil"
  json "encoding/json"
  http "net/http"
)

var (
  eshost = "http://kibana.mtnsatcloud.com:9200/logstash-2014.07*,logstash-2014.08.0*,logstash-2014.08.10,logstash-2014.08.11,logstash-2014.08.12/_search?fields=@timestamp,event,shipName"
  esquery_ur = "&q=@type:zeroimpact-rabbitmq-input%20and%20type:event%20and%20event:Switching*%20and%20shipName:Coral"
  esquery_doc = `{"query":{"filtered":{"query":{"bool":{"must":[{"query_string":{"query":"Switching"}}]}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"from":1407794970791,"to":1407967770791}}},{"fquery":{"query":{"query_string":{"query":"@type:zeroimpact-rabbitmq-input"}}}},{"fquery":{"query":{"query_string":{"query":"type:event or rmevent"}}}},{"fquery":{"query":{"query_string":{"query":"shipName:PROVIDEPYLONS"}}}}]}}}},"size":500,"sort":[{"@timestamp":{"order":"asc","ignore_unmapped":true}},{"@timestamp":{"order":"asc","ignore_unmapped":true}}]}`
  shipname = flag.String("shipname", "", "name of ship according to value set by intelligent router")
  spool_dir = flag.String("spool_dir", "/mnt/spooled_messages", "name of ship according to value set by intelligent router")
)

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
  Fields struct{
    Timestamp []string `json:"@timestamp"`
    Event []string `json:"event"`
    ShipName []string `json:"shipName"`
  } `json:'fields'`
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

func createBatsTimeline(shipname string) (BatsRanges []BatsRange) {
  //  build a client
  client := &http.Client{}
  resolved_esquery_doc := strings.Replace(esquery_doc, "PROVIDEPYLONS",shipname, 1)
  readeresdoc := bytes.NewBufferString(resolved_esquery_doc)
  seriesReq, err := http.NewRequest("GET", eshost, readeresdoc)
  seriesReq.Header.Add("Content-Type", "Application/json")

  resp, err := client.Do(seriesReq)
  if err != nil {
    fmt.Println(err)
  } else {
    var kibanaRespons kbResp
    decoder := json.NewDecoder(resp.Body)
    err = decoder.Decode(&kibanaRespons)
    if err != nil {
      fmt.Println("Failed get!")
        fmt.Println(err)
    } else {
      sort.Sort(ByTime(kibanaRespons.Hits.Hits))
      // start pairing process.  if first record is VSATs, throw it away
      messages_only := kibanaRespons.Hits.Hits
      if strings.Contains(strings.ToLower(messages_only[0].Fields.Event[0]), "vsat") {
        messages_only = messages_only[1:]
      }
      BatsRanges = make([]BatsRange, len(messages_only)/2+1, len(messages_only)/2+1)
      for idx, v := range messages_only {
        if idx % 2 == 0 {
          BatsRanges[idx/2].StartTime = v.Fields.Timestamp[0]
        } else {
          BatsRanges[idx/2].EndTime = v.Fields.Timestamp[0]
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
    fmt.Println(err)
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

func main() {
  flag.Parse()
  batRanges := createBatsTimeline(*shipname)

  fileList := createArchiveFilelist(*spool_dir)
  filteredList := filterArchiveFiles(batRanges, fileList)

  fmt.Println("filtered list:")
  for _, v := range filteredList {
    fmt.Println(v)
  }
}
