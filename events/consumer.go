package events

import (
	"fmt"
	"github.com/avct/uasurfer"
	"github.com/oschwald/geoip2-golang"
	"log"
	"net"
	"sync"
	"time"
)

type Consumer struct {
	eventsBatch        []*Event
	maxEventsBatchSize int
	storage            *Storage
	queueCh            chan *Event
	doneCh             chan bool
	wg                 sync.WaitGroup
	lastStoreTime      time.Time
	GeoIpReader        *geoip2.Reader
	RecordsMutex       sync.Mutex
}

func NewConsumer(maxSize int, clickhouseProvider *Storage) *Consumer {
	geoIp, err := geoip2.Open("./GeoIP2-City.mmdb")
	if err != nil {
		fmt.Println("Geoip db not found")
		geoIp = nil
	}

	return &Consumer{
		GeoIpReader:        geoIp,
		eventsBatch:        make([]*Event, 0, maxSize),
		maxEventsBatchSize: maxSize,
		storage:            clickhouseProvider,
		queueCh:            make(chan *Event, maxSize*2),
		RecordsMutex:       sync.Mutex{},
	}
}

func (consumer *Consumer) EventCount() int {
	return len(consumer.eventsBatch)
}

func (consumer *Consumer) AppendEvent(record *Event) {
	consumer.eventsBatch = append(consumer.eventsBatch, record)
	if consumer.persistIsRequired() {
		consumer.Persist()
	}
}

func (consumer *Consumer) PreProcessEvents() {
	for i, row := range consumer.eventsBatch {
		uaData := uasurfer.Parse(row.UserAgent)
		consumer.eventsBatch[i].Os = uaData.OS.Name.StringTrimPrefix()
		consumer.eventsBatch[i].OsPatch = uint16(uaData.OS.Version.Patch)
		consumer.eventsBatch[i].OsMajor = uint16(uaData.OS.Version.Major)
		consumer.eventsBatch[i].OsMinor = uint16(uaData.OS.Version.Minor)
		consumer.eventsBatch[i].Browser = uaData.Browser.Name.StringTrimPrefix()
		consumer.eventsBatch[i].BrowserPatch = uint16(uaData.Browser.Version.Patch)
		consumer.eventsBatch[i].BrowserMajor = uint16(uaData.Browser.Version.Major)
		consumer.eventsBatch[i].BrowserMinor = uint16(uaData.Browser.Version.Minor)
		consumer.eventsBatch[i].DeviceType = uaData.DeviceType.StringTrimPrefix()
		consumer.eventsBatch[i].IsBot = uaData.IsBot()

		if consumer.GeoIpReader != nil {
			ip := net.ParseIP(row.IP)
			record, err := consumer.GeoIpReader.City(ip)
			if err != nil {
				log.Fatal(err)
			}
			consumer.eventsBatch[i].Country = record.Country.IsoCode
		}
	}
}

func (consumer *Consumer) Persist() {
	if len(consumer.eventsBatch) == 0 {
		return
	}

	consumer.PreProcessEvents()
	consumer.storage.StoreEvents(&consumer.eventsBatch)
	consumer.eventsBatch = make([]*Event, 0, consumer.maxEventsBatchSize)
}

func (consumer *Consumer) persistIsRequired() bool {
	return consumer.maxEventsBatchSize <= len(consumer.eventsBatch) || consumer.lastStoreTime.Before(time.Now().Add(60*time.Second))
}

func (consumer *Consumer) Stop() {
	consumer.doneCh <- true
	consumer.wg.Wait()
	_ = consumer.GeoIpReader.Close()
}

func (consumer *Consumer) Consume() {
	fmt.Println("Consumer started")
	consumer.wg.Add(1)
	defer consumer.wg.Done()

	for {
		select {
		case record := <-consumer.queueCh:
			consumer.AppendEvent(record)
		case <-consumer.doneCh:
			consumer.Persist()

			fmt.Println("Consumer stopped.")
			break
		}

		if consumer.persistIsRequired() {
			consumer.Persist()
		}
	}

}

func (consumer *Consumer) Enqueue(event *Event) {
	consumer.queueCh <- event
}
