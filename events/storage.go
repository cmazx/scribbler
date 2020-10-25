package events

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"time"
)

type Event struct {
	IP           string
	UserAgent    string
	Gid          string
	Sid          string
	Country      string
	City         string
	Os           string
	OsMajor      uint16
	OsMinor      uint16
	OsPatch      uint16
	Browser      string
	BrowserMajor uint16
	BrowserMinor uint16
	BrowserPatch uint16
	DeviceType   string
	IsBot        bool
	UtmSource    string
	UtmMedium    string
	UtmCampaign  string
	UtmTerm      string
	UtmContent   string
	Tags         []string
	EventName    string
	EventValue   string
	Time         time.Time
}

type Storage struct {
	Connect *sql.DB
}

func newClickhouseConnect(dataSourceName string) *sql.DB {
	connect, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil
	}
	return connect
}

func NewStorage(dataSourceName string) *Storage {
	return &Storage{
		Connect: newClickhouseConnect(dataSourceName),
	}
}

func (service *Storage) StoreEvents(rows *[]*Event) {
	var (
		tx, _   = service.Connect.Begin()
		stmt, _ = tx.Prepare(
			"INSERT INTO stat (gid, sid, ip, user_agent,  city, country, os_name, os_major, os_minor, os_patch," +
				" browser_name, browser_major, browser_minor, browser_patch, device_type, is_bot, utm_source, utm_medium, " +
				"utm_campaign, utm_term, utm_content, tags, event_name, event_value, action_day, action_time) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	)

	defer stmt.Close()

	for _, row := range *rows {
		fmt.Println(
			row.BrowserMajor,
			row.BrowserMinor,
			row.BrowserPatch,
		)
		if _, err := stmt.Exec(
			row.Gid,
			row.Sid,
			row.IP,
			row.UserAgent,
			row.City,
			row.Country,
			row.Os,
			row.OsMajor,
			row.OsMinor,
			row.OsPatch,
			row.Browser,
			row.BrowserMajor,
			row.BrowserMinor,
			row.BrowserPatch,
			row.DeviceType,
			row.IsBot,
			row.UtmSource,
			row.UtmMedium,
			row.UtmCampaign,
			row.UtmTerm,
			row.UtmContent,
			row.Tags,
			row.EventName,
			row.EventValue,
			row.Time.Format("2006-01-02"),
			row.Time.Format("2006-01-02 15:04:05"),
		); err != nil {
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
func (service *Storage) CreateDatabase() {
	_, err := service.Connect.Exec(`
		CREATE TABLE IF NOT EXISTS stat
		(
			gid           UUID COMMENT 'global user id',
			sid           UUID COMMENT 'session id',
		
			ip String, 
			user_agent String,  
			city String,  
			country  FixedString(2),
			os_name       Enum8(
				'Unknown'=1 ,
				'WindowsPhone'=2,
				'Windows'=3,
				'Macosx'=4,
				'IOS'=5,
				'Android'=6,
				'BlackBerry'=7,
				'ChromeOS'=8,
				'Kindle'=9,
				'WebOS'=10,
				'Linux'=11,
				'Playstation'=12,
				'Xbox'=13,
				'Nintendo'=14,
				'Bot'=15
				),
			os_major      UInt16,
			os_minor      UInt16,
			os_patch      UInt16,
		
			browser_name  Enum8(
				'Unknown'=1,
				'Chrome'=2,
				'IE'=3,
				'Safari'=4,
				'Firefox'=5,
				'Android'=6,
				'Opera'=7,
				'Blackberry'=8,
				'UCBrowser'=9,
				'Silk'=10,
				'Nokia'=11,
				'NetFront'=12,
				'QQ'=13,
				'Maxthon'=14,
				'SogouExplorer'=15,
				'Spotify'=16,
				'Nintendo'=17,
				'Samsung'=18,
				'Yandex'=19,
				'CocCoc'=20,
				'Bot'=21,
				'AppleBot'=22,
				'BaiduBot'=23,
				'BingBot'=24,
				'DuckDuckGoBot'=25,
				'FacebookBot'=26,
				'GoogleBot'=27,
				'LinkedInBot'=28,
				'MsnBot'=29,
				'PingdomBot'=30,
				'TwitterBot'=31,
				'YandexBot'=32,
				'CocCocBot'=33,
				'YahooBot'=34),
		
			browser_major UInt16,
			browser_minor UInt16,
			browser_patch UInt16,
			device_type   Enum8(
				'Unknown'=1,
				'Computer'=2,
				'Tablet'=3,
				'Phone'=4,
				'Console'=5,
				'Wearable'=6,
				'TV'=7
				),
			is_bot        UInt8,
			utm_source    String,
			utm_medium    String,
			utm_campaign  String,
			utm_term      String,
			utm_content   String,
		
			tags          Array(String) COMMENT 'any custom tags',
			event_name	  String,
			event_value   String,
			action_day    Date,
			action_time   DateTime
		) engine = Memory
	`)

	if err != nil {
		log.Fatal(err)
	}
}
