package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.run-sentinel.tgbot/botapi"
	"go.run-sentinel.tgbot/clouddb"
)

var (
	INFLUX_DB_CONFIG            clouddb.InfluxDBConfig
	INFLUX_DB_RUNS_START_STRING string  = "-1d"
	HUMIDITY_DELTA_THRESHOLD    float64 = 0.1
	HUMIDITY_VALUE_THRESHOLD    float64 = 0.2
	START_TIME                  time.Time
	LAST_CHECK_TIME             time.Time
	LAST_CHECKED_RUN            *clouddb.RunMetadata
)

type ResultType int

const (
	RESULT_OK  ResultType = 0
	RESULT_NOK ResultType = 1
)

var (
	VERIFIED_ITEMS_MAP map[string]string
)

func init() {
	START_TIME = time.Now()

	INFLUX_DB_CONFIG.URL = os.Getenv("INFLUX_DB_URL")
	if INFLUX_DB_CONFIG.URL == "" {
		log.Fatalln("no INFLUX_DB_URL env var provided")
	}

	INFLUX_DB_CONFIG.Org = os.Getenv("INFLUX_DB_ORG")
	if INFLUX_DB_CONFIG.Org == "" {
		log.Fatalln("no INFLUX_DB_ORG env var provided")
	}

	INFLUX_DB_CONFIG.Token = os.Getenv("INFLUX_DB_TOKEN")
	if INFLUX_DB_CONFIG.Token == "" {
		log.Fatalln("no INFLUX_DB_TOKEN env var provided")
	}

	INFLUX_DB_CONFIG.Name = os.Getenv("INFLUX_DB_NAME")
	if INFLUX_DB_CONFIG.Name == "" {
		log.Fatalln("no INFLUX_DB_NAME env var provided")
	}

	INFLUX_DB_RUNS_START_STRING = os.Getenv("INFLUX_DB_RUNS_START_STRING")
	if INFLUX_DB_RUNS_START_STRING == "" {
		log.Fatalln("no INFLUX_DB_RUNS_START_STRING env var provided")
	}

	var err error
	deltaThresholdStr := os.Getenv("HUMIDITY_DELTA_THRESHOLD")
	if deltaThresholdStr == "" {
		log.Fatalln("no HUMIDITY_DELTA_THRESHOLD env var provided")
	}
	HUMIDITY_DELTA_THRESHOLD, err = strconv.ParseFloat(deltaThresholdStr, 64)
	if err != nil {
		log.Fatalln(err)
	}

	valueThresholdStr := os.Getenv("HUMIDITY_VALUE_THRESHOLD")
	if valueThresholdStr == "" {
		log.Fatalln("no HUMIDITY_VALUE_THRESHOLD env var provided")
	}
	HUMIDITY_VALUE_THRESHOLD, err = strconv.ParseFloat(valueThresholdStr, 64)
	if err != nil {
		log.Fatalln(err)
	}

	resetStoredVerifiedItemsMap := os.Getenv("RESET_STORED_VERIFIED_ITEMS_MAP")
	if resetStoredVerifiedItemsMap != "" {
		log.Println("Resetting previously saved VERIFIED_ITEMS_MAP..")
		VERIFIED_ITEMS_MAP = make(map[string]string)
		err = clouddb.WriteCache(filepath.Join("storage", "verified_items_map.gob"), VERIFIED_ITEMS_MAP)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		err = clouddb.ReadCache(filepath.Join("storage", "verified_items_map.gob"), &VERIFIED_ITEMS_MAP)
		if err != nil {
			log.Printf("Could not load previously saved VERIFIED_ITEMS_MAP: %s", err)
			VERIFIED_ITEMS_MAP = make(map[string]string)
		}
		log.Printf("Reusing previously saved VERIFIED_ITEMS_MAP: %d", len(VERIFIED_ITEMS_MAP))
	}
}

func getMeanHumidity(frames []*clouddb.FrameBase) float64 {
	var baselineAcc float64
	var baselineNb int
	for _, frame := range frames {
		if frame.Section == "baseline" {
			baselineAcc += frame.Value
			baselineNb++
			continue
		}
	}
	return baselineAcc / float64(baselineNb)
}

func CheckHumidityDelta(framesA []*clouddb.FrameBase, framesB []*clouddb.FrameBase) (ResultType, float64, error) {
	var err error

	meanA := getMeanHumidity(framesA)
	meanB := getMeanHumidity(framesB)

	delta := math.Abs(meanB - meanA)
	if delta >= HUMIDITY_DELTA_THRESHOLD {
		return RESULT_NOK, delta, err
	} else {
		return RESULT_OK, delta, err
	}
}

func CheckHumidityValue(frames []*clouddb.FrameBase) (ResultType, float64, error) {
	var err error
	mean := getMeanHumidity(frames)

	if mean >= HUMIDITY_VALUE_THRESHOLD {
		return RESULT_NOK, mean, err
	} else {
		return RESULT_OK, mean, err
	}
}

func PerformChecks(tsdbRef string, api api.QueryAPI, bot *tgbotapi.BotAPI) error {

	var err error
	err = clouddb.ReadCache(filepath.Join("storage", "verified_items_map.gob"), &VERIFIED_ITEMS_MAP)
	if err != nil {
		return err
	}

	runs, err := clouddb.GetRecentRuns(INFLUX_DB_RUNS_START_STRING, tsdbRef, api)
	if err != nil {
		return err
	}
	for _, run := range runs {
		// log.Printf("Performing check for run %s (%s): %s (%s)", run.ID, run.UID, run.Name, run.ProtocolName)
		items, err := clouddb.GetItems(run.UID, tsdbRef, api)
		if err != nil {
			log.Println(err)
			continue
		}
		var previousFrames []*clouddb.FrameBase
		for _, item := range items {
			itemRef, err := clouddb.HashString(fmt.Sprintf("%s_%s_%d", item.RunUID, item.Name, item.Cycle))
			if err != nil {
				return err
			}
			if _, ok := VERIFIED_ITEMS_MAP[itemRef]; ok {
				continue
			}
			// log.Printf("Performing check for item %s #%d of run %s (%s)", item.Name, item.Cycle, run.ID, run.UID)
			currentFrames, err := clouddb.FetchItemHumidityFrames(run.UID, item, api)
			if err != nil {
				return err
			}
			resultV, v, err := CheckHumidityValue(currentFrames)
			if err != nil {
				return err
			}
			if resultV > 0 {
				text := fmt.Sprintf(
					"Aberration found: item <b>%s #%d</b> of run <b>%s (%s)</b> shows high baseline humidity: RH = <b>%.2f (> %.2f)</b>\n<a href=\"https://hub.aryballe.com/clouddb?tsdb_ref_name=%s&selected_runs=%s@%s\">Link to DOH</a>",
					item.Name, item.Cycle, run.ID, run.Name, v, HUMIDITY_VALUE_THRESHOLD,
					INFLUX_DB_CONFIG.Name, INFLUX_DB_CONFIG.Name, run.UID,
				)
				fmt.Print(text)
				msg := tgbotapi.NewMessageToChannel(botapi.TELEGRAM_BOT_CHANNEL_USERNAME, text)
				msg.ParseMode = tgbotapi.ModeHTML
				_, err := bot.Send(msg)
				if err != nil {
					log.Println(err)
				}
				log.Println("Attempting to send the alert by email..")
				err = botapi.SendEmail(text)
				if err != nil {
					log.Println(err)
				}
			}
			if len(previousFrames) != 0 {
				resultD, d, err := CheckHumidityDelta(previousFrames, currentFrames)
				if err != nil {
					return err
				}
				if resultD > 0 {
					text := fmt.Sprintf(
						"Aberration found: item <b>%s #%d of run %s (%s)</b> registered high variation of humidity compared to previous measurement: dRH = <b>%.2f (> %.2f)</b>\n<a href=\"https://hub.aryballe.com/clouddb?tsdb_ref_name=%s&selected_runs=%s@%s\">Link to DOH</a>",
						item.Name, item.Cycle, run.ID, run.Name, d, HUMIDITY_DELTA_THRESHOLD,
						INFLUX_DB_CONFIG.Name, INFLUX_DB_CONFIG.Name, run.UID,
					)
					fmt.Print(text)
					msg := tgbotapi.NewMessageToChannel(botapi.TELEGRAM_BOT_CHANNEL_USERNAME, text)
					msg.ParseMode = tgbotapi.ModeHTML
					_, err := bot.Send(msg)
					if err != nil {
						log.Println(err)
					}
				}
			}
			previousFrames = currentFrames
			if item.TimestampEnd > 0 {
				VERIFIED_ITEMS_MAP[itemRef] = itemRef
				err = clouddb.WriteCache(filepath.Join("storage", "verified_items_map.gob"), VERIFIED_ITEMS_MAP)
				if err != nil {
					return err
				}
			}
		}
		LAST_CHECKED_RUN = run
	}
	log.Println("Checks completed!")
	return err
}

func main() {

	tsdbRef, err := clouddb.HashString(fmt.Sprintf("%s_%s_%s", INFLUX_DB_CONFIG.URL, INFLUX_DB_CONFIG.Org, INFLUX_DB_CONFIG.Token))
	if err != nil {
		log.Fatalln(err)
	}

	api := influxdb2.NewClientWithOptions(
		INFLUX_DB_CONFIG.URL,
		INFLUX_DB_CONFIG.Token,
		influxdb2.DefaultOptions().SetHTTPRequestTimeout(300).SetTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}),
	).QueryAPI(INFLUX_DB_CONFIG.Org)

	log.Println(tsdbRef, api)

	bot, updates := botapi.InitBot()

	go func() {
		for update := range updates {
			if update.Message != nil {
				if update.Message.IsCommand() {
					switch update.Message.Command() {
					case "status":
						var text string
						text = fmt.Sprintf(
							"Monitoring runs for humidity aberrations on <b>%s</b>'s TSDB: <code>%sorgs/%s</code>\nLast check: %s ago (%s)\nHumidity value threshold: <b>%.2f</b>, Humidity delta threshold: <b>%.2f</b>\nLast restart: %s ago (%s)\n",
							strings.ToUpper(INFLUX_DB_CONFIG.Name),
							INFLUX_DB_CONFIG.URL,
							INFLUX_DB_CONFIG.Org,
							time.Since(LAST_CHECK_TIME).Round(time.Second), LAST_CHECK_TIME.Format(time.RFC822),
							HUMIDITY_VALUE_THRESHOLD,
							HUMIDITY_DELTA_THRESHOLD,
							time.Since(START_TIME).Round(time.Second),
							START_TIME.Format(time.RFC822),
						)
						if LAST_CHECKED_RUN == nil {
							text += "<b>No runs checked so far</b>"
						} else {
							text += fmt.Sprintf(
								"Last checked run: <b>%s (%s)</b>",
								LAST_CHECKED_RUN.ID, LAST_CHECKED_RUN.Name,
							)
						}
						msg := tgbotapi.NewMessage(update.Message.From.ID, text)
						msg.ReplyToMessageID = update.Message.MessageID
						msg.ParseMode = tgbotapi.ModeHTML
						_, err = bot.Send(msg)
						if err != nil {
							log.Println(err)
						}
					}
				}
			}
		}
	}()

	for {
		err = PerformChecks(tsdbRef, api, bot)
		if err != nil {
			log.Println(err)
		}
		LAST_CHECK_TIME = time.Now()
		time.Sleep(time.Minute * 1)
	}

}
