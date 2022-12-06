package botapi

import (
	"log"
	"os"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var (
	TELEGRAM_BOT_TOKEN            string
	TELEGRAM_BOT_CHANNEL_USERNAME string
)

func init() {
	TELEGRAM_BOT_TOKEN = os.Getenv("TELEGRAM_BOT_TOKEN")
	if TELEGRAM_BOT_TOKEN == "" {
		log.Fatalln("no TELEGRAM_BOT_TOKEN env var provided")
	}
	TELEGRAM_BOT_CHANNEL_USERNAME = os.Getenv("TELEGRAM_BOT_CHANNEL_USERNAME")
	if TELEGRAM_BOT_CHANNEL_USERNAME == "" {
		log.Fatalln("no TELEGRAM_BOT_CHANNEL_USERNAME env var provided")
	}
}

func InitBot() (*tgbotapi.BotAPI, tgbotapi.UpdatesChannel) {
	bot, err := tgbotapi.NewBotAPI(TELEGRAM_BOT_TOKEN)
	if err != nil {
		log.Fatalln(err)
	}

	// bot.Debug = true

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	return bot, bot.GetUpdatesChan(u)
}
