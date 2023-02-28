package botapi

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"os"
)

var (
	SMTP_SERVERNAME  string
	SMTP_USERNAME string
	SMTP_PASSWORD string
	SMTP_RECEPIENT string
)
func init() {
	SMTP_SERVERNAME = os.Getenv("SMTP_SERVERNAME")
	if SMTP_SERVERNAME == "" {
		log.Fatalln("no SMTP_SERVERNAME env var provided")
	}

	SMTP_USERNAME = os.Getenv("SMTP_USERNAME")
	if SMTP_USERNAME == "" {
		log.Fatalln("no SMTP_USERNAME env var provided")
	}

	SMTP_PASSWORD = os.Getenv("SMTP_PASSWORD")
	if SMTP_PASSWORD == "" {
		log.Fatalln("no SMTP_PASSWORD env var provided")
	}

	SMTP_RECEPIENT = os.Getenv("SMTP_RECEPIENT")
	if SMTP_RECEPIENT == "" {
		log.Fatalln("no SMTP_RECEPIENT env var provided")
	}
}


type loginAuth struct {
    username, password string
}

func LoginAuth(username, password string) smtp.Auth {
    return &loginAuth{username, password}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
    return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
    if more {
        switch string(fromServer) {
        case "Username:":
            return []byte(a.username), nil
        case "Password:":
            return []byte(a.password), nil
        default:
            return nil, errors.New("unknown from server")
        }
    }
    return nil, nil
}

func SendEmail(body string) error {

    host, _,  _ := net.SplitHostPort(SMTP_SERVERNAME)
	
	auth := LoginAuth(SMTP_USERNAME, SMTP_PASSWORD)

    // TLS config
    tlsconfig := &tls.Config {
        InsecureSkipVerify: true,
        ServerName: host,
    }

	c, err := smtp.Dial(SMTP_SERVERNAME)
	if err != nil {
		return err
	}

	err = c.StartTLS(tlsconfig)
	if err != nil {
		return err
	}

    // Auth
    if err = c.Auth(auth); err != nil {
        return err
    }

	// Set the sender and recipient first
	if err := c.Mail(SMTP_USERNAME); err != nil {
		return err
	}
	if err := c.Rcpt(SMTP_RECEPIENT); err != nil {
		return err
	}

	// Send the email body.
	wc, err := c.Data()
	if err != nil {
		return err
	}

	// _, err = fmt.Fprintf(wc, "This is the email body")
	_, err = wc.Write([]byte(fmt.Sprintf("Subject: Run Sentinel Bot Alert\r\n\r\n%s", body)))
	if err != nil {
		return err
	}
	err = wc.Close()
	if err != nil {
		return err
	}

	// Send the QUIT command and close the connection.
	err = c.Quit()
	if err != nil {
		return err
	}
	return err
}