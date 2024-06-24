package main

import (
	"dbb-proxy/internal"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"net"
	"os"
)

func main() {
	logrus.SetFormatter(new(logrus.JSONFormatter))

	if err := godotenv.Load(); err != nil {
		logrus.Fatalf("Error loading .env file: %s", err.Error())
	}

	proxyBindAddr := os.Getenv("PROXY_BIND_ADDR")
	if proxyBindAddr == "" {
		logrus.Fatalf("PROXY_BIND_ADDR env var not set")
	}

	listener, err := net.Listen("tcp", ":"+proxyBindAddr)
	if err != nil {
		logrus.Fatalf("Can't start listen on port: %v", err)
	}
	defer listener.Close()

	logrus.Infof("Proxy started at: %s", proxyBindAddr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("Can't accept connection: %v", err)
			continue
		}
		logrus.Infof("Connection opened with: %v", clientConn.RemoteAddr())
		go func() {
			errConn := internal.HandleConnection(clientConn)
			if errConn != nil {
				logrus.Errorf("Can't handle connection: %v", err)
				return
			}
		}()
	}
}
