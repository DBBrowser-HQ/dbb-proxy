package internal

import (
	"bytes"
	"dbb-proxy/internal/model"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
)

func HandleConnection(clientConn net.Conn) error {
	defer func(clientConn net.Conn) {
		err := clientConn.Close()
		if err != nil {
			logrus.Errorf("Can't close client connection: %v", err)
			return
		}
	}(clientConn)

	accessToken, datasourceId, err := getInitialConnectionData(clientConn)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't get initial connection data: %v", err))
	}

	connectionInfo, err := getConnectionData(accessToken, datasourceId)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't get connection data: %v", err))
	}

	logrus.Infof("Connection info: %+v", connectionInfo)

	if connectionInfo.Host == "" || connectionInfo.Port == 0 ||
		connectionInfo.User == "" || connectionInfo.Password == "" ||
		connectionInfo.Name == "" {
		return errors.New(fmt.Sprintf("Empty connection info: %+v", connectionInfo))
	}

	psqlConn, err := net.Dial("tcp", net.JoinHostPort(connectionInfo.Host, strconv.Itoa(connectionInfo.Port)))
	if err != nil {
		return errors.New(fmt.Sprintf("Ошибка при подключении к PostgreSQL серверу: %v\n", err))
	}
	defer func(psqlConn net.Conn) {
		err := psqlConn.Close()
		if err != nil {
			logrus.Errorf("Can't close Postgres connection: %v", err)
			return
		}
	}(psqlConn)

	// from Client to Postgres
	go func() {
		_, err := io.Copy(psqlConn, clientConn)
		if err != nil {
			logrus.Errorf("Error copy data from Client to DB: %v", err)
			return
		}
	}()

	// from Postgres to Client
	if _, err = io.Copy(clientConn, psqlConn); err != nil {
		logrus.Errorf("Error copy data from DB to Client: %v", err)
	}
	return nil
}

func parseStartupMessage(data []byte) map[string]string {
	params := make(map[string]string)
	buffer := bytes.NewBuffer(data)

	for {
		key, err := buffer.ReadString(0)
		if err != nil {
			break
		}
		value, err := buffer.ReadString(0)
		if err != nil {
			break
		}
		key = key[:len(key)-1]
		value = value[:len(value)-1]

		params[key] = value
	}

	return params
}

func getConnectionData(accessToken string, datasourceId int) (model.ConnectionInfo, error) {
	serverBindAddr := os.Getenv("SERVER_BIND_ADDR")
	if serverBindAddr == "" {
		return model.ConnectionInfo{}, errors.New("SERVER_BIND_ADDR env var not set")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return model.ConnectionInfo{}, errors.New("SERVER_HOST env var not set")
	}

	requestUrl := "http://" + net.JoinHostPort(serverHost, serverBindAddr) + "/connect/" + strconv.Itoa(datasourceId)
	logrus.Infof("Request URL: %s", requestUrl)

	request, err := http.NewRequest(http.MethodGet, requestUrl, nil)
	if err != nil {
		return model.ConnectionInfo{}, errors.New("Can't create request: " + err.Error())
	}
	request.Header.Add("Authorization", "Bearer "+accessToken)

	var connectionInfo model.ConnectionInfo
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return model.ConnectionInfo{}, errors.New("Can't do request: " + err.Error())
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return model.ConnectionInfo{}, errors.New("Can't read response body: " + err.Error())
	}
	defer response.Body.Close()
	if err := json.Unmarshal(body, &connectionInfo); err != nil {
		return model.ConnectionInfo{}, errors.New("Can't unmarshal response body: " + err.Error())
	}
	return connectionInfo, nil
}

func getInitialConnectionData(clientConn net.Conn) (string, int, error) {
	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		return "", 0, errors.New(fmt.Sprintf("Error reading first packet: %v", err))
	}

	message := buf[4:n]
	protocolVersion := binary.BigEndian.Uint32(message[:4])

	if protocolVersion != 196608 {
		return "", 0, errors.New(fmt.Sprintf("Unsupported protocol version: %d", protocolVersion))
	}

	params := parseStartupMessage(message[4:])
	datasourceId, err := strconv.Atoi(params["database"])
	if err != nil {
		return "", 0, err
	}
	return params["user"], datasourceId, nil
}
