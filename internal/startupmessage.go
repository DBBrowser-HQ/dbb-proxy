package internal

import (
	"bytes"
	"dbb-proxy/internal/model"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
)

func createStartUpMessage(params map[string]string) ([]byte, error) {
	var buffer bytes.Buffer

	err := binary.Write(&buffer, binary.BigEndian, uint32(196608))
	if err != nil {
		return nil, err
	}

	for key, value := range params {
		buffer.WriteString(key)
		buffer.WriteByte(0)
		buffer.WriteString(value)
		buffer.WriteByte(0)
	}

	buffer.WriteByte(0)

	messageLength := int32(buffer.Len() + 4)
	finalMessage := new(bytes.Buffer)
	err = binary.Write(finalMessage, binary.BigEndian, messageLength)
	if err != nil {
		return nil, err
	}
	finalMessage.Write(buffer.Bytes())

	return finalMessage.Bytes(), nil
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

func getInitialConnectionData(clientConn net.Conn) (map[string]string, error) {
	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error reading first packet: %v", err))
	}

	message := buf[4:n]
	protocolVersion := binary.BigEndian.Uint32(message[:4])

	if protocolVersion != 196608 {
		return nil, errors.New(fmt.Sprintf("Unsupported protocol version: %d", protocolVersion))
	}

	params := parseStartupMessage(message[4:])

	return params, nil
}
