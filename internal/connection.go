package internal

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
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

	// get StartupMessage params
	params, err := getInitialConnectionData(clientConn)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't get initial connection data: %v", err))
	}

	datasourceId, err := strconv.Atoi(params["database"])
	if err != nil {
		return errors.New(fmt.Sprintf("DatasourceId is not a number: %v", err))
	}

	// Get connection info from the server
	connectionInfo, err := getConnectionData(params["user"], datasourceId)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't get connection data: %v", err))
	}

	if connectionInfo.Host == "" || connectionInfo.Port == 0 ||
		connectionInfo.User == "" || connectionInfo.Password == "" ||
		connectionInfo.Name == "" {
		return errors.New(fmt.Sprintf("Empty connection info: %+v", connectionInfo))
	}

	// send new startup message
	params["user"] = connectionInfo.User
	params["database"] = connectionInfo.Name

	newStartupMessage, err := createStartUpMessage(params)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't create startup message: %v", err))
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

	_, err = psqlConn.Write(newStartupMessage)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't send startup message: %v", err))
	}

	err = handleAuthConnection(clientConn, psqlConn, connectionInfo.User, connectionInfo.Password)
	if err != nil {
		return err
	}

	go func() {
		err := pipe(psqlConn, clientConn, true)
		if err != nil {
			return
		}
	}()
	err = pipe(clientConn, psqlConn, false)
	if err != nil {
		return err
	}
	return nil
}

func pipe(dst net.Conn, src net.Conn, send bool) error {
	if send {
		err := intercept(src, dst)
		if err != nil {
			return err
		}
		return nil
	}

	_, err := io.Copy(dst, src)
	if err != nil {
		return err
	}
	return nil
}

func intercept(src, dst net.Conn) error {
	buffer := make([]byte, 4096)

	for {
		n, err := src.Read(buffer)
		if err != nil {
			return errors.New("Can't read from source connection: " + err.Error())
		}

		_, err = dst.Write(buffer[:n])
		if err != nil {
			return errors.New("Can't write to destination connection: " + err.Error())
		}
	}
}
