package internal

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	KeepAliveTimeout = 30 * time.Minute
)

func HandleConnection(clientConn net.Conn) error {
	defer func(clientConn net.Conn) {
		err := clientConn.Close()
		if err != nil {
			logrus.Errorf("Error closing client connection: %v", err)
			return
		} else {
			logrus.Infof("Connection closed with: %v", clientConn.RemoteAddr())
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
		return errors.New(fmt.Sprintf("Error connecting to PostgreSQL: %v\n", err))
	}
	defer func(psqlConn net.Conn) {
		err := psqlConn.Close()
		if err != nil {
			logrus.Errorf("Can't close Postgres connection: %v", err)
			return
		}
	}(psqlConn)

	err = SetTimeout(psqlConn, clientConn, KeepAliveTimeout)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't set initial timeout: %v", err))
	}

	_, err = psqlConn.Write(newStartupMessage)
	if err != nil {
		return errors.New(fmt.Sprintf("Can't send startup message: %v", err))
	}

	// handle authentication
	err = handleAuthConnection(clientConn, psqlConn, connectionInfo.User, connectionInfo.Password)
	if err != nil {
		return err
	}

	// messaging between client and server
	go func() {
		errConn := pipe(psqlConn, clientConn, true)
		if errConn != nil {
			logrus.Error("From client to psql failed: ", err, " <- err err.Error() -> ", err.Error())
			return
		}
		return
	}()
	err = pipe(clientConn, psqlConn, false)
	if err != nil {
		logrus.Errorf("From psql to client failed: %v", err)
		return err
	}
	return nil
}

func SetTimeout(client, server net.Conn, timeout time.Duration) error {
	err := client.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return err
	}
	err = server.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return err
	}
	return nil
}

func pipe(dst net.Conn, src net.Conn, send bool) error {
	var err error
	if send {
		err = intercept(src, dst)
	} else {
		_, err = io.Copy(dst, src)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err := GracefulShutdown(dst, src)
			if err != nil {
				return err
			}
			return nil
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return errors.New(fmt.Sprintf("Can't copy data from %v to %v: %v", src.RemoteAddr(), dst.RemoteAddr(), err))
		}
	}
	return nil
}

func intercept(src, dst net.Conn) error {
	buffer := make([]byte, 4096)

	for {
		n, err := src.Read(buffer)
		if err != nil {
			return err
		}

		_, err = dst.Write(buffer[:n])
		if err != nil {
			return err
		}

		err = SetTimeout(src, dst, KeepAliveTimeout)
		if err != nil {
			return errors.New("Can't update timeout: " + err.Error())
		}
	}
}

func GracefulShutdown(conn1, conn2 net.Conn) error {
	err := SetTimeout(conn1, conn2, 0)
	if err != nil {
		return errors.New("Can't shutdown connections: " + err.Error())
	}
	logrus.Info("Gracefully shutting down connections")
	return nil
}
