package internal

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

func handleAuthConnection(clientConn net.Conn, psqlConn net.Conn, user, password string) error {
	buf := make([]byte, 1024)

	for {
		n, err := psqlConn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = GracefulShutdown(clientConn, psqlConn)
				if err != nil {
					return err
				}
				return nil
			}
			return err
		}

		// interception of AuthenticationMD5Password message
		if buf[0] == 'R' && binary.BigEndian.Uint32(buf[1:5]) == 12 && binary.BigEndian.Uint32(buf[5:9]) == 5 {
			salt := make([]byte, 4)
			copy(salt, buf[9:13])

			_, err = clientConn.Write(buf[:n])
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = GracefulShutdown(clientConn, psqlConn)
					if err != nil {
						return err
					}
					return nil
				}
				return errors.New(fmt.Sprintf("Can't send request for the password to client: %v", err.Error()))
			}

			n, err = clientConn.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = GracefulShutdown(clientConn, psqlConn)
					if err != nil {
						return err
					}
					return nil
				}
				return errors.New(fmt.Sprintf("Can't get the password from client: %v", err.Error()))
			}

			newPasswordHash := md5PasswordHash(password, user, salt)
			newPasswordMessage := createPasswordMessage(newPasswordHash)

			_, err = psqlConn.Write(newPasswordMessage)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = GracefulShutdown(clientConn, psqlConn)
					if err != nil {
						return err
					}
					return nil
				}
				return errors.New(fmt.Sprintf("Can't send the password to psql: %v", err.Error()))
			}
			return nil
		}
	}
}

func md5PasswordHash(password, user string, salt []byte) string {
	step1 := md5.Sum([]byte(password + user))
	step1Hex := hex.EncodeToString(step1[:])
	step2 := md5.Sum(append([]byte(step1Hex), salt...))

	return "md5" + hex.EncodeToString(step2[:])
}

func createPasswordMessage(passwordHash string) []byte {
	var buffer bytes.Buffer
	buffer.WriteByte('p')
	messageLength := int32(len(passwordHash) + 5)
	err := binary.Write(&buffer, binary.BigEndian, messageLength)
	if err != nil {
		logrus.Error("Can't write message length to buffer: %v", err)
		return nil
	}
	buffer.WriteString(passwordHash)
	buffer.WriteByte(0)
	return buffer.Bytes()
}
