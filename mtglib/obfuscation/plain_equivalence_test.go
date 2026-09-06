package obfuscation

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestHandshakeFromFrameEquivalentToReadHandshake(t *testing.T) {
	secret := make([]byte, 16)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	client, err := Obfuscator{Secret: secret}.SendHandshake(&captureConn{frameConn: frameConn{r: bytes.NewReader(nil)}, buf: &wire}, 2)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello from telegram client")
	payload := append([]byte{}, plaintext...)
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}

	frameBytes := wire.Bytes()[:hfLen]
	encrypted := wire.Bytes()[hfLen:]

	rh := frameConn{r: bytes.NewReader(append(append([]byte{}, frameBytes...), encrypted...))}
	dc1, conn1, err := Obfuscator{Secret: secret}.ReadHandshake(rh)
	if err != nil {
		t.Fatal(err)
	}
	out1 := make([]byte, len(plaintext))
	n1, err := conn1.Read(out1)
	if err != nil {
		t.Fatal(err)
	}
	out1 = out1[:n1]

	frame, err := ReadHandshakeFrame(bytes.NewReader(frameBytes))
	if err != nil {
		t.Fatal(err)
	}
	ff := frameConn{r: bytes.NewReader(encrypted)}
	dc2, conn2, ok := Obfuscator{Secret: secret}.HandshakeFromFrame(frame, ff)
	if !ok {
		t.Fatal("no match")
	}
	out2 := make([]byte, len(plaintext))
	n2, err := conn2.Read(out2)
	if err != nil {
		t.Fatal(err)
	}
	out2 = out2[:n2]

	if dc1 != dc2 || dc1 != 2 {
		t.Fatalf("dc mismatch %d %d", dc1, dc2)
	}
	if !bytes.Equal(out1, out2) {
		t.Fatalf("decrypt mismatch\n old=%x (n=%d)\n new=%x (n=%d)", out1, n1, out2, n2)
	}
	if !bytes.Equal(out1, plaintext) {
		t.Fatalf("plaintext mismatch %x (n=%d) want %x", out1, n1, plaintext)
	}

	var s1, s2 bytes.Buffer
	c1 := conn1.(conn)
	c2 := conn2.(conn)
	c1.Conn = &captureConn{frameConn: frameConn{r: bytes.NewReader(nil)}, buf: &s1}
	c2.Conn = &captureConn{frameConn: frameConn{r: bytes.NewReader(nil)}, buf: &s2}
	if _, err := c1.Write([]byte("server->client")); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Write([]byte("server->client")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.Bytes(), s2.Bytes()) {
		t.Fatalf("send cipher streams differ")
	}
}
