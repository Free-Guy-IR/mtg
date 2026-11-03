package mtglib

import (
	"net"
	"testing"
)

func TestRemoteTCPIP(t *testing.T) {
	t.Parallel()

	var typedNil *net.TCPAddr

	cases := []struct {
		name string
		addr net.Addr
		want string
	}{
		{"nil interface", nil, ""},
		{"typed nil tcp", typedNil, ""},
		{"tcp without ip", &net.TCPAddr{Port: 443}, ""},
		{"tcp with empty ip", &net.TCPAddr{IP: net.IP{}, Port: 443}, ""},
		{"udp", &net.UDPAddr{IP: net.IPv4(192, 0, 2, 7), Port: 443}, ""},
		{"unix", &net.UnixAddr{Name: "/run/a", Net: "unix"}, ""},
		{"ip", &net.IPAddr{IP: net.IPv4(192, 0, 2, 7)}, ""},
		{"tcp4", &net.TCPAddr{IP: net.IPv4(192, 0, 2, 7), Port: 443}, "192.0.2.7"},
		{"tcp6", &net.TCPAddr{IP: net.ParseIP("2001:db8::7"), Port: 443}, "2001:db8::7"},
	}

	for _, tc := range cases {
		got := remoteTCPIP(tc.addr)

		switch {
		case tc.want == "" && got != nil:
			t.Errorf("%s: got %v, want nil", tc.name, got)
		case tc.want != "" && got.String() != tc.want:
			t.Errorf("%s: got %v, want %s", tc.name, got, tc.want)
		}
	}
}
