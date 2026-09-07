package mtglib

import (
	"fmt"
	"slices"
	"time"
)

// ProxyOpts is a structure with settings to mtg proxy.
//
// This is not required per se, but this is to shorten function signature and
// give an ability to conveniently provide default values.
type ProxyOpts struct {
	// Secret defines a secret which should be used by a proxy.
	//
	// This is a mandatory setting, unless Secrets is populated.
	Secret Secret

	// Secrets defines a set of secrets this proxy accepts, keyed by an
	// opaque, caller-chosen ID (e.g. a user ID). This is a
	// Free-Guy-IR/PasarGuard addition: upstream mtg deliberately supports
	// only a single secret per proxy. When this is non-empty, it takes
	// priority over Secret and the proxy runs in multi-secret mode: an
	// incoming connection is matched against every entry until one
	// succeeds, and the matched ID is attached to the stream (surfaced via
	// EventAuthenticated) so callers can attribute EventTraffic byte counts
	// per secret/user.
	//
	// In multi-secret mode, DomainFrontingHost must be set explicitly
	// (there is no single secret's Host to fall back on for the domain
	// fronting decoy target).
	//
	// This is an optional setting.
	Secrets map[string]Secret

	// Network defines a network instance which should be used for all network
	// communications made by proxies.
	//
	// This is a mandatory setting.
	Network Network

	// AntiReplayCache defines an instance of antireplay cache.
	//
	// This is a mandatory setting.
	AntiReplayCache AntiReplayCache

	// IPBlocklist defines an instance of IP blocklist.
	//
	// This is a mandatory setting.
	IPBlocklist IPBlocklist

	// IPAllowlist defines a whitelist of IPs to allow to use proxy.
	//
	// This is an optional setting, ignored by default (no restrictions).
	IPAllowlist IPBlocklist

	// EventStream defines an instance of event stream.
	//
	// This ia a mandatory setting.
	EventStream EventStream

	// Logger defines an instance of the logger.
	//
	// This is a mandatory setting.
	Logger Logger

	// BufferSize is a size of the copy buffer in bytes.
	//
	// Please remember that we multiply this number in 2, because when we relay
	// between proxies, we have to create 2 intermediate buffers: to and from.
	//
	// This is an optional setting.
	//
	// Deprecated: this setting is no longer makes any effect.
	BufferSize uint

	// Concurrency is a size of the worker pool for connection management.
	//
	// If we have more connections than this number, they are going to be
	// rejected.
	//
	// This is an optional setting.
	Concurrency uint

	// IdleTimeout is a timeout for relay when we have to break a stream.
	//
	// This is a timeout for any activity. So, if we have any message which will
	// pass to either direction, a timer is reset. If we have no any reads or
	// writes for this timeout, a connection will be aborted.
	//
	// This is an optional setting.
	IdleTimeout time.Duration

	// HandshakeTimeout is a timeout during which all handshake ceremonies must
	// be completed, otherwise this process will be aborted
	//
	// This is an optional setting.
	HandshakeTimeout time.Duration

	// TolerateTimeSkewness is a time boundary that defines a time range where
	// faketls timestamp is acceptable.
	//
	// This means that if if you got a timestamp X, now is Y, then if |X-Y| <
	// TolerateTimeSkewness, then you accept a packet.
	//
	// This is an optional setting.
	TolerateTimeSkewness time.Duration

	// PreferIP defines an IP connectivity preference. Valid values are:
	// 'prefer-ipv4', 'prefer-ipv6', 'only-ipv4', 'only-ipv6'.
	//
	// This is an optional setting.
	PreferIP string

	// AutoUpdate defines if it is required to auto update proxy list from
	// Telegram instead of relying on a hardcoded list.
	//
	// This is an optional setting.
	AutoUpdate bool

	// DomainFrontingPort is a port we use to connect to a fronting domain.
	//
	// This is required because secret does not specify a port. It specifies a
	// hostname only.
	//
	// This is an optional setting.
	DomainFrontingPort uint

	// DomainFrontingHost is the address to use when connecting to the
	// fronting domain instead of resolving the hostname from the secret via
	// DNS. It can be a literal IP or a hostname; hostnames are resolved at
	// dial time via the native dialer (which honours dual-stack and Happy
	// Eyeballs).
	//
	// This is useful when DNS resolution of the secret's hostname is blocked
	// or loops back to this server. The hostname from the secret is still
	// used for SNI in the TLS handshake.
	//
	// This is an optional setting.
	DomainFrontingHost string

	// DomainFrontingIP previously held the dial target for the fronting
	// domain. The setting is no longer honoured: setting it logs a warning
	// at proxy startup and the value is dropped.
	//
	// Deprecated: use DomainFrontingHost. Setting this field has no effect.
	DomainFrontingIP string

	// DomainFrontingProxyProtocol is used if communication between upstream
	// endpoint and mtg supports proxy protocol. This is useful in case
	// if mtg is also placed behind load balancer, and this will make
	// fronting webserver to know about real IP addresses
	//
	// This is an optional setting.
	DomainFrontingProxyProtocol bool

	// AllowFallbackOnUnknownDC defines how proxy behaves if unknown DC was
	// requested. If this setting is set to false, then such connection will be
	// rejected. Otherwise, proxy will chose any DC.
	//
	// Telegram is designed in a way that any DC can serve any request, the
	// problem is a latency.
	//
	// This is an optional setting.
	AllowFallbackOnUnknownDC bool

	// UseTestDCs defines if we have to connect to production or to staging DCs of
	// Telegram.
	//
	// This is required if you use mtglib as an integration library for your
	// Telegram-related projects.
	//
	// This is an optional setting.
	//
	// OBSOLETE and DEPRECATED. Ignored.
	UseTestDCs bool

	// DCOverrides defines a set of IP addresses that should be used
	// with a higher priority to those that are calculated somehow by mtg.
	//
	// OBSOLETE and DEPRECATED. Ignored.
	DCOverrides map[int][]string

	// DoppelGangerURLs is a list of URLs that should be crawled by
	// mtg to calculate parameters for statistical distribution of a
	// traffic for fronting domains. If nothing is given, then predefined
	// statistics is going to be used.
	DoppelGangerURLs []string

	// DoppelGangerPerRaid defines how many time each URL from
	// DoppelGangerURLs list should be crawled per raid. We recommend to
	// have this number ~10.
	DoppelGangerPerRaid uint

	// DoppelGangerEach defines a time period between each raid. We recommend
	// to use hours here.
	DoppelGangerEach time.Duration

	// DoppelGangerDRS defines if TLS Dynamic Record Sizing is active.
	DoppelGangerDRS bool

	PlainMode bool

	FakeTLSDomains []string
}

func (p ProxyOpts) fakeTLSHostnames() []string {
	if len(p.FakeTLSDomains) > 0 {
		return slices.Clone(p.FakeTLSDomains)
	}

	if p.DomainFrontingHost != "" {
		return []string{p.DomainFrontingHost}
	}

	if p.Secret.Host != "" {
		return []string{p.Secret.Host}
	}

	return nil
}

func (p ProxyOpts) valid() error {
	switch {
	case p.Network == nil:
		return ErrNetworkIsNotDefined
	case p.AntiReplayCache == nil:
		return ErrAntiReplayCacheIsNotDefined
	case p.IPBlocklist == nil:
		return ErrIPBlocklistIsNotDefined
	case p.IPAllowlist == nil:
		return ErrIPAllowlistIsNotDefined
	case p.EventStream == nil:
		return ErrEventStreamIsNotDefined
	case p.Logger == nil:
		return ErrLoggerIsNotDefined
	}

	// A non-nil-but-empty Secrets map is a legitimate multi-secret-mode state
	// (a proxy that has started with zero users synced yet) - it must not
	// fall through to the single-secret Secret validation below, which would
	// wrongly require an unused Secret field to be populated too.
	if p.Secrets != nil {
		if len(p.FakeTLSDomains) > 0 {
			for id, secret := range p.Secrets {
				if secret.Key == secretEmptyKey {
					return fmt.Errorf("%w: id=%s", ErrSecretInvalid, id)
				}
			}

			return nil
		}

		if p.PlainMode {
			for id, secret := range p.Secrets {
				if secret.Key == secretEmptyKey {
					return fmt.Errorf("%w: id=%s", ErrSecretInvalid, id)
				}
			}

			return nil
		}

		if p.DomainFrontingHost == "" {
			return ErrDomainFrontingHostRequiredForMultiSecret
		}

		for id, secret := range p.Secrets {
			if !secret.Valid() {
				return fmt.Errorf("%w: id=%s", ErrSecretInvalid, id)
			}
		}

		return nil
	}

	if !p.Secret.Valid() {
		return ErrSecretInvalid
	}

	return nil
}

func (p ProxyOpts) getConcurrency() int {
	if p.Concurrency == 0 {
		return DefaultConcurrency
	}

	return int(p.Concurrency)
}

func (p ProxyOpts) getDomainFrontingPort() int {
	if p.DomainFrontingPort == 0 {
		return DefaultDomainFrontingPort
	}

	return int(p.DomainFrontingPort)
}

func (p ProxyOpts) getTolerateTimeSkewness() time.Duration {
	if p.TolerateTimeSkewness == 0 {
		return DefaultTolerateTimeSkewness
	}

	return p.TolerateTimeSkewness
}

func (p ProxyOpts) getPreferIP() string {
	if p.PreferIP == "" {
		return DefaultPreferIP
	}

	return p.PreferIP
}

func (p ProxyOpts) getHandshakeTimeout() time.Duration {
	if p.HandshakeTimeout == 0 {
		return DefaultHandshakeTimeout
	}

	return p.HandshakeTimeout
}

func (p ProxyOpts) getIdleTimeout() time.Duration {
	if p.IdleTimeout == 0 {
		return DefaultIdleTimeout
	}

	return p.IdleTimeout
}

func (p ProxyOpts) getLogger(name string) Logger {
	return p.Logger.Named(name)
}
