// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/chstore"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// ErrUsage: the command line or configuration is wrong; nothing was done.
var ErrUsage = errors.New("usage")

// Config is the connection and the databases (§2), from defaults, then a JSON
// file, then FUSESCOPE_SCHEMA_* variables, then flags.
type Config struct {
	Conn        chstore.Config
	Databases   map[string]string
	LockTimeout time.Duration
}

// fileConfig is the JSON file. It has no password field: an inline password
// is refused (ADR 0004), as is any unknown key.
type fileConfig struct {
	Endpoint     *string `json:"endpoint"`
	Username     *string `json:"username"`
	PasswordFile *string `json:"password_file"`
	TLS          struct {
		Insecure           *bool   `json:"insecure"`
		CAFile             *string `json:"ca_file"`
		InsecureSkipVerify *bool   `json:"insecure_skip_verify"`
	} `json:"tls"`
	DialTimeout *string           `json:"dial_timeout"`
	Cluster     *string           `json:"cluster"`
	Databases   map[string]string `json:"databases"`
	LockTimeout *string           `json:"lock_timeout"`
}

const envPrefix = "FUSESCOPE_SCHEMA_"

// setting is one connection setting, by flag name; its variable is the name
// upper-cased with '-' as '_', after envPrefix.
type setting struct {
	name, help string
	set        func(string) error
	boolean    bool
}

func usageErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrUsage, fmt.Sprintf(format, a...))
}

// load reads the configuration, then parses args with the connection flags
// and each command's own, returning what follows the flags.
func load(args []string, getenv func(string) string, register ...func(*flag.FlagSet)) (Config, []string, error) {
	cfg := Config{
		Conn:        chstore.Config{Username: "default", Cluster: "cluster", DialTimeout: 10 * time.Second},
		Databases:   map[string]string{},
		LockTimeout: 10 * time.Minute,
	}
	for role, db := range schema.DefaultDatabases {
		cfg.Databases[role] = db
	}
	if getenv(envPrefix+"PASSWORD") != "" {
		return cfg, nil, usageErr("%sPASSWORD is refused: give the password in a file, with --password-file (ADR 0004)", envPrefix)
	}
	duration := func(d *time.Duration) func(string) error {
		return func(v string) error {
			p, err := time.ParseDuration(v)
			if err != nil || p <= 0 {
				return fmt.Errorf("%q is not a positive duration", v)
			}
			*d = p
			return nil
		}
	}
	str := func(s *string) func(string) error { return func(v string) error { *s = v; return nil } }
	boolean := func(b *bool) func(string) error {
		return func(v string) error {
			p, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("%q is not a boolean", v)
			}
			*b = p
			return nil
		}
	}
	settings := []setting{
		{name: "endpoint", help: "ClickHouse native protocol endpoint, host:port", set: str(&cfg.Conn.Endpoint)},
		{name: "username", help: "ClickHouse user", set: str(&cfg.Conn.Username)},
		{name: "password-file", help: "file holding the ClickHouse password", set: str(&cfg.Conn.PasswordFile)},
		{name: "tls-insecure", help: "plaintext, by explicit opt-in (ADR 0007)", set: boolean(&cfg.Conn.TLS.Insecure), boolean: true},
		{name: "tls-ca-file", help: "CA bundle verifying the server", set: str(&cfg.Conn.TLS.CAFile)},
		{name: "tls-insecure-skip-verify", help: "do not verify the server's certificate (the chart's verify: false)", set: boolean(&cfg.Conn.TLS.InsecureSkipVerify), boolean: true},
		{name: "dial-timeout", help: "connection timeout", set: duration(&cfg.Conn.DialTimeout)},
		{name: "cluster", help: "the cluster in remote_servers", set: str(&cfg.Conn.Cluster)},
		{name: "lock-timeout", help: "how long migrate and repair wait for the lease", set: duration(&cfg.LockTimeout)},
	}
	for _, role := range schema.Roles {
		settings = append(settings, setting{name: "database-" + role, help: "the " + role + " database", set: func(v string) error { cfg.Databases[role] = v; return nil }})
	}

	fs := flag.NewFlagSet("fusescope-schema", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configFile := fs.String("config", getenv(envPrefix+"CONFIG"), "JSON configuration file")
	flagged := map[string]string{}
	for _, s := range settings {
		fs.Func(s.name, s.help, func(v string) error { flagged[s.name] = v; return nil })
	}
	for _, r := range register {
		r(fs)
	}
	if err := fs.Parse(normaliseBools(args, settings)); err != nil {
		return cfg, nil, usageErr("%v", err)
	}

	if *configFile != "" {
		if err := applyFile(*configFile, &cfg, settings); err != nil {
			return cfg, nil, err
		}
	}
	for _, s := range settings {
		if v := getenv(envPrefix + strings.ToUpper(strings.ReplaceAll(s.name, "-", "_"))); v != "" {
			if err := s.set(v); err != nil {
				return cfg, nil, usageErr("%s%s: %v", envPrefix, strings.ToUpper(strings.ReplaceAll(s.name, "-", "_")), err)
			}
		}
	}
	for _, s := range settings {
		if v, ok := flagged[s.name]; ok {
			if err := s.set(v); err != nil {
				return cfg, nil, usageErr("--%s: %v", s.name, err)
			}
		}
	}
	if cfg.Conn.Endpoint == "" {
		return cfg, nil, usageErr("no endpoint: set --endpoint or %sENDPOINT", envPrefix)
	}
	roles := map[string]string{}
	for _, role := range schema.Roles {
		db := cfg.Databases[role]
		if db == "" {
			return cfg, nil, usageErr("the %s database is empty", role)
		}
		if other, dup := roles[db]; dup {
			return cfg, nil, usageErr("the %s and %s databases are both %s", other, role, db)
		}
		roles[db] = role
	}
	return cfg, fs.Args(), nil
}

// normaliseBools lets a boolean flag stand alone, --tls-insecure meaning
// --tls-insecure=true.
func normaliseBools(args []string, settings []setting) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		name := strings.TrimLeft(a, "-")
		if strings.HasPrefix(a, "-") && !strings.Contains(name, "=") && slices.ContainsFunc(settings, func(s setting) bool { return s.boolean && s.name == name }) {
			a += "=true"
		}
		out = append(out, a)
	}
	return out
}

func applyFile(path string, cfg *Config, settings []setting) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return usageErr("read the configuration file: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return usageErr("the configuration file is not a JSON object: %v", err)
	}
	if _, ok := raw["password"]; ok {
		return usageErr("the configuration file sets a password: give it in a file, with password_file (ADR 0004)")
	}
	var fc fileConfig
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return usageErr("the configuration file: %v", err)
	}
	set := func(name string, v *string) error {
		if v == nil {
			return nil
		}
		i := slices.IndexFunc(settings, func(s setting) bool { return s.name == name })
		if err := settings[i].set(*v); err != nil {
			return usageErr("the configuration file's %s: %v", name, err)
		}
		return nil
	}
	b2s := func(b *bool) *string {
		if b == nil {
			return nil
		}
		s := strconv.FormatBool(*b)
		return &s
	}
	for name, v := range map[string]*string{
		"endpoint": fc.Endpoint, "username": fc.Username, "password-file": fc.PasswordFile,
		"tls-insecure": b2s(fc.TLS.Insecure), "tls-ca-file": fc.TLS.CAFile, "tls-insecure-skip-verify": b2s(fc.TLS.InsecureSkipVerify),
		"dial-timeout": fc.DialTimeout, "cluster": fc.Cluster, "lock-timeout": fc.LockTimeout,
	} {
		if err := set(name, v); err != nil {
			return err
		}
	}
	for role, db := range fc.Databases {
		if !slices.Contains(schema.Roles, role) {
			return usageErr("the configuration file names a database for %q, not one of %v", role, schema.Roles)
		}
		cfg.Databases[role] = db
	}
	return nil
}
