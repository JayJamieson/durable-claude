package main

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestServiceUnitRunsPushAllThenExtraArgs(t *testing.T) {
	u, err := newUnit([]string{"-prefix", "/vm"})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(u.argv[0]) {
		t.Errorf("argv[0] = %q, want an absolute path to the dcs binary", u.argv[0])
	}
	if want := []string{"push", "-all", "-prefix", "/vm"}; !slices.Equal(u.argv[1:], want) {
		t.Errorf("args = %q, want %q", u.argv[1:], want)
	}
	if u.user == "" || u.home == "" {
		t.Errorf("unit = %+v, want a user and home", u)
	}
}

func TestRenderSystemd(t *testing.T) {
	u := unit{
		user: "ubuntu", home: "/home/ubuntu",
		argv: []string{"/usr/local/bin/dcs", "push", "-all", "-dir", `/data/my "claude"`},
	}
	got := renderSystemd(u)
	for _, line := range []string{
		"Wants=network-online.target",
		"After=network-online.target",
		"User=ubuntu",
		"Environment=HOME=/home/ubuntu",
		`ExecStart=/usr/local/bin/dcs push -all -dir "/data/my \"claude\""`,
		"Restart=always",
		"RestartSec=5",
		"WantedBy=multi-user.target",
	} {
		if !slices.Contains(strings.Split(got, "\n"), line) {
			t.Errorf("unit is missing the line %q:\n%s", line, got)
		}
	}
}
