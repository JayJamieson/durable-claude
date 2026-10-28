package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
)

const serviceUsage = `usage: dcs service install   [-- extra push args]   (with sudo)
       dcs service uninstall                        (with sudo)
       dcs service print     [-- extra push args]`

// serviceName is the systemd unit that runs dcs push -all from boot.
const serviceName = "dcs-push"

const unitPath = "/etc/systemd/system/" + serviceName + ".service"

// unit is what the systemd unit needs: what to run, and as whom.
type unit struct {
	argv []string // the dcs binary, then its arguments
	user string
	home string
}

func cmdService(_ context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New(serviceUsage)
	}
	action, extra := args[0], args[1:]
	if len(extra) > 0 && extra[0] == "--" {
		extra = extra[1:]
	}
	if runtime.GOOS != "linux" && action != "print" {
		return fmt.Errorf("dcs service needs systemd, which %s does not have", runtime.GOOS)
	}
	u, err := newUnit(extra)
	if err != nil {
		return err
	}

	switch action {
	case "print":
		fmt.Fprintln(os.Stderr, dim(unitPath))
		fmt.Print(renderSystemd(u))
		return nil
	case "install":
		if err := installSystemd(u); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "%s %s, running as %s\n", bold("installed"), unitPath, u.user)
		return nil
	case "uninstall":
		if err := uninstallSystemd(); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "%s %s\n", bold("removed"), unitPath)
		return nil
	default:
		return fmt.Errorf("unknown action %q\n%s", action, serviceUsage)
	}
}

// newUnit runs this dcs binary as the user who invoked sudo, if anyone did, so
// it reads that user's ~/.claude/projects rather than root's.
func newUnit(extra []string) (unit, error) {
	exe, err := os.Executable()
	if err == nil {
		exe, err = filepath.EvalSymlinks(exe)
	}
	if err != nil {
		return unit{}, err
	}
	var account *user.User
	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		account, err = user.Lookup(sudoUser)
	} else {
		account, err = user.Current()
	}
	if err != nil {
		return unit{}, err
	}
	argv := append([]string{exe, "push", "-all"}, extra...)
	return unit{argv: argv, user: account.Username, home: account.HomeDir}, nil
}

func renderSystemd(u unit) string {
	args := make([]string, len(u.argv))
	for i, a := range u.argv {
		if a == "" || strings.IndexFunc(a, unicode.IsSpace) >= 0 {
			a = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(a) + `"`
		}
		args[i] = a
	}
	return `[Unit]
Description=dcs push -all: publish Claude Code sessions as they change
Wants=network-online.target
After=network-online.target

[Service]
User=` + u.user + `
Environment=HOME=` + u.home + `
ExecStart=` + strings.Join(args, " ") + `
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
`
}

func installSystemd(u unit) error {
	if os.Geteuid() != 0 {
		return errors.New("run with sudo")
	}
	if err := os.WriteFile(unitPath, []byte(renderSystemd(u)), 0o644); err != nil {
		return err
	}
	// restart rather than start, so a reinstall picks up a new binary.
	return runAll([]string{"systemctl", "daemon-reload"},
		[]string{"systemctl", "enable", serviceName + ".service"},
		[]string{"systemctl", "restart", serviceName + ".service"})
}

func uninstallSystemd() error {
	if os.Geteuid() != 0 {
		return errors.New("run with sudo")
	}
	exec.Command("systemctl", "disable", "--now", serviceName+".service").Run() // fails when not installed
	if err := os.Remove(unitPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return runAll([]string{"systemctl", "daemon-reload"})
}

// runAll runs each command in turn, stopping at the first failure.
func runAll(cmds ...[]string) error {
	for _, c := range cmds {
		cmd := exec.Command(c[0], c[1:]...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(c, " "), err)
		}
	}
	return nil
}
