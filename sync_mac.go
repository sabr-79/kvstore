//go:build darwin

package main

import (
	"golang.org/x/sys/unix"
)

func (p *Pager) syncFile() error {
	_, err := unix.FcntlInt(p.file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
