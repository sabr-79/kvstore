//go:build !darwin

package main

func (p *Pager) syncFile() error {
	return p.file.Sync()
}
