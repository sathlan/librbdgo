package rbd

// https://code.google.com/p/go-wiki/wiki/cgo

/*
#cgo LDFLAGS: -lrados -lrbd
#include "stdlib.h"
#include <errno.h>
#include <rados/librados.h>
#include <rbd/librbd.h>
*/
import "C"
import "fmt"
import "unsafe"
import "bytes"

type Config struct {
	oldFormat   bool
	features    uint64
	orderC      C.int
	stripeUnit  uint64
	stripeCount uint64
}

type IoCtxCreateDestroyer interface {
	IoCtxCreate(string) (uintptr, error)
	IoCtxDestroy(uintptr)
}

type RadosPoolDestroyer interface {
	DeletePool(string) error
}

type IoCtxGetter interface {
	IoCtxGet() (uintptr, error)
}

func (r *Rbd) IoCtxGet() (uintptr, error) {
	return r.ctx, nil
}

type Rbd struct {
	ctx      uintptr // holds a C.rados_ioctx_t
	PoolName string
}

func NewRbd(rados IoCtxCreateDestroyer, poolName string) (*Rbd, error) {
	ctx, _ := rados.IoCtxCreate(poolName)
	return &Rbd{ctx, poolName}, nil
}

func (c *Config) setOldFormat() error {
	c.oldFormat = true
	return nil
}

func OldFormat() func(*Config) error {
	return func(c *Config) error {
		return c.setOldFormat()
	}
}

func (c *Config) setFeature(mask uint64) error {
	c.features = c.features | mask
	return nil
}

func Layering() func(*Config) error {
	return func(c *Config) error {
		return c.setFeature(uint64(C.RBD_FEATURE_LAYERING))
	}
}

func Stripingv2() func(*Config) error {
	return func(c *Config) error {
		return c.setFeature(uint64(C.RBD_FEATURE_STRIPINGV2))
	}
}

func (r *Rbd) GetHandle() C.rados_ioctx_t {
	return (C.rados_ioctx_t)(r.ctx)
}

func (r *Rbd) Create(name string, size uint64, options ...func(*Config) error) error {
	// , order uint, old_format bool, features byte, stripe_unit int, stripe_count int) error {
	var retC C.int
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	config := &Config{
		oldFormat: false,
		// 4Mb object size by default
		orderC: 22,
		// No RAID0
		stripeCount: 0,
		// same as object size
		stripeUnit: 0,
		// features, none by default
		features: 0,
	}
	for _, option := range options {
		option(config)
	}

	ctxC := (C.rados_ioctx_t)(r.ctx)
	if config.oldFormat {
		if config.features != 0 || config.stripeUnit != 0 || config.stripeCount != 0 {
			return fmt.Errorf("Format image 1 does not support feature, masks or non-default striping")
		}
		retC = C.rbd_create(ctxC, nameC, C.uint64_t(size), &config.orderC)
	} else {
		retC = C.rbd_create3(
			ctxC,
			nameC,
			C.uint64_t(size),
			C.uint64_t(config.features),
			(*C.int)(&config.orderC),
			C.uint64_t(config.stripeUnit),
			C.uint64_t(config.stripeCount),
		)
	}
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot create device %s in pool %s", name, r.PoolName), 0, retC}
	}
	return nil
}

func (r *Rbd) Clone(pName string, pSnapName string, rbdChild *Rbd, cName string, options ...func(*Config) error) error {
	pNameC := C.CString(pName)
	defer C.free(unsafe.Pointer(pNameC))
	pSnapNameC := C.CString(pSnapName)
	defer C.free(unsafe.Pointer(pSnapNameC))
	cNameC := C.CString(cName)
	defer C.free(unsafe.Pointer(cNameC))
	config := &Config{
		orderC:   0,
		features: 0,
	}
	for _, option := range options {
		option(config)
	}

	ctxC := (C.rados_ioctx_t)(r.ctx)
	cCtxC := (C.rados_ioctx_t)(rbdChild.ctx)
	retC := C.rbd_clone(ctxC, pNameC, pSnapNameC, cCtxC, cNameC, C.uint64_t(config.features), &config.orderC)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot clone image %s", pName), 0, retC}
	}
	return nil
}

// int rbd_remove(rados_ioctx_t io, const char *name);
func (r *Rbd) Remove(name string) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	if errC := C.rbd_remove((C.rados_ioctx_t)(r.ctx), nameC); errC < 0 {
		return &cError{fmt.Sprintf("Cannot remove device %s from pool %s", name, r.PoolName), 0, errC}
	}
	return nil
}

// int rbd_list(rados_ioctx_t io, char *names, size_t *size);
// TODO: check http://commandcenter.blogspot.com.au/2014/01/self-referential-functions-and-design.html
//
//
func splitDataN(data []byte, count int) (res []string, err error) {
	// func (*Buffer) ReadString
	//e	buf := bytes.NewBuffer(data)
	for _, v := range bytes.SplitN(data, []byte{0}, count) {
		res = append(res, string(v))
	}
	return res, nil
}

func splitData(data []byte) (res []string, err error) {
	// data\0data\0data\0
	// TODO: cannot make the difference between empty name and empty list
	// if it has a empty name the value returned is the same as the one with no element : \0
	if len(data) == 1 && data[0] == []byte{0}[0] {
		return []string{}, err
	}
	if len(data) == 2 && data[0] == []byte{0}[0] && data[1] == []byte{0}[0] {
		return []string{}, err
	}
	cleanData := data[:len(data)-1]
	for _, v := range bytes.Split(cleanData, []byte{0}) {
		res = append(res, string(v))
	}
	return res, err
}

func (r *Rbd) List() ([]string, error) {
	sizeC := C.size_t(1)
	var data []byte
	data = make([]byte, int(sizeC))
	maxRetry := 10
	retries := 0
	var retC C.int
retry:
	for ; retries < maxRetry; retries++ {
		retC = C.rbd_list((C.rados_ioctx_t)(r.ctx), (*C.char)(unsafe.Pointer(&data[0])), &sizeC)
		switch {
		case retC >= 0:
			break retry
		case retC == -C.ERANGE:
			// TODO(chem): check func (*Buffer) Grow from http://golang.org/pkg/bytes
			data = make([]byte, int(sizeC))
		case retC < 0:
			return nil, &cError{"Cannot list devices from pool", 0, retC}
		}
	}
	if retries == maxRetry {
		return nil, fmt.Errorf("Cannot list devices from pool, max retries (%d) reached", retries)
	}
	listDevices, err := splitData(data)
	if err != nil {
		return nil, fmt.Errorf("Cannot split data list: %v", err)
	}
	return listDevices, nil

}

func (r *Rbd) Rename(src string, dest string) error {
	errC := C.rbd_rename((C.rados_ioctx_t)(r.ctx), C.CString(src), C.CString(dest))
	if errC != 0 {
		return &cError{fmt.Sprintf("Cannot rename %s to %s", src, dest), 0, errC}
	}
	return nil
}
