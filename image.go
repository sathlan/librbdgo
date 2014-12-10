// Package rbd is a partial binding of the librbd ceph library.
package rbd

/*
#cgo LDFLAGS: -lrados -lrbd
#include "stdlib.h"
#include <errno.h>
#include <rados/librados.h>
#include <rbd/librbd.h>

extern int goDiffCB(uint64_t, size_t, int, void *);

static int goDiffIter(rbd_image_t imageHandle, const char *snapName, uint64_t offset, uint64_t len, void* userdata) {
   return rbd_diff_iterate(imageHandle, snapName, offset, len, goDiffCB, userdata);
}

*/
import "C"
import "unsafe"
import "fmt"
import "reflect"
import "io"

// Image holds the C structure and information about the block device.
type Image struct {
	closed       bool
	name         string
	readOnly     bool
	snapshot     string
	wantSnapshot bool
	c            uintptr
}

// Locker describes all the locker attached to a block device
type Locker struct {
	tag       string
	exclusive bool
	lockers   [][]string
}

const (
	//LayeringMask is the equivalent of the C data in go.
	LayeringMask uint64 = 1 << iota
	//Stripingv2Mask is the equivalent of the C data in go.
	Stripingv2Mask
)

// NewImage is the entry point for block device manipulation.
func NewImage(rados IoCtxGetter, name string, options ...func(*Image) error) (*Image, error) {
	var imgC C.rbd_image_t
	img := Image{closed: true, name: name, wantSnapshot: false}
	for _, option := range options {
		option(&img)
	}
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	snapNameC := C.CString("")
	if img.wantSnapshot {
		snapNameC = C.CString(img.snapshot)
	}
	defer C.free(unsafe.Pointer(snapNameC))
	ctx, _ := rados.IoCtxGet()
	ctxC := (C.rados_ioctx_t)(ctx)
	var errC C.int
	if img.readOnly == true {
		errC = C.rbd_open_read_only(ctxC, nameC, &imgC, snapNameC)
	} else {
		errC = C.rbd_open(ctxC, nameC, &imgC, snapNameC)
	}
	if (uintptr)(imgC) == 0 {
		return nil, &cError{fmt.Sprintf("Cannot Open image %s invalid pointer %v", name, imgC), 0, errC}
	}
	if errC != 0 {
		return nil, &cError{fmt.Sprintf("Cannot Open image %s", name), 0, errC}
	}
	img.c = reflect.ValueOf(imgC).Pointer()
	return &img, nil
}

func (img *Image) getC() C.rbd_image_t {
	//	if img.c == 0 {
	//	}
	return (C.rbd_image_t)(img.c)
}

// Close the associated image.
func (img *Image) Close() error {
	retC := C.rbd_close(img.getC())

	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot close image %s", img.name), 0, retC}
	}
	return nil
}

// Stat gets information about the image.
func (img *Image) Stat() (map[string]interface{}, error) {
	var infoC C.rbd_image_info_t
	retC := C.rbd_stat(img.getC(), &infoC, C.size_t(unsafe.Sizeof(infoC)))
	if retC != 0 {
		return nil, &cError{fmt.Sprintf("Cannot stat image %s", img.name), 0, retC}
	}
	return map[string]interface{}{
		"size":     uint64(infoC.size),
		"obj_size": uint64(infoC.obj_size),
		"num_objs": uint64(infoC.num_objs),
		"order":    int(infoC.order),
		"block_name_prefix": C.GoStringN((*C.char)(&infoC.block_name_prefix[0]),
			C.RBD_MAX_BLOCK_NAME_SIZE),
		"parent_pool": int64(infoC.parent_pool),
		"parent_name": C.GoStringN((*C.char)(&infoC.parent_name[0]),
			C.RBD_MAX_IMAGE_NAME_SIZE),
	}, nil
}

// Resize changes the size of the image.
func (img *Image) Resize(newSize uint64) error {
	retC := C.rbd_resize(img.getC(), C.uint64_t(newSize))
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot resize image %s to %d", img.name, newSize), 0, retC}
	}
	return nil
}

// ParentInfo gets information about a cloned image's parent.
func (img *Image) ParentInfo() (map[string]string, error) {
	size := 8
	retC := (C.int)(-C.ERANGE)
	var poolC *C.char
	var nameC *C.char
	var snapNameC *C.char
	var poolBuf []byte
	var nameBuf []byte
	var snapNameBuf []byte
	for retC == -C.ERANGE && size <= 4096 {
		sizeC := C.size_t(size)
		poolBuf = make([]byte, size)
		poolC = (*C.char)(unsafe.Pointer(&poolBuf[0]))
		nameBuf = make([]byte, size)
		nameC = (*C.char)(unsafe.Pointer(&nameBuf[0]))
		snapNameBuf = make([]byte, size)
		snapNameC = (*C.char)(unsafe.Pointer(&snapNameBuf[0]))
		retC = C.rbd_get_parent_info(
			img.getC(),
			poolC, sizeC,
			nameC, sizeC,
			snapNameC, sizeC,
		)
		if retC == -C.ERANGE {
			size *= 2
		}
	}
	if retC != 0 {
		return nil, &cError{fmt.Sprintf("Cannot get parent info for images %s", img.name), 0, retC}
	}
	return map[string]string{
		"pool":     string(poolBuf),
		"name":     string(nameBuf),
		"snapname": string(snapNameBuf),
	}, nil
}

// OldFormat determines whether the image uses the old RBD format.
func (img *Image) OldFormat() (bool, error) {
	var old C.uint8_t

	retC := C.rbd_get_old_format(img.getC(), &old)
	if retC != 0 {
		return false, &cError{fmt.Sprintf("Cannot get old format for image %s", img.name), 0, retC}
	}
	return old != 0, nil
}

// Size gets the size of the image.
func (img *Image) Size() (size uint64, err error) {
	var image_size C.uint64_t

	retC := C.rbd_get_size(img.getC(), &image_size)
	if retC != 0 {
		return 0, &cError{fmt.Sprintf("Error getting size for images %s", img.name), 0, retC}
	}
	return uint64(image_size), nil
}

// Features gets the features bitmask of the image.
func (img *Image) Features() (mask uint64, err error) {
	var featuresC C.uint64_t

	retC := C.rbd_get_features(img.getC(), &featuresC)
	if retC != 0 {
		return 0, &cError{fmt.Sprintf("Error getting size for images %s", img.name), 0, retC}
	}
	mask = uint64(featuresC)
	return
}

// CreateSnap creates a snapshot of the image.
func (img *Image) CreateSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_create(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot create snapshot of %s", img.name), 0, retC}
	}
	return nil
}

// RemoveSnap deletes a snapshot of the image.
func (img *Image) RemoveSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_remove(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot remove snapshot %s from image %s", snapName, img.name), 0, retC}
	}
	return nil
}

// RollbackToSnap reverts the image to its contents at a snapshot.
func (img *Image) RollbackToSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_rollback(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot rollback image %s to snapshot %s", img.name, snapName), 0, retC}
	}
	return nil
}

// ProtectSnap marks a snapshot as protected.
func (img *Image) ProtectSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_protect(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot protect snapshot of %s", img.name), 0, retC}
	}
	return nil
}

// UnProtectSnap marks a snapshot as unprotected.
func (img *Image) UnProtectSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_unprotect(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot unprotect snapshot of %s", img.name), 0, retC}
	}
	return nil
}

// IsProtectedSnap finds out if a snapshot is protected.
func (img *Image) IsProtectedSnap(snapName string) (bool, error) {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))
	var isProtectedC C.int
	retC := C.rbd_snap_is_protected(img.getC(), snapNameC, &isProtectedC)
	if retC != 0 {
		return false, &cError{fmt.Sprintf("Cannot unprotect snapshot of %s", img.name), 0, retC}
	}
	return isProtectedC == 1, nil
}

// SetSnap sets the snapshot to read from.
func (img *Image) SetSnap(snapName string) error {
	snapNameC := C.CString(snapName)
	defer C.free(unsafe.Pointer(snapNameC))

	retC := C.rbd_snap_set(img.getC(), snapNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot set image %s to snapshot %s", img.name, snapName), 0, retC}
	}
	return nil
}

// Overlap gets the number of overlapping bytes between the image and its parent.
func (img *Image) Overlap() (uint64, error) {
	var overlapC C.uint64_t
	retC := C.rbd_get_overlap(img.getC(), &overlapC)
	if retC != 0 {
		return 0, &cError{fmt.Sprintf("Cannot get overlap for image %s", img.name), 0, retC}
	}
	return uint64(overlapC), nil
}

// Copy the image to another location.
func (img *Image) Copy(r *Rbd, dstName string) error {
	dstNameC := C.CString(dstName)
	defer C.free(unsafe.Pointer(dstNameC))
	retC := C.rbd_copy(img.getC(), r.GetHandle(), dstNameC)
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot get overlap for image %s", img.name), 0, retC}
	}
	return nil
}

// StripeUnit returns the stripe unit used for the image.
func (img *Image) StripeUnit() (uint64, error) {
	var stripeUnitC C.uint64_t
	retC := C.rbd_get_overlap(img.getC(), &stripeUnitC)
	if retC != 0 {
		return 0, &cError{fmt.Sprintf("Cannot get stripe unit for image %s", img.name), 0, retC}
	}
	return uint64(stripeUnitC), nil
}

// StripeCount returns the stripe count used for the image.
func (img *Image) StripeCount() (uint64, error) {
	var stripeCountC C.uint64_t
	retC := C.rbd_get_overlap(img.getC(), &stripeCountC)
	if retC != 0 {
		return 0, &cError{fmt.Sprintf("Cannot get stripe count for image %s", img.name), 0, retC}
	}
	return uint64(stripeCountC), nil
}

// Flatten copies all blocks from the parent to the child.
func (img *Image) Flatten() error {
	retC := C.rbd_flatten(img.getC())
	if retC != 0 {
		return &cError{fmt.Sprintf("Cannot flatten image %s", img.name), 0, retC}
	}
	return nil
}

// Read implements the Reader interface.
func (img *Image) Read(p []byte) (n int, err error) {
	size := len(p)
	lenC := C.size_t(size)
	bufC := (*C.char)(unsafe.Pointer(&p[0]))
	offsetC := C.uint64_t(0)
	for size > 0 {
		retC := C.rbd_read(img.getC(), offsetC, lenC, bufC)
		if retC == -C.EINVAL {
			return n, io.EOF
		}
		if retC < 0 {
			return 0, &cError{fmt.Sprintf("Cannot read from image %s (%v)", img.name, retC), 0, (C.int)(retC)}
		}
		n += int(retC)
		bufC = (*C.char)(unsafe.Pointer(&p[int(retC)-1]))
		size -= int(retC)
		lenC -= (C.size_t)(retC)
		offsetC += C.uint64_t(n)
	}
	if size == 0 {
		return n, nil
	}

	return n, fmt.Errorf("Too many bytes red, expected %d, got %d", len(p), len(p)-size)
}

// ReadRaw reads data from the image.
func (img *Image) ReadRaw(offset, length uint) (data string, err error) {
	lenC := C.size_t(length)
	buf := make([]byte, length)
	bufC := (*C.char)(unsafe.Pointer(&buf[0]))
	offsetC := C.uint64_t(offset)
	retC := C.rbd_read(img.getC(), offsetC, lenC, bufC)
	if retC == -C.EINVAL {
		return string(buf), io.EOF
	}
	if retC < 0 {
		return "", &cError{fmt.Sprintf("Cannot read from %d+%d in image %s (%v)", offset, length, img.name, retC), 0, (C.int)(retC)}
	}
	return string(buf), nil
}

// WriteRaw reads data from the image.
func (img *Image) WriteRaw(data string, offset uint) (n int, err error) {
	length := len(data)
	lenC := C.size_t(length)
	offsetC := C.uint64_t(offset)
	bufC := C.CString(data)
	defer C.free(unsafe.Pointer(bufC))

	retC := C.rbd_write(img.getC(), offsetC, lenC, bufC)
	if int(retC) == length {
		return length, nil
	} else if retC < 0 {
		return -1, &cError{fmt.Sprintf("Cannot write from %d in image %s (%v)", offset, img.name, retC), 0, (C.int)(retC)}
	} else if int(retC) < length {
		return int(retC), io.EOF
	}
	return int(retC), fmt.Errorf("Wrote more than expected!")
}

// Write implements the writer interface.
func (img *Image) Write(p []byte) (n int, err error) {
	size := len(p)
	lenC := C.size_t(size)
	bufC := (*C.char)(unsafe.Pointer(&p[0]))
	offsetC := C.uint64_t(0)
	for size > 0 {
		retC := C.rbd_write(img.getC(), offsetC, lenC, bufC)
		if retC < 0 {
			if retC == -C.EINVAL {
				return n, io.EOF
			}
			return n, &cError{fmt.Sprintf("Cannot write to image %s", img.name), 0, (C.int)(retC)}
		}
		n += int(retC)
		if n == size {
			return len(p), nil
		}

		bufC = (*C.char)(unsafe.Pointer(&p[int(retC)-1]))
		size -= int(retC)
		lenC -= (C.size_t)(retC)
		offsetC += C.uint64_t(n)
	}
	if n < size {
		return n, fmt.Errorf("Only %d out of %d bytes were written", n, size)
	}
	// n > size
	return n, fmt.Errorf("More bytes written %d than expected %d", n, size)
}

// Discard the range from the image.
func (img *Image) Discard(offset int, length int) error {
	retC := C.rbd_discard(img.getC(), C.uint64_t(offset), C.uint64_t(length))
	if retC < 0 {
		return fmt.Errorf("Cannot discard region %d~%d from image %s", offset, length, img.name)
	}
	return nil
}

// Flush blocks until all writes are fully flushed if caching is enabled.
func (img *Image) Flush() error {
	retC := C.rbd_flush(img.getC())
	if retC < 0 {
		return fmt.Errorf("Cannot flush image %s", img.name)
	}
	return nil
}

// InvalidateCache drop any cached data.
func (img *Image) InvalidateCache() error {
	retC := C.rbd_invalidate_cache(img.getC())
	if retC < 0 {
		return fmt.Errorf("Cannot invalidate cache from image %s", img.name)
	}
	return nil
}

// ListChildren lists children of the currently set snapshot.
func (img *Image) ListChildren() ([]map[string]string, error) {

	poolsSize := C.size_t(512)
	imagesSize := C.size_t(512)
	var retC C.ssize_t
	var pools []byte
	var images []byte
	for {
		pools = make([]byte, int(poolsSize))
		images = make([]byte, int(imagesSize))
		retC = C.rbd_list_children(img.getC(),
			(*C.char)(unsafe.Pointer(&pools[0])),
			&poolsSize,
			(*C.char)(unsafe.Pointer(&images[0])),
			&imagesSize,
		)
		if retC >= 0 {
			break
		} else if retC != -C.ERANGE {
			return nil, &cError{fmt.Sprintf("Cannot list children of %s", img.name), 0, (C.int)(retC)}
		}
	}
	if retC == 0 {
		return make([]map[string]string, 0), nil
	}
	pool, _ := splitDataN(pools[:int(poolsSize)-1], int(retC))
	image, _ := splitDataN(images[:int(imagesSize)-1], int(retC))
	res := make([]map[string]string, len(pool))

	for i, v := range pool {
		res[i] = map[string]string{v: image[i]}
	}
	return res, nil
}

// ListLockers list clients that have locked the image.
func (img *Image) ListLockers() (Locker, error) {
	clientsSize := C.size_t(512)
	cookiesSize := C.size_t(512)
	addrsSize := C.size_t(512)
	tagSize := C.size_t(512)
	exclusive := C.int(0)

	var retC C.ssize_t
	var clients []byte
	var cookies []byte
	var addrs []byte
	var tag []byte
	for {
		clients = make([]byte, int(clientsSize))
		cookies = make([]byte, int(cookiesSize))
		addrs = make([]byte, int(addrsSize))
		tag = make([]byte, int(tagSize))
		retC = C.rbd_list_lockers(
			img.getC(),
			&exclusive,
			(*C.char)(unsafe.Pointer(&tag[0])),
			&tagSize,
			(*C.char)(unsafe.Pointer(&clients[0])),
			&clientsSize,
			(*C.char)(unsafe.Pointer(&cookies[0])),
			&cookiesSize,
			(*C.char)(unsafe.Pointer(&addrs[0])),
			&addrsSize,
		)
		if retC >= 0 {
			break
		} else if retC != -C.ERANGE {
			return Locker{}, &cError{fmt.Sprintf("Cannot list lockers of %s", img.name), 0, (C.int)(retC)}
		}
	}

	if retC == 0 {
		return Locker{}, nil
	}

	client, _ := splitDataN(clients[:int(clientsSize)-1], int(retC))
	cookie, _ := splitDataN(cookies[:int(cookiesSize)-1], int(retC))
	addr, _ := splitDataN(addrs[:int(addrsSize)-1], int(retC))
	tags := tag[:int(tagSize)-1]
	l := Locker{exclusive: false}
	for i := range client {
		l.lockers = append(l.lockers, []string{client[i], cookie[i], addr[i]})
	}
	if exclusive == 1 {
		l.exclusive = true
	}
	l.tag = string(tags)
	return l, nil
}

// LockExclusive takes an exclusive lock on the image.
func (img *Image) LockExclusive(cookie string) error {
	cookieC := C.CString(cookie)
	defer C.free(unsafe.Pointer(cookieC))
	retC := C.rbd_lock_exclusive(img.getC(), cookieC)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot acquire exclusive lock on image %s", img.name), 0, retC}
	}
	return nil
}

// LockShared takes a shared lock on the image.
func (img *Image) LockShared(cookie string, tag string) error {
	cookieC := C.CString(cookie)
	defer C.free(unsafe.Pointer(cookieC))
	tagC := C.CString(tag)
	defer C.free(unsafe.Pointer(tagC))
	retC := C.rbd_lock_shared(img.getC(), cookieC, tagC)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot acquire shared lock on image %s", img.name), 0, retC}
	}
	return nil
}

// Unlock releases a lock on the image that was locked by this rados client.
func (img *Image) Unlock(cookie string) error {
	cookieC := C.CString(cookie)
	defer C.free(unsafe.Pointer(cookieC))
	retC := C.rbd_unlock(img.getC(), cookieC)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot unlock image %s", img.name), 0, retC}
	}
	return nil
}

// BreakLock releases a lock held by another rados client.
func (img *Image) BreakLock(client string, cookie string) error {
	cookieC := C.CString(cookie)
	defer C.free(unsafe.Pointer(cookieC))
	clientC := C.CString(client)
	defer C.free(unsafe.Pointer(clientC))
	retC := C.rbd_break_lock(img.getC(), clientC, cookieC)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot unlock image %s", img.name), 0, retC}
	}
	return nil
}

// String implements the stringer interface for Locker.
func (l Locker) String() (str string) {
	str = fmt.Sprintf("Locker{tag: %s, exclusive: %v, lockers: %v",
		l.tag,
		l.exclusive,
		l.lockers)
	return
}

// DiffHandler is the signature of the callback passed to DiffIterate.
type DiffHandler func(offset, length, exists int, d interface{}) int

type diffHandlerPasser struct {
	f DiffHandler
	d interface{}
}

//export goDiffCB
func goDiffCB(offset C.uint64_t, length C.size_t, exists C.int, userdata unsafe.Pointer) C.int {
	req := (*diffHandlerPasser)(userdata)
	return C.int(req.f(int(offset), int(length), int(exists), req.d))
}

// DiffIterate iterates over the changed extents of an image.
//
// See https://stackoverflow.com/questions/6125683/call-go-functions-from-c for
// more information on the c plumbing necessary to make this works.
func (img *Image) DiffIterate(offset int, length int, fromSnapshot string, f DiffHandler, d interface{}) error {
	fromSnapshotC := C.CString(fromSnapshot)
	defer C.free(unsafe.Pointer(fromSnapshotC))
	req := unsafe.Pointer(&diffHandlerPasser{f, d})
	retC := C.goDiffIter(
		img.getC(),
		fromSnapshotC,
		C.uint64_t(offset),
		C.uint64_t(length),
		req,
	)
	if retC < 0 {
		return &cError{fmt.Sprintf("Cannot generate diff from snapshot %s", fromSnapshot), 0, retC}
	}
	return nil
}
