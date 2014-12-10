package rbd

/*
#include <errno.h>
*/
import "C"
import "fmt"

// https://code.google.com/p/go/issues/detail?id=435

type cError struct {
	msg  string
	want int
	got  C.int
}

func (e *cError) Error() (errorMsg string) {
	errorC := map[C.int]string{
		C.EPERM:     "Permission Error",
		C.ENOENT:    "Image Not Found",
		C.EIO:       "IO Error",
		C.ENOSPC:    "No Space",
		C.EEXIST:    "Image Exists",
		C.EINVAL:    "Invalid Argument",
		C.EROFS:     "Read OnlyImage",
		C.EBUSY:     "Image Busy",
		C.ENOTEMPTY: "Image Has Snapshots",
		C.ENOSYS:    "Function Not Supported",
		C.EDOM:      "Argument Out Of Range",
		C.ESHUTDOWN: "Connection Shutdown",
		C.ETIMEDOUT: "Timeout",
		-C.EINVAL:   "Snap should be protected",
		-C.EBUSY:    "Snap is not protected",
	}

	if msg, ok := errorC[e.got]; !ok {
		errorMsg = fmt.Sprintf("%s: unknown error %d (expected %d)", e.msg, int(e.got), e.want)
	} else {
		errorMsg = fmt.Sprintf("%s: expected %d, got %s (%d)", e.msg, e.want, msg, int(e.got))
	}
	return
}
