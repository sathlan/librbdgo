package rbd

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func createDevice(rbdTest *rbdTest, prefix string, size uint64, count uint, options ...func(*Config) error) string {
	device := uniqName(prefix, count)
	err := rbdTest.r.Create(device, size, options...)
	checkFatal(rbdTest.t, err, "Problem creating the device %s", device)
	return device
}

func Test_OpenClose(t *testing.T) {
	rbdTest := setupContext(t, "image_test", 0)
	device := createDevice(rbdTest, "test_openclose", 10, 0)
	defer rbdTest.r.Remove(device)
	img, err := NewImage(rbdTest.r, device)
	checkFatal(t, err, "Problem opening the image %s", device)
	features, err := img.Features()
	checkError(t, err, "Problem getting the features for %s", device)
	if features != 0 {
		t.Errorf("Features wasn't 0, got %v", features)
	}
	err = img.Close()
	checkError(t, err, "Problem closing the image %s (features failed)", device)
}

func Test_stat(t *testing.T) {
	rbdTest := setupContext(t, "image_test", 1)
	size := uint64(5)
	device := createDevice(rbdTest, "test_stat", size, 0)
	img, _ := NewImage(rbdTest.r, device)

	// order matters, it's a stack, so here we close and remove
	defer rbdTest.r.Remove(device)
	defer img.Close()
	info, _ := img.Stat()
	aSize, ok := info["size"]
	if !ok {
		t.Errorf("Cannot find the size of the device %s", device)
	}
	if aSize != size {
		t.Errorf("Wrong size expected %d, got %d", size, aSize)
	}
}

func checkSize(t *testing.T, img *Image, size uint64) {
	if stat, err := img.Stat(); stat["size"] != size {
		t.Errorf("Resize failed, expected device %s to be %d not %d (%v)", img.name, size, stat["size"], err)
	}
}

func getImage(t *testing.T, name string, options ...func(*Config) error) (img *Image, rbdTest *rbdTest) {
	fMb := 5 * 1024 * 1024
	size := uint64(fMb)
	return getImageSized(t, name, size, options...)

}

func getImageSized(t *testing.T, name string, size uint64, options ...func(*Config) error) (img *Image, rbdTest *rbdTest) {
	rbdTest = setupContext(t, "image_test", 2)
	device := createDevice(rbdTest, name, size, 0, options...)
	img, _ = NewImage(rbdTest.r, device)
	return
}

func endImage(rbdTest *rbdTest, img *Image) {
	defer rbdTest.r.Remove(img.name)
	defer img.Close()
}

func Test_Resize(t *testing.T) {
	rbdTest := setupContext(t, "image_test", 3)
	size := uint64(5)
	device := createDevice(rbdTest, "test_resize", size, 0)
	img, _ := NewImage(rbdTest.r, device)
	defer rbdTest.r.Remove(device)
	defer img.Close()

	newSizeUp := uint64(10)
	newSizeDown := uint64(2)

	err := img.Resize(newSizeUp)
	checkError(t, err, "Cannot resize device %s from %d to %d", img.name, size, newSizeUp)
	checkSize(t, img, newSizeUp)
	err = img.Resize(newSizeDown)
	checkError(t, err, "Cannot resize device %s from %d to %d", img.name, size, newSizeDown)
	checkSize(t, img, newSizeDown)
}

func Test_Parent_Info(t *testing.T) {
	t.Skip("Unimplemented Test img.ParentInfo()")
}

func Test_Old_Format(t *testing.T) {
	img, rbdTest := getImage(t, "old_format", OldFormat())
	defer endImage(rbdTest, img)
	tf, err := img.OldFormat()
	checkError(t, err, "Cannot get old format from  %s", img.name)
	if !tf {
		t.Errorf("Old format should be true, got: %v", tf)
	}
}

func Test_Layering(t *testing.T) {
	img, rbdTest := getImage(t, "layering", Layering())
	defer endImage(rbdTest, img)
	featuresMask, err := img.Features()
	checkError(t, err, "Cannot get feature for %s", img.name)
	if featuresMask^LayeringMask != 0 {
		t.Errorf("Layering should be enabled for %s, got: %v", img.name, featuresMask)
	}
}

func Test_Stripingv2(t *testing.T) {
	img, rbdTest := getImage(t, "stripingv2", Stripingv2())
	defer endImage(rbdTest, img)
	featuresMask, err := img.Features()
	checkError(t, err, "Cannot get feature for %s", img.name)
	if featuresMask^Stripingv2Mask != 0 {
		t.Errorf("Layering should be enabled for %s, got: %v", img.name, featuresMask)
	}
}

func Test_CreateSnap(t *testing.T) {
	t.Skipf("TODO")
}

func Test_Copy(t *testing.T) {
	t.Skipf("TODO")
}

func Test_StipeUnit(t *testing.T) {
	t.Skipf("TODO")
}

func Test_Write(t *testing.T) {
	img, rbdTest := getImage(t, "writing")
	defer endImage(rbdTest, img)
	buf := []byte("test_writing")
	lenB := len(buf)
	n, err := img.Write(buf)

	if n != lenB {
		t.Errorf("Problem writing to %s (%d): %v", img.name, n, err)
	}
	smallSize := uint64(5) // bytes
	imgTooSmall, rbdTest2 := getImageSized(t, "writting_too_small", smallSize)
	defer endImage(rbdTest2, imgTooSmall)
	buf = []byte("test_too_small")
	lenB = len(buf)
	n2, err := imgTooSmall.Write(buf)

	if err != io.EOF || n2 != int(smallSize) {
		t.Errorf("Did not get the end of file error from the write on %s (%d): %v", imgTooSmall.name, n2, err)
	}
}

func Test_Read(t *testing.T) {
	img, rbdTest := getImageSized(t, "reading", 12)
	defer endImage(rbdTest, img)

	buf := []byte("test_reading")
	img.Write(buf)
	smallBufN := 5
	smallBuf := make([]byte, smallBufN)
	n, err := img.Read(smallBuf)
	if err != nil && err != io.EOF {
		t.Errorf("Problem reading small buffer from %s (%d)", img.name, n)
	}
	if n != smallBufN {
		t.Errorf("Problem filling small buffer from %s (%d)", img.name, n)
	}
	if bytes.Equal(buf, smallBuf) || !bytes.Equal(smallBuf, []byte("test_")) {
		t.Errorf("Problem filling small buffer from %s (%d): %s - %d: %v", img.name, n, string(smallBuf), len(smallBuf), err)
	}

	fitBufN := 12
	fitBuf := make([]byte, fitBufN)
	n, err = img.Read(fitBuf)
	if err != nil && err != io.EOF {
		t.Errorf("Problem reading fit buffer from %s (%d)", img.name, n)
	}
	if n != len(buf) {
		t.Errorf("Problem filling fit buffer from %s (%d)", img.name, n)
	}
	if !bytes.Equal(buf, fitBuf) {
		t.Errorf("Problem filling fit buffer from %s (%d): %s - %d: %v", img.name, n, fitBuf, len(fitBuf), err)
	}

	largeBufN := 100
	largeBuf := make([]byte, largeBufN)
	n, err = img.Read(largeBuf)
	if err != nil && err != io.EOF {
		t.Errorf("Problem reading large buffer from %s (%d): %v", img.name, n, err)
	}
	if err != io.EOF {
		//		t.Skip("rbd_read return the buf len given, so it's not possible to detect end of file.  Investigate rbd_aio_read ?")
		t.Errorf("Problem detecting EOF from %s (%d): %s - %d: %v", img.name, n, string(largeBuf), len(largeBuf), err)
	}
	if !bytes.Contains(largeBuf, buf) || bytes.Equal(largeBuf, buf) {
		t.Errorf("Problem filling large buffer from %s (%d): %s - %d: %v", img.name, n, string(largeBuf), len(largeBuf), err)
	}
}

func Test_ListChildren(t *testing.T) {
	img, rbdTest := getImage(t, "list_children", Layering(), Stripingv2())
	defer endImage(rbdTest, img)

	if err := img.CreateSnap("snap_001"); err != nil {
		t.Fatalf("Cannot snap %s", img.name)
	}

	if err := img.CreateSnap("snap_002"); err != nil {
		t.Fatalf("Cannot snap %s", img.name)
	}

	if err := img.ProtectSnap("snap_001"); err != nil {
		t.Fatalf("Cannot protect snap %s", "snap_001")
	}
	defer img.UnProtectSnap("snap_001")

	if err := rbdTest.r.Clone(img.name, "snap_001", rbdTest.r, "cloned_1_snap_001", Layering(), Stripingv2()); err != nil {
		t.Errorf("Cannot clone image %s: %v", img.name, err)
	}
	defer rbdTest.r.Remove("cloned_snap_001")
	if err := rbdTest.r.Clone(img.name, "snap_001", rbdTest.r, "cloned_2_snap_001", Layering(), Stripingv2()); err != nil {
		t.Errorf("Cannot clone image %s: %v", img.name, err)
	}
	defer rbdTest.r.Remove("cloned_snap_002")
	if err := img.SetSnap("snap_001"); err != nil {
		t.Fatalf("Cannot set snap to %s", "snap_001")
	}

	var child []map[string]string
	var err error
	if child, err = img.ListChildren(); err != nil {
		t.Fatalf("Cannot get children list: %v", err)
	}
	if len(child) != 2 {
		t.Errorf("Didn't get the right number of children")
	}
	for _, v := range child {
		if v["rbd_test"] != "cloned_1_snap_001" && v["rbd_test"] != "cloned_2_snap_001" {
			t.Errorf("Invalid value returned for child list: %v", v)
		}
	}

	if err := img.SetSnap("snap_002"); err != nil {
		t.Fatalf("Cannot set snap to %s", "snap_002")
	}
	if child, err = img.ListChildren(); err != nil {
		t.Errorf("Cannot get children list for snap_002: %v", err)
	}
	if len(child) != 0 {
		t.Errorf("Wrong number of children for snap_002, expected 0, got %d", len(child))
	}

}

func Test_Lock(t *testing.T) {
	img, rbdTest := getImage(t, "list_children", Layering(), Stripingv2())
	defer endImage(rbdTest, img)
	if err := img.LockExclusive("test_lock"); err != nil {
		t.Errorf("Cannot get a lock: %v", err)
	}

	if err := img.Unlock("test_lock"); err != nil {
		t.Errorf("Cannot unlock: %v", err)
	}

	if err := img.LockShared("test_lock", "test_tag"); err != nil {
		t.Errorf("Cannot get a shared lock: %v", err)
	}

	if err := img.LockShared("test_lock2", "test_tag"); err != nil {
		t.Errorf("Cannot get a second shared lock: %v", err)
	}

	l, err := img.ListLockers()
	if err != nil {
		t.Errorf("Cannot list the lock: %v", err)
	}

	out, err := rbdCmd(t, "-p", "rbd_test", "lock", "list", img.name)
	if err != nil {
		t.Errorf("Problem running the rdb command: %v", err)
	}
	if len(l.lockers) != 2 {
		t.Errorf("Didn't find the 2 lockers")
	}
	for _, v := range l.lockers {
		if len(v) != 3 {
			t.Errorf("Didn't find all the information about the locker")
		} else {
			if !strings.Contains(string(out), v[2]) {
				t.Errorf("Wrong information about the locker")
			}
		}
	}
	if !strings.Contains(string(out), l.tag) {
		t.Errorf("Cannot find the right tag for lockers: %s(%d), %s", l.tag, len(l.tag), string(out))
	}
	if err := img.Unlock("test_lock"); err != nil {
		t.Errorf("Cannot unlock shared lock: %v", err)
	}
	if err := img.Unlock("test_lock2"); err != nil {
		t.Errorf("Cannot unlock shared lock: %v", err)
	}
	l2, err := img.ListLockers()
	if err != nil {
		t.Errorf("Cannot list again the lock: %v", err)
	}
	if l2.lockers != nil {
		t.Logf("Cannot remove all lockers")
	}
}

func simpleCallback(off, len, exi int, d interface{}) int {
	data, ok := d.(*[3][4]int)
	if !ok {
		return 1
	}
	var i int
	for i = range *data {
		if data[i][0] == 0 {
			break
		}
	}
	data[i] = [4]int{1, off, len, exi}
	return 0
}

func Test_DiffIter(t *testing.T) {
	img, rbdTest := getImageSized(t, "list_children", 13, Layering(), Stripingv2())
	defer endImage(rbdTest, img)
	// write some data
	buf := []byte("test_writing")
	lenB := len(buf)
	n, err := img.Write(buf)

	if n != lenB {
		t.Errorf("Problem writing to %s (%d): %v", img.name, n, err)
	}

	// create a snap
	if err := img.CreateSnap("snap_001"); err != nil {
		t.Fatalf("Cannot snap %s", img.name)
	}
	// rewrite offset 0, length 1
	n, err = img.Write([]byte("T"))
	// off 5, len 2
	n, err = img.WriteRaw("Ab", 5)
	// off 12, len 1
	n, err = img.WriteRaw("A", 12)

	// check the diff with a simple callback
	got := [3][4]int{}
	img.DiffIterate(0, 24, "snap_001", simpleCallback, &got)

	expected := [3][4]int{
		{1, 0, 1, 1},
		{1, 5, 2, 1},
		{1, 12, 1, 1},
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("Wrong offset, length, exists tuple, got %v expected %v", got[i], expected[i])
		}
	}
}
