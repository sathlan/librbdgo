package rbd

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	rad "github.com/sathlan/libradosgo"
)

type rbdTest struct {
	t        *testing.T
	c        IoCtxCreateDestroyer
	r        *Rbd
	poolName string
	rados    RadosPoolDestroyer
}

func checkError(t *testing.T, e error, message string, args ...interface{}) {
	if e != nil {
		t.Errorf("%v : %v", e, fmt.Sprintf(message, args))
	}
}
func checkFatal(t *testing.T, e error, message string, args ...interface{}) {
	if e != nil {
		t.Fatalf("%v : %v", e, fmt.Sprintf(message, args))
	}
}

func setupContext(t *testing.T, name string, count uint) *rbdTest {
	c, _ := rad.NewRados("/tmp/micro-ceph/ceph.conf")
	c.Connect()
	poolName := "rbd_test"
	c.CreatePool(poolName)
	r, _ := NewRbd(c, poolName)
	return &rbdTest{t, c, r, poolName, c}
}

func rbdCmd(t *testing.T, command ...string) ([]byte, error) {
	out, err := exec.Command("/usr/bin/rbd", command...).Output()
	if err != nil {
		return nil, fmt.Errorf("Problem running rbd %v (%v)", strings.Join(command, " "), err)
	}
	return out, nil
}

func uniqName(prefix string, counter uint) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().Unix(), counter)
}

// func deleteDevice(name string) error {
//
// }

func Test_CrashRecover(t *testing.T) {
	t.Skip("When the rados pointer C.rados_ioctx_t is useless we should recover the panic")
}

func Test_CreateRemove(t *testing.T) {
	rbdTest := setupContext(t, "rbd_test", 0)
	defer rbdTest.rados.DeletePool(rbdTest.poolName)
	device := uniqName("test_createdelete", 0)

	rbdTest.r.Create(device, 10)
	out, err := rbdCmd(t, "list", "-p", rbdTest.poolName)
	checkError(t, err, "Problem findind the created device %s (%v)", device, err)
	if !strings.Contains(string(out[:]), device) {
		t.Errorf("Cannot find the device %s", device)
	}

	rbdTest.r.Remove(device)

	out, err = rbdCmd(t, "list")
	checkError(t, err, "")
	if strings.Contains(string(out[:]), device) {
		t.Errorf("Can still find the device %s after delete", device)
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func testListNameN(t *testing.T, count uint, name string) {
	rbdTest := setupContext(t, "rbd_test", 1)
	defer rbdTest.rados.DeletePool(rbdTest.poolName)

	devices := make(map[string]uint, count)
	if count == 1 {
		// when count is one nothing is added to the name
		devices[name] = count
		rbdTest.r.Create(name, 1)
		defer rbdTest.r.Remove(name)
	} else {
		for i := uint(0); i < count; i++ {
			name := uniqName(name, i)
			devices[name] = count
			rbdTest.r.Create(name, 1)
			defer rbdTest.r.Remove(name)
		}
	}
	listedNames, err := rbdTest.r.List()
	if err != nil {
		t.Errorf("Cannot list devices: %s", err)
	}
	if len(listedNames) != int(count) {
		t.Errorf("Wrong number of existing devices %d", len(listedNames))
	}

	for _, name := range listedNames {
		if _, ok := devices[name]; !ok {
			t.Errorf("Unexpected existing device %s (%v)", name, listedNames)
		}
	}

	for name := range devices {
		if !contains(listedNames, name) {
			t.Errorf("Cannot find created devices named %s in %v", name, devices)
		}
	}
}

func testListN(t *testing.T, count uint) {
	testListNameN(t, count, "test_list")
}

func Test_list_EmptyName(t *testing.T) {
	t.Skip("TODO: cannot make the difference between empty name and empty list")
	testListNameN(t, 1, "")
}

func Test_list_0(t *testing.T) {
	testListN(t, 0)
}
func Test_list_1(t *testing.T) {
	testListN(t, 1)
}
func Test_list_2(t *testing.T) {
	testListN(t, 2)
}
func Test_list_3(t *testing.T) {
	testListN(t, 3)
}
func Test_list_42(t *testing.T) {
	testListN(t, 42)
}

func Test_Clone(t *testing.T) {
	imgSource, rbdTest := getImage(t, "clone_image", Layering())
	if err := imgSource.CreateSnap("snap_source"); err != nil {
		t.Fatalf("Cannot snap %s", imgSource.name)
	}
	defer endImage(rbdTest, imgSource)
	if err := imgSource.ProtectSnap("snap_source"); err != nil {
		t.Fatalf("Cannot protect image")
	}
	if err := rbdTest.r.Clone(imgSource.name, "snap_source", rbdTest.r, "cloned_source", Layering()); err != nil {
		t.Errorf("Cannot clone image %s: %v", imgSource.name, err)
	}
	if err := rbdTest.r.Remove("cloned_source"); err != nil {
		t.Fatalf("Cannot remove cloned resource %s", "cloned_source")
	}
	if err := imgSource.UnProtectSnap("snap_source"); err != nil {
		t.Fatalf("Cannot unprotect snap %s", "snap_source")
	}
	if err := imgSource.RemoveSnap("snap_source"); err != nil {
		t.Fatalf("Cannot remove snap %s", "snap_source")
	}
}

func Test_Overlap(t *testing.T) {
	t.Skip("TODO")
}
