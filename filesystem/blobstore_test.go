package filesystem

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var bsSet = make(map[string]BlobStore)

const (
	BlobStoreMinio = "minio"
	BlobStoreLocal = "local"
)

func TestMain(m *testing.M) {
	var err error
	bsSet[BlobStoreMinio], err = NewBlobStore(KindS3, "/my-bucket",
		map[string]string{
			ConfigHost:       "play.min.io",
			ConfigAk:         "Q3AM3UQ867SPQQA43P2F",
			ConfigSk:         "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
			ConfigRegion:     "us-east-1",
			ConfigDisableSSL: "false",
		})
	if err != nil {
		panic("init minio failed " + err.Error())
	}
	bsSet[BlobStoreLocal], err = NewBlobStore(KindLocal, "./", nil)
	if err != nil {
		panic("init local fs failed " + err.Error())
	}
	os.Exit(m.Run())
}

func TestWriteRaw(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		expectsErr bool
	}{
		{
			name: "path without / for prefix",
			path: "my-bucket/hello",
		},
		{
			name: "regular path",
			path: "hello",
		},
		{
			name:       "empty key",
			path:       "",
			expectsErr: true,
		},
	}

	content := "Hello, world!"
	for _, tc := range testCases {
		for backend, bs := range bsSet {
			t.Run(fmt.Sprintf("backend: %s, test: %s", backend, tc.name), func(t *testing.T) {
				err := bs.WriteRaw(tc.path, strings.NewReader(content))
				if tc.expectsErr {
					t.Logf("write raw failed: %s", err.Error())
					assert.NotNil(t, err)
				} else {
					if err != nil {
						t.Fatalf("write raw error: %v", err)
						return
					}

					out, err := bs.ReadRaw(tc.path)
					if err != nil {
						t.Fatalf("read raw error: %v", err)
					}
					defer bs.DeleteRaw(tc.path)
					defer out.Close()
					allBytes, err := ioutil.ReadAll(out)
					if err != nil {
						t.Fatal(err)
					}
					if string(allBytes) != content {
						t.Fatalf("content: %s mismatch: %s", string(allBytes), content)
					}
				}
			})
		}
	}
}
