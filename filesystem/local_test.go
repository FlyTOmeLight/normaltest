package filesystem

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalGetMeta(t *testing.T) {
	bs := bsSet[BlobStoreLocal]
	basePath := bs.(*localBlobStore).basePath

	testCases := []struct {
		name     string
		path     string
		wantMeta BlobMeta
	}{
		{
			name: "relative path",
			path: "my-bucket/hello",
			wantMeta: BlobMeta{
				Name:        "my-bucket/hello",
				ContentType: "application/octet-stream",
				Size:        11,
				URLPath:     filepath.Join(basePath, "my-bucket/hello"),
			},
		},
	}

	content := "hello world"
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.WriteRaw(tc.path, strings.NewReader(content))
			if err != nil {
				t.Fatalf("write raw error: %v", err)
			}
			defer bs.DeleteRaw(tc.path)

			meta, err := bs.GetMeta(tc.path)
			if err != nil {
				t.Fatalf("get meta error: %v", err)
			}
			if !equal(*meta, tc.wantMeta) {
				t.Fatalf("get meta: %v, but want: %v", *meta, tc.wantMeta)
			}
		})
	}
}

func TestLocalBuildURL(t *testing.T) {
	bs := bsSet[BlobStoreLocal]
	basePath := bs.(*localBlobStore).basePath

	testCases := []struct {
		desc    string
		path    string
		wantURL string
	}{
		{
			desc:    "empty path",
			path:    "",
			wantURL: basePath,
		},
		{
			desc:    "relative path",
			path:    "my-bucket/hello",
			wantURL: filepath.Join(basePath, "my-bucket/hello"),
		},
		{
			desc:    "absolute path",
			path:    filepath.Join(basePath, "hello"),
			wantURL: filepath.Join(basePath, "hello"),
		},
		{
			desc:    "uri format file://basePath",
			path:    "file:/" + basePath,
			wantURL: basePath,
		},
		{
			desc:    "uri format file:///basePath",
			path:    "file://" + basePath,
			wantURL: basePath,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			url, err := bs.BuildURL(tc.path)
			if err != nil {
				t.Fatalf("build url error: %v", err)
			}
			if url != tc.wantURL {
				t.Fatalf("url: %s, wantURL: %s", url, tc.wantURL)
			}
			t.Logf("wantURL: %s", tc.wantURL)
		})
	}
}
