package filesystem

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestS3GetMeta(t *testing.T) {
	testCases := []struct {
		desc     string
		path     string
		wantMeta BlobMeta
	}{
		{
			desc: "absolute path",
			path: "s3://my-bucket/hello",
			wantMeta: BlobMeta{
				Name:        "hello",
				ContentType: "binary/octet-stream",
				Size:        11,
				URLPath:     "s3://my-bucket/hello",
			},
		},
		{
			desc: "relative path",
			path: "hello",
			wantMeta: BlobMeta{
				Name:        "hello",
				ContentType: "binary/octet-stream",
				Size:        11,
				URLPath:     "s3://my-bucket/hello",
			},
		},
	}

	bs := bsSet[BlobStoreMinio]
	content := "hello world"
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
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

func equal(meta, wantMeta BlobMeta) bool {
	return meta.Name == wantMeta.Name &&
		meta.Size == wantMeta.Size &&
		meta.URLPath == wantMeta.URLPath &&
		meta.ContentType == wantMeta.ContentType
}

func TestS3BuildURL(t *testing.T) {
	bs := bsSet[BlobStoreMinio]
	bucket := bs.(*s3BlobStore).bucket
	subPath := bs.(*s3BlobStore).subPath

	testCases := []struct {
		desc    string
		path    string
		wantURL string
	}{
		{
			desc:    "empty path",
			path:    "",
			wantURL: "s3://" + filepath.Join(bucket, subPath, ""),
		},
		{
			desc:    "relative path",
			path:    "hello",
			wantURL: "s3://" + filepath.Join(bucket, subPath, "hello"),
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
		})
	}
}
