package filesystem

import (
	"errors"
	"fmt"
	"io"
	"time"
)

type Kind string

const (
	KindLocal     = "file"
	KindNFS       = "nfs"
	KindCFS       = "cfs"
	KindGlusterFS = "glusterfs"
	KindS3        = "s3"
)

func NewBlobStore(kind Kind, endpoint string, config map[string]string) (BlobStore, error) {
	switch kind {
	case KindS3:
		return newS3BlobStore(endpoint, config)
	case KindLocal:
		return newLocalBlobStore(endpoint, config)
	}
	return nil, fmt.Errorf("kind %s unsupported", kind)
}

type BlobStore interface {
	// ListMeta recursively list all object metas
	ListMeta(path string, option ListMetaOption) ([]*BlobMeta, error)

	// GetMeta get meta from path
	GetMeta(path string) (*BlobMeta, error)

	// ReadRaw retrieves a byte stream from the blob store or an error
	ReadRaw(path string) (io.ReadCloser, error)

	// WriteRaw stores a raw byte stream
	WriteRaw(path string, in io.Reader) error

	// DeleteRaw delete the path
	DeleteRaw(path string) error

	GetSignedURL(path string, expire time.Duration) (string, error)

	BuildURL(path string) (string, error)
}

type ListMetaOption struct {
	// support: s3/file
	DirectoryOnly bool
	// support: s3
	// CommonPrefix与Contents均会计入MaxKeys中,如果同时开启DirectoryOnly,与CommonPrefix同级目录下的object也计入MaxKeys中
	MaxKeys int64
	// support: s3
	StartAfter string
}

type BlobMeta struct {
	// Name represents last level path
	Name string `json:"name"`
	// ContentType only provides in GetMeta when using s3, since only s3.HeadObject provides this field
	ContentType string `json:"contentType"`
	// Size use byte as unit
	Size int64 `json:"size"`
	// URLPath an accessible url using Path-style access
	URLPath string `json:"urlPath"`
	// LastModified last modified time the object.
	LastModified time.Time `json:"lastModified"`
}

func CopyRaw(sourceBS, destBS BlobStore, sourcePath, destPath string) error {
	if sourceBS == nil {
		return errors.New("source blobstore is required")
	}
	if destBS == nil {
		destBS = sourceBS
	}
	stream, err := sourceBS.ReadRaw(sourcePath)
	if err != nil {
		return err
	}
	defer stream.Close()
	return destBS.WriteRaw(destPath, stream)
}
