package filesystem

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type localBlobStore struct {
	config   map[string]string
	basePath string
}

var _ BlobStore = &localBlobStore{}

func newLocalBlobStore(basePath string, config map[string]string) (*localBlobStore, error) {
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("basePath: %s must be directory", basePath)
	}
	basePath, err = filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("get absolute basePath error: %v", err)
	}
	return &localBlobStore{
		config:   config,
		basePath: basePath,
	}, nil
}

// format1: file://basePath/subPath --> /basePath/subPath
// format2: file:///basePath/subPath --> /basePath/subPath
// format3: /basePath/subPath --> /basePath/subPath
// format4: subPath --> /basePath/subPath
func (f *localBlobStore) getFullPath(path string) (string, error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "":
		if !filepath.IsAbs(path) {
			return filepath.Join(f.basePath, path), nil
		}
	case KindLocal:
		path = filepath.Join(u.Host, u.Path)
		if !filepath.IsAbs(path) {
			path = "/" + path
		}
	default:
		return "", errors.New("scheme should be empty or " + KindLocal)
	}

	if !strings.HasPrefix(path, f.basePath) {
		return "", errors.New("path " + path + " does not begin with basePath " + f.basePath)
	}

	return path, nil
}

func (f *localBlobStore) ListMeta(path string, option ListMetaOption) ([]*BlobMeta, error) {
	fullPath, err := f.getFullPath(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("list meta must operate a dir")
	}
	metas := make([]*BlobMeta, 0)
	if option.DirectoryOnly {
		metas, err = addDirMetas(path, fullPath, metas)
		return metas, err
	}
	metas, err = addFileMetas(path, fullPath, metas)
	return metas, err
}

// addDirMetas 只加入fullPath这一级下的目录
func addDirMetas(path, fullPath string, metas []*BlobMeta) ([]*BlobMeta, error) {
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return metas, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return metas, err
			}
			metas = append(metas, &BlobMeta{
				Name:         filepath.Join(path, info.Name()),
				Size:         info.Size(),
				URLPath:      filepath.Join(fullPath, info.Name()),
				LastModified: info.ModTime(),
			})
		}
	}
	return metas, nil
}

// addFileMetas 深度优先遍历加入所有文件，目录被丢弃
func addFileMetas(path, fullPath string, metas []*BlobMeta) ([]*BlobMeta, error) {
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return metas, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			// recursive add files below directories, but do not add directories
			metas, err = addFileMetas(filepath.Join(path, entry.Name()), filepath.Join(fullPath, entry.Name()), metas)
			if err != nil {
				return metas, err
			}
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return metas, err
		}
		metas = append(metas, &BlobMeta{
			Name:         filepath.Join(path, info.Name()),
			Size:         info.Size(),
			URLPath:      filepath.Join(fullPath, info.Name()),
			LastModified: info.ModTime(),
		})
	}
	return metas, nil
}

func (f *localBlobStore) GetMeta(path string) (*BlobMeta, error) {
	fullPath, err := f.getFullPath(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("cannot get meta from a dir")
	}
	contentType, err := getFileContentType(fullPath)
	if err != nil {
		return nil, err
	}
	return &BlobMeta{
		Name:         path,
		ContentType:  contentType,
		Size:         info.Size(),
		URLPath:      fullPath,
		LastModified: info.ModTime(),
	}, nil
}

func (f *localBlobStore) ReadRaw(path string) (io.ReadCloser, error) {
	fullPath, err := f.getFullPath(path)
	if err != nil {
		return nil, err
	}
	readout, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	return readout, nil
}

func (f *localBlobStore) WriteRaw(path string, in io.Reader) error {
	fullPath, err := f.getFullPath(path)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(fullPath), 0777)
	if err != nil {
		return err
	}
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, in)
	return err
}

func (f *localBlobStore) DeleteRaw(path string) error {
	fullPath, err := f.getFullPath(path)
	if err != nil {
		return err
	}
	return os.Remove(fullPath)
}

func getFileContentType(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	buffer := make([]byte, 512)
	_, err = f.Read(buffer)
	if err != nil {
		return "", err
	}

	return http.DetectContentType(buffer), nil
}

func (f *localBlobStore) GetSignedURL(path string, expire time.Duration) (string, error) {
	return "", errors.New("local blob store do not support GetSignedURL")
}

func (f *localBlobStore) BuildURL(path string) (string, error) {
	return f.getFullPath(path)
}
