package filesystem

import (
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	ConfigHost        = "host"
	ConfigAk          = "ak"
	ConfigSk          = "sk"
	ConfigToken       = "token"
	ConfigRegion      = "region"
	ConfigDisableSSL  = "disableSSL"
	ConfigDisplayHost = "displayHost"
)

const (
	defaultExpire = 12 * time.Hour
)

const (
	MaxKeys   = 1000
	Delimiter = "/"
)

type s3BlobStore struct {
	config       map[string]string
	awsConfig    *aws.Config
	client       *s3.S3
	signedClient *s3.S3
	bucket       string
	subPath      string
}

var _ BlobStore = &s3BlobStore{}

func newS3BlobStore(endpoint string, config map[string]string) (*s3BlobStore, error) {
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(config[ConfigAk], config[ConfigSk], config[ConfigToken]),
		Endpoint:         aws.String(config[ConfigHost]),
		Region:           aws.String(config[ConfigRegion]),
		S3ForcePathStyle: aws.Bool(true),
	}
	awsConfig.DisableSSL = aws.Bool(strings.ToLower(config[ConfigDisableSSL]) == "true")

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}
	client := s3.New(sess)
	// check health
	_, err = client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	bucket, subPath, err := parseS3Endpoint(endpoint)
	if err != nil {
		return nil, err
	}

	signedClient, err := newSignedClient(config[ConfigDisplayHost], *awsConfig)
	if err != nil {
		return nil, err
	}

	return &s3BlobStore{
		config:       config,
		awsConfig:    awsConfig,
		client:       client,
		signedClient: signedClient,
		bucket:       bucket,
		subPath:      subPath,
	}, nil
}

func newSignedClient(displayHost string, awsConfig aws.Config) (*s3.S3, error) {
	if displayHost != "" {
		awsConfig.Endpoint = aws.String(displayHost)
	}
	sess, err := session.NewSession(&awsConfig)
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func parseS3Endpoint(endpoint string) (bucket, subPath string, err error) {
	endpoint = strings.Trim(endpoint, Delimiter)
	if endpoint == "" {
		return "", "", errors.New("bucket cannot be empty")
	}
	idx := strings.Index(endpoint, Delimiter)
	if idx == -1 {
		return endpoint, Delimiter, nil
	}
	return endpoint[:idx], endpoint[idx:], nil
}

func (s *s3BlobStore) getBucketAndKey(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	// 相对路径
	if u.Scheme == "" {
		return s.bucket, filepath.Join(s.subPath, uri), nil
	}
	if u.Scheme != KindS3 {
		return "", "", errors.New("scheme should be " + KindS3)
	}
	// 绝对路径
	return u.Host, u.Path, nil
}

func (s *s3BlobStore) fetchURLPath(path string) string {
	return KindS3 + "://" + strings.Trim(path, Delimiter)
}

func (s *s3BlobStore) ListMeta(path string, option ListMetaOption) ([]*BlobMeta, error) {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return nil, err
	}

	input := newListObjectsV2Input(bucket, key, option)
	output, err := s.client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	if option.DirectoryOnly {
		metas := make([]*BlobMeta, len(output.CommonPrefixes))
		for i, obj := range output.CommonPrefixes {
			metas[i] = &BlobMeta{
				Name:    strings.Trim(*obj.Prefix, Delimiter),
				URLPath: s.fetchURLPath(filepath.Join(bucket, *obj.Prefix)),
			}
		}
		return metas, nil
	}

	metas := make([]*BlobMeta, len(output.Contents))
	for i, obj := range output.Contents {
		metas[i] = &BlobMeta{
			Name:         strings.TrimPrefix(*obj.Key, s.subPath),
			Size:         *obj.Size,
			URLPath:      s.fetchURLPath(filepath.Join(bucket, *obj.Key)),
			LastModified: *obj.LastModified,
		}
	}
	return metas, nil
}

func newListObjectsV2Input(bucket, key string, option ListMetaOption) *s3.ListObjectsV2Input {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int64(MaxKeys),
	}

	if option.DirectoryOnly {
		input.Delimiter = aws.String(Delimiter)
	}
	if option.MaxKeys > 0 && option.MaxKeys < MaxKeys {
		input.MaxKeys = aws.Int64(option.MaxKeys)
	}
	if option.StartAfter != "" {
		input.StartAfter = aws.String(option.StartAfter)
	}

	return input
}

func (s *s3BlobStore) GetMeta(path string) (*BlobMeta, error) {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return nil, err
	}
	output, err := s.client.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return &BlobMeta{
		Name:         strings.TrimPrefix(key, s.subPath),
		ContentType:  *output.ContentType,
		Size:         *output.ContentLength,
		URLPath:      s.fetchURLPath(filepath.Join(bucket, key)),
		LastModified: *output.LastModified,
	}, nil
}

func (s *s3BlobStore) ReadRaw(path string) (io.ReadCloser, error) {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return nil, err
	}
	response, err := s.client.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (s *s3BlobStore) WriteRaw(path string, in io.Reader) error {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return err
	}
	// create bucket if not exist
	_, err = s.client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		aErr, ok := err.(awserr.Error)
		if !ok || !(aErr.Code() == s3.ErrCodeBucketAlreadyExists || aErr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou) {
			return err
		}
	}
	uploader := s3manager.NewUploaderWithClient(s.client)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   in,
	})
	return err
}

func (s *s3BlobStore) DeleteRaw(path string) error {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (s *s3BlobStore) GetSignedURL(path string, expire time.Duration) (string, error) {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return "", err
	}
	// todo: cache

	req, _ := s.signedClient.GetObjectRequest(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", err
	}
	if expire == 0 {
		expire = defaultExpire
	}
	return req.Presign(expire)
}

func (s *s3BlobStore) BuildURL(path string) (string, error) {
	bucket, key, err := s.getBucketAndKey(path)
	if err != nil {
		return "", err
	}
	return s.fetchURLPath(filepath.Join(bucket, key)), nil
}
