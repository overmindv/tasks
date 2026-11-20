// Package suitestore публикует приватные входные манифесты через S3 conditional PUT.
package suitestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type Config struct{ Endpoint, Region, Bucket, AccessKey, SecretKey, Prefix string }
type Store struct {
	client         *s3.Client
	bucket, prefix string
}

func New(c Config) (*Store, error) {
	if c.Endpoint == "" || c.Region == "" || c.Bucket == "" || c.AccessKey == "" || c.SecretKey == "" {
		return nil, errors.New("incomplete suite storage configuration")
	}
	client := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, ""), HTTPClient: &http.Client{Timeout: 15 * time.Second}})

	return &Store{client: client, bucket: c.Bucket, prefix: strings.Trim(c.Prefix, "/")}, nil
}
func (s *Store) PutImmutable(ctx context.Context, key string, payload []byte) error {
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(payload), IfNoneMatch: aws.String("*"), ContentType: aws.String("application/json")})
	if err == nil {
		return nil
	}
	var response *smithyhttp.ResponseError
	if !errors.As(err, &response) || response.HTTPStatusCode() != http.StatusPreconditionFailed {
		return errors.New("suite upload failed")
	}
	existing, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return errors.New("suite verification failed")
	}
	defer func() { _ = existing.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(existing.Body, int64(len(payload)+1)))
	if err != nil || !bytes.Equal(data, payload) {
		return errors.New("immutable suite conflict")
	}

	return nil
}
