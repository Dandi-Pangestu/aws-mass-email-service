package main

import (
	"context"
	"encoding/csv"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
)

func handler(ctx context.Context, s3Event events.S3Event) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	s3Client := s3.NewFromConfig(sdkConfig)

	for _, record := range s3Event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.URLDecodedKey
		log.Printf("success receive s3 event trigger for bucket: %s and key: %s\n", bucket, key)

		r, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			log.Printf("failed to get object: %s\n", err)
			return err
		}

		isHeaderFetched := false
		reader := csv.NewReader(r.Body)
		for {
			data, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Printf("failed when reading data from csv: %s\n", err)
				break
			}
			if !isHeaderFetched {
				isHeaderFetched = true
				continue
			}
			log.Printf("===== email: %s, name: %s =====\n", data[0], data[1])
		}
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
