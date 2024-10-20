package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"io"
	"log"
)

type messageBody struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

var sendMessageQueueURL = "https://sqs.us-east-1.amazonaws.com/116981782358/send-email"

func handler(ctx context.Context, s3Event events.S3Event) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	sqsClient := sqs.NewFromConfig(sdkConfig)

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
				return err
			}
			if !isHeaderFetched {
				isHeaderFetched = true
				continue
			}
			email, name := data[0], data[1]
			log.Printf("===== email: %s, name: %s =====\n", email, name)

			bodyBytes, err := json.Marshal(&messageBody{
				Email: email,
				Name:  name,
			})
			if err != nil {
				log.Printf("failed to encode the object with email %s: %s\n", email, err)
				continue
			}
			bodyStr := string(bodyBytes)

			resp, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    &sendMessageQueueURL,
				MessageBody: &bodyStr,
			})
			if err != nil {
				log.Printf("failed to publish queue with email %s: %s\n", email, err)
				continue
			}
			log.Printf("message published for email %s with message id %s\n", email, *resp.MessageId)
		}
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
