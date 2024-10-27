package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"log"
)

type messageBody struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

var fromEmailAddress = "dandipangestu96@gmail.com"
var subject = "Greeting new friend!"
var emailTemplate = "<html><h1>Hallo %s, welcome to the jungle!!</h1></html>"

func handler(ctx context.Context, event events.SQSEvent) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	sesClient := ses.NewFromConfig(sdkConfig)

	for _, record := range event.Records {
		err = processMessage(ctx, sesClient, record)
		if err != nil {
			return err
		}
	}

	return nil
}

func processMessage(ctx context.Context, sesClient *ses.Client, record events.SQSMessage) error {
	var body messageBody

	log.Printf("received message from SQS with messageID: %s, body: %s\n", record.MessageId, record.Body)
	err := json.Unmarshal([]byte(record.Body), &body)
	if err != nil {
		log.Printf("failed to unmarshal this messageID %s: %s\n", record.MessageId, err)
		return err
	}

	return sendMessage(ctx, sesClient, body)
}

func sendMessage(ctx context.Context, sesClient *ses.Client, body messageBody) error {
	emailBody := fmt.Sprintf(emailTemplate, body.Name)
	resp, err := sesClient.SendEmail(ctx, &ses.SendEmailInput{
		Source: &fromEmailAddress,
		Destination: &types.Destination{
			ToAddresses: []string{body.Email},
		},
		Message: &types.Message{
			Subject: &types.Content{
				Data: &subject,
			},
			Body: &types.Body{
				Html: &types.Content{
					Data: &emailBody,
				},
			},
		},
	})
	if err != nil {
		log.Printf("failed to send email for %s: %s\n", body.Email, err)
		return err
	}
	log.Printf("successfully send email for %s with messageID %s\n", body.Email, *resp.MessageId)

	return nil
}

func main() {
	lambda.Start(handler)
}
