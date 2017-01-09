package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	svc           *sqs.SQS
	queueUrlCache = make(map[string]*string)
	from          = "fromQueueName"
	to            = "toQueueName"
	region        = "us-east-1"
)

func main() {
	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	svc = sqs.New(sess, &aws.Config{Region: aws.String(region)})
	for {
		msg, err := ReceiveMessage(from)
		if err != nil {
			log.Fatal(err)
		}
		err = SendMessageBatch(to, msg)
		if err != nil {
			log.Fatal(err)
		}
		err = DeleteMessageBatch(from, msg)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Done...")
	}
}

func ReceiveMessage(queueName string) ([]*sqs.Message, error) {
	url, err := GetQueueUrl(queueName)
	if err != nil {
		return nil, err
	}
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            url,
		MaxNumberOfMessages: aws.Int64(10),
	}
	resp, err := svc.ReceiveMessage(params)
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func GetQueueUrl(name string) (*string, error) {
	url, ok := queueUrlCache[name]
	if ok {
		return url, nil
	}
	params := &sqs.GetQueueUrlInput{QueueName: aws.String(name)}
	resp, err := svc.GetQueueUrl(params)
	if err != nil {
		return nil, err
	}
	queueUrlCache[name] = resp.QueueUrl
	return queueUrlCache[name], nil
}

func SendMessageBatch(queueName string, messages []*sqs.Message) error {
	input := []*sqs.SendMessageBatchRequestEntry{}
	for _, v := range messages {
		input = append(input, &sqs.SendMessageBatchRequestEntry{
			Id: v.MessageId, MessageBody: v.Body,
		})
	}
	url, err := GetQueueUrl(queueName)
	if err != nil {
		return err
	}
	params := &sqs.SendMessageBatchInput{
		Entries:  input,
		QueueUrl: url,
	}
	_, err = svc.SendMessageBatch(params)
	return err
}

func DeleteMessageBatch(queueName string, messages []*sqs.Message) error {
	url, err := GetQueueUrl(queueName)
	if err != nil {
		return err
	}
	input := []*sqs.DeleteMessageBatchRequestEntry{}
	for _, v := range messages {
		input = append(input, &sqs.DeleteMessageBatchRequestEntry{
			Id:            v.MessageId,
			ReceiptHandle: v.ReceiptHandle})
	}
	params := &sqs.DeleteMessageBatchInput{
		Entries:  input,
		QueueUrl: url}
	_, err = svc.DeleteMessageBatch(params)
	return err
}
