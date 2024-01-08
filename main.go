package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sod-auctions/auctions-db"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var database *auctions_db.Database

func init() {
	log.SetFlags(0)

	var err error
	database, err = auctions_db.NewDatabase(os.Getenv("DB_CONNECTION_STRING"))
	if err != nil {
		panic(err)
	}
}

func download(ctx context.Context, record *events.S3EventRecord, key string) (*s3.GetObjectOutput, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(key),
	}

	return s3Client.GetObjectWithContext(ctx, input)
}

func parseIntOrCrash(s string, base int, bitSize int) int64 {
	result, err := strconv.ParseInt(s, base, bitSize)
	if err != nil {
		log.Fatalf("could not parse integer '%s'", s)
	}
	return result
}

func mapRowToAuction(row []string) (*auctions_db.Auction, error) {
	t, err := time.Parse(time.RFC3339, row[0])
	if err != nil {
		return nil, err
	}

	return &auctions_db.Auction{
		RealmID:        int16(parseIntOrCrash(row[1], 10, 16)),
		AuctionHouseID: int16(parseIntOrCrash(row[2], 10, 16)),
		ItemID:         int(parseIntOrCrash(row[3], 10, 64)),
		Interval:       1,
		Timestamp:      int32(t.Unix()),
		Quantity:       int32(parseIntOrCrash(row[4], 10, 32)),
		Min:            int32(parseIntOrCrash(row[5], 10, 32)),
		Max:            int32(parseIntOrCrash(row[6], 10, 32)),
		P05:            int32(parseIntOrCrash(row[7], 10, 32)),
		P10:            int32(parseIntOrCrash(row[8], 10, 32)),
		P25:            int32(parseIntOrCrash(row[9], 10, 32)),
		P50:            int32(parseIntOrCrash(row[10], 10, 32)),
		P75:            int32(parseIntOrCrash(row[11], 10, 32)),
		P90:            int32(parseIntOrCrash(row[12], 10, 32)),
	}, nil
}

func sendIdsToSqs(ids []int32) error {
	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	sqsClient := sqs.New(sess)

	batchSize := 10
	for i := 0; i < len(ids); i += batchSize {
		j := i + batchSize
		if j > len(ids) {
			j = len(ids)
		}
		batch := ids[i:j]
		err := sendBatchToSQS(sqsClient, batch)
		if err != nil {
			return err
		}
	}

	return nil
}

func sendBatchToSQS(client *sqs.SQS, batch []int32) error {
	var messages []*sqs.SendMessageBatchRequestEntry
	for _, id := range batch {
		messages = append(messages, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(int(id))),
			MessageBody: aws.String(strconv.Itoa(int(id))),
		})
	}

	_, err := client.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/153163178336/item-ids"),
		Entries:  messages,
	})

	return err
}

func handler(ctx context.Context, snsEvent events.SNSEvent) error {
	for _, snsRecord := range snsEvent.Records {

		var event *events.S3Event
		// Unmarshal record.sns.message json into event
		err := json.Unmarshal([]byte(snsRecord.SNS.Message), &event)
		if err != nil {
			return fmt.Errorf("error unmarshalling SNS message: %v", err)
		}

		record := event.Records[0]

		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			return fmt.Errorf("error decoding S3 object key: %v", err)
		}

		components := strings.Split(key, "/")
		dateInfo := make(map[string]string)
		for _, component := range components {
			parts := strings.Split(component, "=")
			if len(parts) == 2 {
				dateInfo[parts[0]] = parts[1]
			}
		}

		log.Printf("downloading file %s\n", key)
		file, err := download(ctx, &record, key)
		if err != nil {
			return fmt.Errorf("error downloading file: %v", err)
		}
		defer file.Body.Close()

		log.Println("querying item ids from database")
		itemIds, err := database.GetItemIDs()
		if err != nil {
			return fmt.Errorf("error while querying item ids: %v", err)
		}

		log.Println("reading auctions from file..")
		var auctions []*auctions_db.Auction
		r := csv.NewReader(file.Body)

		_, err = r.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV header: %v", err)
		}

		log.Printf("comparing item ids in file against %d ids in database\n", len(itemIds))
		var nonExistentItemIds = make(map[int32]struct{})
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading CSV file: %v", err)
			}

			auction, err := mapRowToAuction(row)
			if err != nil {
				return fmt.Errorf("error mapping row to auction: %v", err)
			}
			auctions = append(auctions, auction)

			itemId := int32(parseIntOrCrash(row[3], 10, 32))
			_, exists := itemIds[itemId]
			if exists == false {
				nonExistentItemIds[itemId] = struct{}{}
			}
		}

		idSlice := make([]int32, 0, len(nonExistentItemIds))
		for id := range nonExistentItemIds {
			idSlice = append(idSlice, id)
		}

		log.Printf("writing %d auctions to auction history\n", len(auctions))
		err = database.InsertAuctions(auctions)
		if err != nil {
			return fmt.Errorf("error writing auction history: %v", err)
		}

		log.Printf("writing %d auctions to current auctions\n", len(auctions))
		err = database.ReplaceCurrentAuctions(auctions)
		if err != nil {
			return fmt.Errorf("error writing current auctions: %v", err)
		}

		log.Printf("found %d item ids to update, writing to queue..\n", len(idSlice))
		err = sendIdsToSqs(idSlice)
		if err != nil {
			return fmt.Errorf("error occurred while sending message to queue, %v", err)
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
