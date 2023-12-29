package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sod-auctions/auctions-db"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"io"
	"net/url"
)

func download(ctx context.Context, record *events.S3EventRecord) (*s3.GetObjectOutput, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(record.S3.Object.Key),
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

func mapRowToAuction(row []string, t *time.Time, interval int16) *auctions_db.Auction {
	return &auctions_db.Auction{
		RealmID:        int16(parseIntOrCrash(row[0], 10, 16)),
		AuctionHouseID: int16(parseIntOrCrash(row[1], 10, 16)),
		ItemID:         int(parseIntOrCrash(row[2], 10, 64)),
		Interval:       interval,
		Timestamp:      int32(t.Unix()),
		Quantity:       int32(parseIntOrCrash(row[3], 10, 32)),
		Min:            int32(parseIntOrCrash(row[4], 10, 32)),
		Max:            int32(parseIntOrCrash(row[5], 10, 32)),
		P05:            int32(parseIntOrCrash(row[6], 10, 32)),
		P10:            int32(parseIntOrCrash(row[7], 10, 32)),
		P25:            int32(parseIntOrCrash(row[8], 10, 32)),
		P50:            int32(parseIntOrCrash(row[9], 10, 32)),
		P75:            int32(parseIntOrCrash(row[10], 10, 32)),
		P90:            int32(parseIntOrCrash(row[11], 10, 32)),
	}
}

func handler(ctx context.Context, event events.S3Event) error {
	database, err := auctions_db.NewDatabase(os.Getenv("DB_CONNECTION_STRING"))
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	for _, record := range event.Records {
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

		log.Printf("downloading file %s", key)
		file, err := download(ctx, &record)
		defer file.Body.Close()

		log.Printf("reading auctions from file..")
		var auctions []*auctions_db.Auction
		r := csv.NewReader(file.Body)

		_, err = r.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV header: %v", err)
		}

		interval := int16(parseIntOrCrash(dateInfo["interval"], 10, 16))
		year := parseIntOrCrash(dateInfo["year"], 10, 64)
		month := parseIntOrCrash(dateInfo["month"], 10, 64)
		day := parseIntOrCrash(dateInfo["day"], 10, 64)
		hour := parseIntOrCrash(dateInfo["hour"], 10, 64)
		t := time.Date(int(year), time.Month(month), int(day), int(hour), 0, 0, 0, time.UTC)

		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading CSV file: %v", err)
			}
			auctions = append(auctions, mapRowToAuction(row, &t, interval))
		}

		log.Printf("writing %d auctions to database", len(auctions))
		err = database.InsertAuctions(auctions)
		if err != nil {
			return fmt.Errorf("error inserting auctions into database: %v", err)
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
