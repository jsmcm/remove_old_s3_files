// Remove images from S3 for past competitions.
// If a competition is older than last month
// then remove their images except for the
// winners entries
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type remoteConnection struct {
	Active bool   `json:"active"`
	Bucket string `json:"bucket"`
}

func getAwsConfig() aws.Config {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("Can't load AWS config")
	}

	return config
}

func deleteOrphanedImages(s3Client *s3.Client, bucket string) error {

	fmt.Println("deleteOrphanedImages")
	var continuationToken *string
	// x := 0
	for {
		// List objects in the bucket with the given prefix
		listObjectsInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String("entry/images/"),
			ContinuationToken: continuationToken,
		}

		listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), listObjectsInput)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		if len(listObjectsOutput.Contents) == 0 {
			// No objects found, nothing to delete
			return nil
		}

		// Get the current time
		currentTime := time.Now()

		var objectsToDelete []types.ObjectIdentifier
		// Iterate over the objects in the bucket
		for _, object := range listObjectsOutput.Contents {
			// Calculate the object's age
			objectAge := currentTime.Sub(*object.LastModified)

			// If the object is older than 24 hours, delete it
			if objectAge > time.Hour*24 {
				objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: object.Key})
			}
		}

		// Delete objects
		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		}

		fmt.Println("len(objectsToDelete): ", len(objectsToDelete))
		_, err = s3Client.DeleteObjects(context.TODO(), deleteObjectsInput)
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		if listObjectsOutput.IsTruncated != nil && *listObjectsOutput.IsTruncated {
			continuationToken = listObjectsOutput.NextContinuationToken
		} else {
			break
		}
	}
	return nil

}

func deleteFolder(s3Client *s3.Client, bucket string, prefix string) error {

	var continuationToken *string

	fmt.Println("deleteFolder: ", prefix)
	for {
		// List objects in the bucket with the given prefix
		listObjectsInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), listObjectsInput)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("failed to list objects: %w", err)
		}

		if len(listObjectsOutput.Contents) == 0 {
			// No objects found, nothing to delete
			return nil
		}

		// Prepare the objects to delete
		var objectsToDelete []types.ObjectIdentifier
		for _, object := range listObjectsOutput.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: object.Key})
		}

		// Delete objects
		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		}

		_, err = s3Client.DeleteObjects(context.TODO(), deleteObjectsInput)
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		if listObjectsOutput.IsTruncated != nil && *listObjectsOutput.IsTruncated {
			continuationToken = listObjectsOutput.NextContinuationToken
		} else {
			break
		}
	}
	return nil
}

func main() {

	var connections []remoteConnection

	exePath, err := os.Executable()
	if err != nil {
		log.Fatal("Error reading connections.json: ", err)
	}

	exeDirectory := filepath.Dir(exePath)

	filePath := filepath.Join(exeDirectory, "connections.json")

	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatal("Error reading connections.json: ", err)
	}

	err = json.Unmarshal(fileBytes, &connections)
	if err != nil {
		log.Fatal("error unmarshaling: ", err)
	}

	for _, connection := range connections {

		if !connection.Active {
			continue
		}

		awsConfig := getAwsConfig()
		s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
			o.UsePathStyle = true // Force path-style URLs
		})

		// get folders for 12 months
		month := time.Now()
		month = month.AddDate(0, -1, 0)
		for x := 0; x < 12; x++ {

			month = month.AddDate(0, -1, 0)
			entryBucket := fmt.Sprintf("entry/%s/", month.Format("2006-01"))
			// fmt.Println("entryBucket: ", entryBucket)

			deleteFolder(s3Client, connection.Bucket, entryBucket)
		}

		deleteOrphanedImages(s3Client, connection.Bucket)

	}

}
