package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	file, err := os.Open("user.csv")
	if err != nil {
		log.Fatal("Error while reading the file:", err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)

	var wg sync.WaitGroup
	wg.Add(2)

	dataChan := Read(&wg, csvReader)
	ageCountChan := make(chan int)

	go calculateAge(&wg, dataChan, ageCountChan)

	// Start a goroutine to close the ageCountChan when processing is done
	go func() {
		wg.Wait()
		close(ageCountChan)
	}()

	totalCount := 0
	for count := range ageCountChan {
		totalCount += count
	}

	fmt.Printf("Number of people above 30 years old: %d\n", totalCount)
}

// Read function reads CSV lines and sends them to a channel
func Read(wg *sync.WaitGroup, csvReader *csv.Reader) <-chan []string {
	out := make(chan []string)
	go func() {
		defer wg.Done()
		defer close(out)

		_, err := csvReader.Read()
		if err != nil {
			log.Println("Error reading header:", err)
			return
		}

		// Read the records
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading record: %v", err)
				continue
			}
			if len(record) < 4 {
				log.Println("Invalid record:", record)
				continue
			}
			out <- record
		}
	}()
	return out
}

// calculateAge function processes user data and sends counts to a channel
func calculateAge(wg *sync.WaitGroup, in <-chan []string, out chan<- int) {
	defer wg.Done()

	count := 0
	for record := range in {
		birthdate := record[3]

		date, err := time.Parse("2006/01/02", birthdate)
		if err != nil {
			log.Printf("Error parsing date '%s': %v", birthdate, err)
			continue
		}

		personAge := age(date)
		if personAge > 30 {
			count++
		}
	}

	log.Println("calculateAge completed. Sending count:", count)
	out <- count // Send the final count to the channel
}

// age function calculates age based on the birthdate
func age(birthday time.Time) int {
	now := time.Now()
	years := now.Year() - birthday.Year()
	if now.YearDay() < birthday.YearDay() {
		years--
	}
	return years
}
