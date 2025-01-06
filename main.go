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

	// Add one goroutine for Read and one for calculateAge
	wg.Add(2)
	dataChan := Read(&wg, csvReader)
	ageCountChan := calculateAge(&wg, dataChan)

	// Wait for both goroutines to complete
	wg.Wait()

	// Safely read from ageCountChan
	result := <-ageCountChan
	fmt.Printf("Number of people above 30 years old: %d\n", result)
}

// Read function reads CSV lines and sends them to a channel
func Read(wg *sync.WaitGroup, csvReader *csv.Reader) <-chan []string {
	out := make(chan []string)
	go func() {
		defer wg.Done()
		defer close(out)

		// Skip the header row
		_, err := csvReader.Read()
		if err != nil {
			log.Println("Error reading header:", err)
			return
		}

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading record: %v", err)
				continue
			}
			// Validate record has the required number of fields
			if len(record) < 4 {
				log.Println("Invalid record:", record)
				continue
			}
			out <- record
		}
	}()
	return out
}

// calculateAge function processes user data and counts people above 30
func calculateAge(wg *sync.WaitGroup, in <-chan []string) <-chan int {
	out := make(chan int)
	go func() {
		defer wg.Done()
		defer close(out)

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
		out <- count
	}()
	return out
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
