package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShadowDash2000/howlongtobeat"
)

func main() {
	hltb, err := howlongtobeat.New()
	if err != nil {
		log.Fatalf("hltb.New error: %v", err)
	}

	ctx := context.Background()

	outName := fmt.Sprintf("hltb-%s.json", time.Now().UTC().Format("20060102"))
	f, err := os.Create(outName)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()

	header := struct {
		SchemaVersion int    `json:"schema_version"`
		ScrapedAt     string `json:"scraped_at"`
		Source        string `json:"source"`
	}{
		SchemaVersion: 1,
		ScrapedAt:     time.Now().UTC().Format(time.RFC3339),
		Source:        "hltb",
	}

	encHeader, _ := json.Marshal(header)
	if _, err := f.Write(encHeader[:len(encHeader)-1]); err != nil {
		log.Fatalf("write header: %v", err)
	}
	if _, err := f.WriteString(",\"data\":["); err != nil {
		log.Fatalf("write data open bracket: %v", err)
	}

	ch := make(chan []byte, 1)

	var (
		mu          sync.Mutex
		wroteFirst  bool
		consumersWG sync.WaitGroup
		wroteCount  atomic.Int64
	)

	workers := runtime.NumCPU()
	consumersWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer consumersWG.Done()
			for b := range ch {
				mu.Lock()
				if !wroteFirst {
					if _, err := f.Write(b); err != nil {
						mu.Unlock()
						log.Printf("write first item error: %v", err)
						continue
					}
					wroteFirst = true
					wroteCount.Add(1)
				} else {
					if _, err := f.WriteString(","); err != nil {
						mu.Unlock()
						log.Printf("write comma error: %v", err)
						continue
					}
					if _, err := f.Write(b); err != nil {
						mu.Unlock()
						log.Printf("write item error: %v", err)
						continue
					}
					wroteCount.Add(1)
				}
				mu.Unlock()
			}
		}()
	}

	var reportedCount int
	go func() {
		defer close(ch)

		pageSize := 20
		firstRes, err := hltb.Search(ctx, "", howlongtobeat.SearchModifierNone, &howlongtobeat.SearchOptions{
			Pagination: &howlongtobeat.SearchGamePagination{
				Page:     1,
				PageSize: pageSize,
			},
			Search: false,
		})
		if err != nil {
			log.Printf("first search error: %v", err)
			return
		}

		reportedCount = firstRes.Count

		for _, game := range firstRes.Data {
			b, err := json.Marshal(game)
			if err != nil {
				log.Printf("marshal game error: %v", err)
				continue
			}
			ch <- b
		}

		totalPages := firstRes.PageTotal
		log.Printf("Total pages: %d", totalPages)
		for p := 2; p <= totalPages; p++ {
			time.Sleep(rand.N(100 * time.Millisecond))

			res, err := hltb.Search(ctx, "", howlongtobeat.SearchModifierNone, &howlongtobeat.SearchOptions{
				Pagination: &howlongtobeat.SearchGamePagination{
					Page:     p,
					PageSize: pageSize,
				},
				Search: false,
			})
			if err != nil {
				log.Printf("search page %d error: %v", p, err)
				continue
			}
			for _, game := range res.Data {
				b, err := json.Marshal(game)
				if err != nil {
					log.Printf("marshal game error: %v", err)
					continue
				}
				ch <- b
			}
		}
	}()
	consumersWG.Wait()

	if _, err := f.WriteString("]}"); err != nil {
		log.Fatalf("write footer: %v", err)
	}

	log.Printf("Scraping completed. Result written to %s", outName)
	log.Printf("API reported total count=%d; actually written=%d", reportedCount, wroteCount.Load())
}
