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

type Scraper struct {
	ctx        context.Context
	hltb       *howlongtobeat.Client
	f          *os.File
	outName    string
	mu         sync.Mutex
	wroteFirst bool
	wroteCount atomic.Int64
	totalCount int
	pageSize   int
	totalPages int
	nextPage   atomic.Int32
	workers    int
}

func NewScraper(ctx context.Context, outName string, f *os.File, workers int, pageSize int) *Scraper {
	client, err := howlongtobeat.New()
	if err != nil {
		log.Fatalf("hltb.New error: %v", err)
	}
	s := &Scraper{
		ctx:      ctx,
		hltb:     client,
		f:        f,
		outName:  outName,
		workers:  workers,
		pageSize: pageSize,
	}
	return s
}

func (s *Scraper) writeHeader() {
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
	if _, err := s.f.Write(encHeader[:len(encHeader)-1]); err != nil {
		log.Fatalf("write header: %v", err)
	}
	if _, err := s.f.WriteString(",\"data\":["); err != nil {
		log.Fatalf("write data open bracket: %v", err)
	}
}

func (s *Scraper) writeFooter() {
	if _, err := s.f.WriteString("]}"); err != nil {
		log.Fatalf("write footer: %v", err)
	}
}

func (s *Scraper) writeItemBytes(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.wroteFirst {
		if _, err := s.f.Write(b); err != nil {
			log.Printf("Write first item error: %v", err)
			return
		}
		s.wroteFirst = true
	} else {
		if _, err := s.f.WriteString(","); err != nil {
			log.Printf("Write comma error: %v", err)
			return
		}
		if _, err := s.f.Write(b); err != nil {
			log.Printf("Write item error: %v", err)
			return
		}
	}
	s.wroteCount.Add(1)
}

func (s *Scraper) processFirstPage() {
	res, err := s.fetchPage(1, false)
	if err != nil {
		log.Fatalf("First page search error: %v", err)
	}
	s.totalCount = res.Count
	for _, game := range res.Data {
		b, err := json.Marshal(game)
		if err != nil {
			log.Printf("Marshal game error: %v", err)
			continue
		}
		s.writeItemBytes(b)
	}
	s.totalPages = res.PageTotal
	log.Printf("Total pages: %d", s.totalPages)
	s.nextPage.Store(2)
}

func (s *Scraper) fetchPage(p int, sleep bool) (*howlongtobeat.SearchGame, error) {
	if sleep {
		time.Sleep(rand.N(100 * time.Millisecond))
	}
	res, err := s.hltb.Search(s.ctx, "", howlongtobeat.SearchModifierNone, &howlongtobeat.SearchOptions{
		Pagination: &howlongtobeat.SearchGamePagination{
			Page:     p,
			PageSize: s.pageSize,
		},
		Search: false,
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Scraper) workerLoop() {
	for {
		p := int(s.nextPage.Add(1) - 1)
		if p > s.totalPages {
			return
		}
		res, err := s.fetchPage(p, true)
		if err != nil {
			log.Printf("Search page %d error: %v", p, err)
			continue
		}
		for _, game := range res.Data {
			b, err := json.Marshal(game)
			if err != nil {
				log.Printf("Marshal game error: %v", err)
				continue
			}
			s.writeItemBytes(b)
		}
		log.Printf("Page %d of %d scraped", p, s.totalPages)
	}
}

func (s *Scraper) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(s.workers)
	for i := 0; i < s.workers; i++ {
		go func() {
			defer wg.Done()
			s.workerLoop()
		}()
	}
	wg.Wait()
}

func main() {
	ctx := context.Background()

	outName := fmt.Sprintf("hltb-%s.json", time.Now().UTC().Format("20060102"))
	f, err := os.Create(outName)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()
	workers := runtime.NumCPU()
	pageSize := 20

	s := NewScraper(ctx, outName, f, workers, pageSize)
	s.writeHeader()
	s.processFirstPage()
	s.runWorkers()
	s.writeFooter()

	log.Printf("Scraping completed. Result written to %s", s.outName)
	log.Printf("API total count=%d; actually written=%d", s.totalCount, s.wroteCount.Load())
}
