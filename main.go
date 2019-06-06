package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Origin redisConfig `yaml:"origin"`
	Target redisConfig `yaml:"target"`
}

type redisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database int    `yaml:"database"`
	Password string `yaml:"password"`
	Secure   bool   `yaml:"secure"`
}

type Migration struct {
	Mode    int
	Pattern string
	Workers int
}

type BoundedWaitGroup struct {
	sync.WaitGroup
	ch chan struct{}
}

type Stats struct {
	Total     int
	Completed int
	Pattern   string
	Duration  time.Duration
	Pending   []string
}

func NewBoundedWaitGroup(cap int) BoundedWaitGroup {
	return BoundedWaitGroup{ch: make(chan struct{}, cap)}
}

func (bwg *BoundedWaitGroup) Add(delta int) {
	for i := 0; i > delta; i-- {
		<-bwg.ch
	}
	for i := 0; i < delta; i++ {
		bwg.ch <- struct{}{}
	}
	bwg.WaitGroup.Add(delta)
}

func (bwg *BoundedWaitGroup) Done() {
	bwg.Add(-1)
}

func (bwg *BoundedWaitGroup) Wait() {
	bwg.WaitGroup.Wait()
}

const (
	ModeCopy = iota
	ModeMove
)

var (
	modes = map[string]int{
		"COPY": ModeCopy,
		"MOVE": ModeMove,
	}
	mutex sync.Mutex
)

func newRedisClient(conf *redisConfig) *redis.Client {
	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Password: conf.Password,
		DB:       conf.Database,
	}
	if conf.Secure {
		options.TLSConfig = &tls.Config{
			ServerName: strings.Split(options.Addr, ":")[0],
		}
	}
	return redis.NewClient(options)
}

func migrate(config *Config, migration *Migration) (*Stats, error) {
	init := time.Now()
	origin := newRedisClient(&config.Origin)
	target := newRedisClient(&config.Target)

	if err := origin.Ping().Err(); err != nil {
		return nil, fmt.Errorf("could not connect to origin redis server: %+v", err)
	}

	if err := target.Ping().Err(); err != nil {
		return nil, fmt.Errorf("could not connect to target redis server: %+v", err)
	}

	bw := NewBoundedWaitGroup(migration.Workers)
	scan := origin.Scan(0, migration.Pattern, origin.DBSize().Val())
	keys, _ := scan.Val()
	stats := &Stats{
		Pattern:   migration.Pattern,
		Completed: len(keys),
		Total:     len(keys),
	}
	for _, key := range keys {
		bw.Add(1)
		go move(origin, target, migration.Mode, &bw, stats, key)
	}
	bw.Wait()
	stats.Duration = time.Since(init)
	return stats, nil
}

func move(origin, target *redis.Client, mode int, bw *BoundedWaitGroup, stats *Stats, key string) {
	defer bw.Done()

	keyTtl := origin.TTL(key)
	keyGet := origin.Get(key)

	var hasError bool
	if err := keyGet.Err(); err != nil {
		hasError = true
		log.Printf("could not get key %s from origin redis: %+v\n", key, keyGet.Err())
	}
	if err := keyTtl.Err(); err != nil {
		hasError = true
		log.Printf("could not get key %s ttl from origin redis: %+v\n", key, keyTtl.Err())
	}

	// set no expiration by default
	exp := 0 * time.Second
	if keyTtl.Val().Seconds() > 0 {
		exp = keyTtl.Val()
	}
	keySet := target.Set(key, keyGet.Val(), exp)
	if err := keySet.Err(); err != nil {
		log.Printf("could not set key %s to target redis: %+v\n", key, keySet.Err())
		hasError = true
	}
	if hasError {
		mutex.Lock()
		stats.Completed -= 1
		stats.Pending = append(stats.Pending, key)
		mutex.Unlock()
	}

	if !hasError && mode == ModeMove {
		if err := origin.Del(key).Err(); err != nil {
			log.Printf("could not delete keys: %+v\n", err)
		}
	}
}

func printStats(stats *Stats) {
	statsMessage := "migration completed\npattern: '%s'\nduration: %+vs\ntotal: %d\nfailed: %d\n"
	fmt.Printf(statsMessage, stats.Pattern, stats.Duration.Seconds(), stats.Total, stats.Total-stats.Completed)
	if len(stats.Pending) > 0 {
		fmt.Printf("\npending keys:\n")
		for _, pending := range stats.Pending {
			fmt.Printf("  %s\n", pending)
		}
	}
}

func main() {
	var (
		config     string
		pattern    string
		mode       string
		workers    int
		maxWorkers = 500
	)

	flag.StringVar(&config, "config", "config.yaml", "config path")
	flag.StringVar(&pattern, "pattern", "*", "pattern to be used with `KEYS <pattern>` command")
	flag.StringVar(&mode, "mode", "COPY", "migration mode, COPY|MOVE (COPY: replicate, MOVE: replicate and delete)")
	flag.IntVar(&workers, "workers", maxWorkers, fmt.Sprintf("number max of workers to sync (max: %d)", maxWorkers))
	flag.Parse()

	if _, err := os.Stat(config); err != nil {
		log.Fatalf("could not load config file: %+v", err)
	}
	if _, ok := modes[mode]; !ok {
		log.Fatalf("unsupported mode %s. Instead use `COPY` or `MOVE` modes", mode)
	}

	if workers > maxWorkers {
		workers = maxWorkers
	}

	var settings Config
	buf, err := ioutil.ReadFile(config)
	if err != nil {
		log.Fatalf("could not read config file: %+v", err)
	}
	if err := yaml.Unmarshal(buf, &settings); err != nil {
		log.Fatalf("could not parse config file: %+v", err)
	}

	migration := &Migration{Mode: modes[mode], Pattern: pattern, Workers: workers}
	stats, err := migrate(&settings, migration)
	if err != nil {
		log.Fatalf("could not migrate: %+v\n", err)
	}
	printStats(stats)
}
