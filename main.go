package main

import (
	//"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/patrickmn/go-cache"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Max request per minute
const fail_limit_value int = 1
const rate_limit_value int = 1000
const limit_expiration time.Duration = time.Minute

var api_key_cache = cache.New(5*time.Minute, 10*time.Minute)
var request_fail_cache = cache.New(5*time.Minute, 10*time.Minute)
var rate_limit_cache = cache.New(5*time.Minute, 10*time.Minute)

func main() {

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	router := http.NewServeMux()
	router.HandleFunc("POST /", incomingRequestHandler)

	err := http.ListenAndServe(":8080", router)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Server running on :8080")
}

func incomingRequestHandler(w http.ResponseWriter, r *http.Request) {

	origin_ip, err := get_origin_ip(r)
	if err != nil {
		w.WriteHeader(http.StatusPreconditionFailed)
		fmt.Println(err)
		io.WriteString(w, "Unable to check origin IP")
		return
	}

	if apply_fail_limit(origin_ip) {
		fmt.Printf("Fail limit applied to IP: %s\n", origin_ip)
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "Too many failed requests in less than a minute")
		return
	}

	if apply_rate_limit(origin_ip) {
		fmt.Printf("Rate limit applied to IP: %s\n", origin_ip)
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "Too many requests in less than a minute")
		return
	}

	err = r.ParseForm()
	if err != nil {
		register_request_fail(origin_ip)
		fmt.Println("Error parsing form: ", err)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	api_key := r.Form.Get("ApiKey")
	if api_key == "" {
		register_request_fail(origin_ip)
		err = errors.New("ApiKey not specified")
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	id_platform, err := get_platform(api_key)
	if err == ErrApiKeyNotFound {
		register_request_fail(origin_ip)
		err = errors.New("ApiKey not found")
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, err.Error())
		return
	}
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "An error has occurred")
		return
	}

	// Request Ok, send to broker to process
	fmt.Printf("Request queued for platform %s\n", id_platform)
	publish_message([]byte{})
}

func get_platform(api_key string) (string, error) {

	// Search first in cache
	if platform, found := api_key_cache.Get(api_key); found {
		fmt.Println("Cache hit")
		return platform.(string), nil
	}

	fmt.Println("Cache miss. Searching in database")

	// If not in cache, retrieve from database
	var id_platform string
	/*
	   row := repository.db.QueryRow("SELECT ID_PLATAFORMA FROM PLATAFORMA WHERE API_KEY = ?", api_key)
	   if err := row.Scan(id_platform); err != nil {
	       if err == sql.ErrNoRows {
	           fmt.Println("Get plaform by Api Key [%d]: no platform found", api_key)
	           return "", ErrApiKeyNotFound
	       }
	       return "", fmt.Errorf("Get platform by Api Key [%d] from DB : %v", api_key, err)
	   }
	*/
	fmt.Printf("ApiKey found in database. Platform %s\n", id_platform)
	api_key_cache.Set(api_key, id_platform, cache.NoExpiration)

	return id_platform, nil

}

func apply_fail_limit(origin_ip string) bool {
	// Search the ip in the cache to apply fail limit
	if value, found := request_fail_cache.Get(origin_ip); found {
		return value.(int) >= fail_limit_value
	}

	return false
}

func apply_rate_limit(origin_ip string) bool {

	// Search the ip in the cache to apply rate limit
	if value, found := rate_limit_cache.Get(origin_ip); found {
		return value.(int) >= rate_limit_value
	}

	// Increase the counter only if rate limit is not applied
	_, err := rate_limit_cache.IncrementInt(origin_ip, 1)
	if err != nil {
		rate_limit_cache.Set(origin_ip, 1, cache.NoExpiration)
	}

	// Decrease the counter after reaching the expiration time
	time.AfterFunc(limit_expiration, func() {
		value, err := rate_limit_cache.DecrementInt(origin_ip, 1)
		if value < 0 || err != nil {
			rate_limit_cache.Set(origin_ip, 0, cache.NoExpiration)
		}
	})

	return false
}

func register_request_fail(origin_ip string) {

	_, err := request_fail_cache.IncrementInt(origin_ip, 1)
	if err != nil {
		request_fail_cache.Set(origin_ip, 1, cache.NoExpiration)
	}

	// Decrease the counter after reaching the expiration time
	time.AfterFunc(limit_expiration, func() {
		value, err := request_fail_cache.DecrementInt(origin_ip, 1)
		if value < 0 || err != nil {
			request_fail_cache.Set(origin_ip, 0, cache.NoExpiration)
		}
	})

}

func get_origin_ip(r *http.Request) (string, error) {

	forwarderForHeader := r.Header.Get("X-Forwarded-For")
	if forwarderForHeader == "" {
		return "", errors.New("X-Forwarded-For value is empty")
	}

	// Find the index of the first comma
	index := strings.Index(forwarderForHeader, ",")

	// Extract the substring until the first comma
	var origin_ip string
	if index != -1 {
		origin_ip = forwarderForHeader[:index]
	} else {
		origin_ip = forwarderForHeader // If there is no comma, take the whole string
	}

	if !is_valid_ip_address(origin_ip) {
		return "", fmt.Errorf("detected IP is not valid: %s", origin_ip)
	}

	return origin_ip, nil
}

func is_valid_ip_address(ip string) bool {
	// Regular expression to match IPv4 addresses
	pattern := `^(\d{1,3}\.){3}\d{1,3}$`

	// Compile the regular expression
	regex := regexp.MustCompile(pattern)

	// Check if the IP address matches the pattern
	return regex.MatchString(ip)
}

func publish_message(message []byte) {

	rabbit_user := os.Getenv("RABBIT_USER")
	rabbit_pwd := os.Getenv("RABBIT_PWD")
	rabbit_host := os.Getenv("RABBIT_HOST")
	rabbit_port := os.Getenv("RABBIT_PORT")
	// TODO define queue
	rabbit_queue := "TEST_QUEUE"
	rabbit_exchange := "TEST_EXCHANGE"

	// RabbitMQ server connection information
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", rabbit_user, rabbit_pwd, rabbit_host, rabbit_port))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare a queue
	q, err := ch.QueueDeclare(
		rabbit_queue, // Queue name
		true,         // Durable
		false,        // Delete when unused
		false,        // Exclusive
		false,        // No-wait
		nil,          // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Publish the message to the queue
	err = ch.Publish(
		rabbit_exchange, // Exchange
		q.Name,          // Routing key
		false,           // Mandatory
		false,           // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	if err != nil {
		log.Fatalf("Failed to publish a message: %v", err)
	}

	fmt.Println("Message sent successfully!")

}

var (
	ErrApiKeyNotFound = errors.New("not found")
)
