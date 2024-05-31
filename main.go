package main
import (
    "fmt"
    "log"
    "net/http"
    "github.com/patrickmn/go-cache"

)

// Max request per minute
const fail_limit_value int = 1
const rate_limit_value int = 1000
const limit_expiration time.Duration = time.Minute

func main() {

    api_key_cache      := cache.New(5*time.Minute, 10*time.Minute)
    request_fail_cache := cache.New(5*time.Minute, 10*time.Minute)
    rate_limit_cache   := cache.New(5*time.Minute, 10*time.Minute)

	router := http.NewServeMux()
	router.HandleFunc("POST /", incomingRequestHandler)

    err := http.ListenAndServe(":8080", router)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Server running on :8080")
}

function incomingRequestHandler(w http.ResponseWriter, r *http.Request){

    if apply_fail_limit(origin_ip) {
        fmt.Println("Fail limit applied to IP: " . origin_ip)
        w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "Too many failed requests in less than a minute")
        return
    }

    if apply_rate_limit(origin_ip) {
        fmt.Println("Rate limit applied to IP: " . origin_ip)
        w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "Too many requests in less than a minute")
        return
    }

    err := r.ParseForm()
    if err != nil {
        register_request_fail(origin_ip)
        fmt.Println("Error parsing form:", err)
        w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
        return
    }

    api_key := r.Form.Get("ApiKey")
    if api_key == ''{
        register_request_fail(origin_ip)
        err = errors.New("ApiKey not specified")
        fmt.Println(err.Error())
        w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
        return
    }

    id_platform, err = get_platform(api_key)
    if err == ApiKeyNotFound {
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
    // TODO
}

func get_platform(api_key string) (string, error){

    // Search first in cache
    if platform, found := api_key_cache.Get(api_key); found {
		return platform.(string)
	}

    // If not in cache, retrieve from database
    var id_platform string
    row := repository.db.QueryRow("SELECT ID_PLATAFORMA FROM PLATAFORMA WHERE API_KEY = ?", api_key)
    if err := row.Scan(id_platform); err != nil {
        if err == sql.ErrNoRows {
            fmt.Println("Get plaform by Api Key [%d]: no platform found", api_key)
            return nil, ApiKeyNotFound
        }
        return nil, fmt.Errorf("Get platform by Api Key [%d] from DB : %v", api_key, err)
    }
    
    api_key_cache.Set(api_key, id_platform, cache.NoExpiration)

    return id_platform

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
    value, err := rate_limit_cache.IncrementInt(origin_ip, 1)
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

func register_request_fail(origin_ip string){

    value, err := request_fail_cache.IncrementInt(origin_ip, 1)
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

var (
	ApiKeyNotFound = errors.New("not found")
)