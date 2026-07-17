package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/redis/go-redis/v9"
)

type Storage interface {
	Set(ctx context.Context, shortURL string, longURL string) error
	Get(ctx context.Context, shortURL string) (string, error)
}

type PostgresStorage struct {
	db *sql.DB
}

type CachedStorage struct {
	db    *PostgresStorage   // Наш старый код для работы с PostgreSQL
	cache *redis.Client      // Клиент для работы с Redis
	sf    singleflight.Group // Группа для защиты от Thundering Herd
}

// Get ищет длинный URL по короткому
func (c *CachedStorage) Get(ctx context.Context, shortURL string) (string, error) {

	// ЭТАП 1: Идем в кэш (Cache-Aside: шаг 1)
	val, err := c.cache.Get(ctx, shortURL).Result()
	if err == nil {
		return val, nil // Cache Hit! Возвращаем данные за миллисекунду
	}

	// Если ключа в кэше нет, драйвер Redis возвращает специальную ошибку redis.Nil
	// Если ошибка какая-то другая (например, сеть упала) - отдаем ее наверх
	if err != redis.Nil { // <--- ПРОПУСК 1: Какую ошибку мы здесь ожидаем при Cache Miss?
		return "", err
	}

	// ЭТАП 2: Cache Miss. Идем в базу данных, защищаясь от стада бизонов
	// Метод Do гарантирует, что для одинакового shortURL функция выполнится ТОЛЬКО ОДИН РАЗ
	v, err, _ := c.sf.Do(shortURL, func() (interface{}, error) {

		// Идем в настоящий PostgreSQL
		longURL, dbErr := c.db.Get(ctx, shortURL)
		if dbErr != nil {
			return nil, dbErr
		}

		// ЭТАП 3: Асинхронно сохраняем результат в кэш (Cache-Aside: шаг 2)
		// Запускаем в отдельной горутине, чтобы не заставлять клиента ждать записи
		go func() {
			// Метод Set принимает: контекст, ключ, значение, и время жизни (TTL)
			c.cache.Set(context.Background(), shortURL, longURL, 24*time.Hour) // <--- ПРОПУСК 2: Как в пакете time указать часы?
		}()

		return longURL, nil
	})

	if err != nil {
		return "", err
	}

	// Приводим пустой интерфейс к строке и возвращаем клиенту
	return v.(string), nil
}

func NewPostgresStorage(db *sql.DB) *PostgresStorage {
	return &PostgresStorage{
		db: db,
	}
}

func (ps *PostgresStorage) Set(ctx context.Context, shortURL string, longURL string) error {
	query := `INSERT INTO urls (short_url, long_url) VALUES ($1, $2)`
	_, execError := ps.db.ExecContext(ctx, query, shortURL, longURL)
	return execError
}

func (ps *PostgresStorage) Get(ctx context.Context, shortURL string) (string, error) {
	query := `SELECT long_url FROM urls WHERE short_url = $1`
	var longURL string
	erro := ps.db.QueryRowContext(ctx, query, shortURL).Scan(&longURL)
	if erro != nil {
		return "", erro
	}
	return longURL, nil

}

type URLStorage struct {
	URLmap      map[string]string
	mu          sync.RWMutex
	analyticsCh chan string
}

// создание мапы
func NewURLStorage(ctx context.Context) *URLStorage {
	s := &URLStorage{
		URLmap:      make(map[string]string),
		analyticsCh: make(chan string, 100),
	}

	go func() {
		for {
			select {
			case url := <-s.analyticsCh:
				fmt.Printf("Аналитика: переход по ссылке %s\n", url)
			case <-ctx.Done():
				return

			}
		}
	}()

	return s

}

func HandleRedirect(storage Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Ne get", http.StatusMethodNotAllowed)
			return
		}
		urlWithSlesh := r.URL.Path
		urlWithoutSlesh := urlWithSlesh[1:]
		if v, err := storage.Get(r.Context(), urlWithoutSlesh); err == nil {
			// не понял почему так нельзя w = v
			http.Redirect(w, r, v, http.StatusFound)
		} else {
			http.NotFound(w, r)
		}
	}
}

type CreateRequest struct {
	ShortURL string `json:"short_url"`
	LongURL  string `json:"long_url"`
}

func HandleCreate(storage Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Ne post", http.StatusMethodNotAllowed)
			return
		}

		var cr CreateRequest
		err := json.NewDecoder(r.Body).Decode(&cr)
		if err != nil {
			http.Error(w, "Json error", http.StatusBadRequest)
			return
		}

		storage.Set(r.Context(), cr.ShortURL, cr.LongURL)

		fmt.Fprint(w, "Ссылка успешно создана!")
	}
}

func (s *URLStorage) Set(ctx context.Context, shortURL string, longURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.URLmap[shortURL] = longURL
	return sql.ErrNoRows
}

func (s *URLStorage) Get(ctx context.Context, shortURL string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case s.analyticsCh <- shortURL:
	default:
	}
	if value, ok := s.URLmap[shortURL]; ok {
		return value, sql.ErrNoRows
	}

	return "", sql.ErrNoRows

}
