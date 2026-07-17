package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		// Если мы запускаем код локально без Докера, используем этот fallback
		dsn = "postgres://admin:secretpassword@localhost:5432/shortener?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}

	// 2. Механизм Retry (ждем базу)
	var dbErr error
	for i := 1; i <= 5; i++ {
		dbErr = db.Ping()
		if dbErr == nil {
			fmt.Println("Успешное подключение к PostgreSQL!")
			break // База ответила, выходим из цикла
		}
		fmt.Printf("База данных недоступна (попытка %d/5), ждем 2 секунды...\n", i)
		time.Sleep(2 * time.Second)
	}

	// Если после 5 попыток база так и не ожила - тогда уже падаем
	if dbErr != nil {
		panic(fmt.Sprintf("Не удалось подключиться к БД после 5 попыток: %v", dbErr))
	}

	// === ДОБАВЬ ЭТОТ БЛОК ===
	// Создаем таблицу, если ее еще нет
	query := `CREATE TABLE IF NOT EXISTS urls (
        short_url VARCHAR(255) PRIMARY KEY,
        long_url TEXT NOT NULL
    );`

	_, err = db.Exec(query)
	if err != nil {
		panic(fmt.Sprintf("Ошибка при создании таблицы: %v", err))
	}
	fmt.Println("Таблица urls готова к работе!")
	// ========================

	// ЭТАП 2: Инициализация хранилища и роутеров
	storage := NewPostgresStorage(db)

	http.HandleFunc("/", HandleRedirect(storage))
	http.HandleFunc("/create", HandleCreate(storage))

	// ЭТАП 3: Настройка HTTP-сервера
	srv := &http.Server{
		Addr:    ":8080",
		Handler: nil, // Пока используем стандартный роутер
	}

	// 1. TODO: Создай канал quit, который может передавать значения типа os.Signal.
	// Канал ОБЯЗАТЕЛЬНО должен быть буферизованным (размером 1), чтобы ядро ОС не заблокировалось при отправке.
	quit := make(chan os.Signal, 1)

	// 2. TODO: Подпиши канал quit на системные сигналы SIGINT (Ctrl+C в терминале) и SIGTERM (остановка в Docker).
	// Используй функцию signal.Notify
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 3. Запускаем сервер в отдельной горутине (на фоне)
	go func() {
		fmt.Println("Сервер запущен на порту 8080...")
		// ErrServerClosed возвращается, когда мы сами штатно гасим сервер. Это не ошибка.
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Аварийная остановка сервера: %v\n", err)
		}
	}()

	// 4. TODO: Заблокируй выполнение функции main, ожидая сообщение из канала quit.
	// Подсказка: тебе нужно просто прочитать значение из канала.
	<-quit

	fmt.Println("\nПолучен сигнал завершения от ОС. Начинаем Graceful Shutdown...")

	// Создаем контекст с таймаутом в 5 секунд.
	// Мы даем серверу ровно 5 секунд, чтобы дообработать текущие запросы пользователей.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 5. Выключаем сервер, передав ему контекст с таймаутом.
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Ошибка при остановке сервера: %v\n", err)
	}

	fmt.Println("Сервер успешно и безопасно остановлен. Программа завершена.")
}

/*package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq" // Анонимный импорт драйвера PostgreSQL
)

func main() {
	// 1. Строка подключения (Data Source Name)
	// В реальном проекте эти данные берутся из переменных окружения (.env)
	dsn := "postgres://user:password@localhost:5432/dbname?sslmode=disable"

	// 2. Открываем пул соединений
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Ошибка подключения к БД: %v", err)
	}
	defer db.Close() // Закроем пул при остановке сервера

	// 3. Проверяем, что база реально доступна (Ping)
	if err := db.Ping(); err != nil {
		log.Fatalf("База данных недоступна: %v", err)
	}

	// 4. Инициализируем НАШЕ НОВОЕ хранилище
	storage := NewPostgresStorage(db)

	// 5. Регистрируем хендлеры
	http.HandleFunc("/", HandleRedirect(storage))
	http.HandleFunc("/create", HandleCreate(storage))

	fmt.Println("Сервер запущен на порту 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

package main

import (
	"concurrent_map/concurrentmap"
	"context"
	"fmt"
	"net/http"
)

func main() {
	ctx := context.Background()

	cm := concurrentmap.NewConcurrentMap()
	cm.Set("test_key", "hello")
	value, ok := cm.Get("test_key")
	fmt.Printf("value=%q, ok=%v\n", value, ok)
	cm.Delete("test_key")
	value, ok = cm.Get("test_key")
	fmt.Printf("after delete: value=%q, ok=%v\n", value, ok)

	storage := NewURLStorage(ctx)

	storage.Set("ya", "https://ya.ru")
	storage.Get("ya")
	storage.Get("yas")
	if url, ok := storage.Get("ya"); ok {
		fmt.Printf("Found: %s\n", url)
	} else {
		fmt.Println("URL not found")
	}

	// Пытаемся достать несуществующую ссылку
	if url, ok := storage.Get("google"); ok {
		fmt.Printf("Found: %s\n", url)
	} else {
		fmt.Println("URL not found")
	}

	http.HandleFunc("/", HandleRedirect(storage))
	http.HandleFunc("/create", HandleCreate(storage))
	fmt.Println("Сервер запущен на порту 8080...")
	http.ListenAndServe(":8080", nil)
}
*/
