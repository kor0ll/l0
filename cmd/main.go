package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/kor0ll/l0/internal/broker"
	"github.com/kor0ll/l0/internal/common"
	"github.com/kor0ll/l0/internal/postgres"
)

var Cache = make(map[string]*common.Order)

// Handler для /
func GetMainPage(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{
		"title": "Введите uid заказа:",
	})
}

// Handler для /order
func GetOrder(c *gin.Context) {
	//достаем значение параметра, которое было отправлено с html формы
	id := c.Query("uid")
	if id == "" {
		GetMainPage(c)
	} else {
		order, ok := Cache[id]
		if !ok {
			c.HTML(http.StatusNotFound, "order.tmpl", gin.H{
				"title": "Id не найден!",
				"data":  "",
			})
		} else {
			jsonData, _ := json.MarshalIndent(&order, "", "  ")
			c.HTML(http.StatusOK, "order.tmpl", gin.H{
				"title": "Order with uid=" + id,
				"data":  fmt.Sprint(string(jsonData)),
			})
		}
	}
}

func main() {
	router := gin.Default()
	router.LoadHTMLGlob("internal/templates/*")

	//соединение с бд
	db, err := postgres.NewPostgresDB(postgres.Config{
		Host:     "localhost",
		Port:     "5432",
		Username: "oleg",
		Password: "root",
		DBName:   "l0_task_db",
		SSLMode:  "disable",
	})
	if err != nil {
		fmt.Println("Не удалось создать подключение к бд")
		log.Fatal(err)
	}
	fmt.Println("Подключение к бд успешно")

	var count int
	if err := postgres.GetOrdersCount(db, &count); err != nil {
		log.Fatal(err)
	}

	if count == 0 {
		// если данных в БД нет, запускаем nats-streaming для получения данных
		go broker.StartStan(db, Cache)
	} else {
		// если данные есть, заполняем ими кэш
		go postgres.PullCache(db, Cache)
	}

	router.GET("/", GetMainPage)
	router.GET("/order", GetOrder)

	router.Run(":8080")
}
