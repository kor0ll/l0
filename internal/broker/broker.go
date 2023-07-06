package broker

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/kor0ll/l0/internal/common"
	"github.com/kor0ll/l0/internal/postgres"
	"github.com/nats-io/stan.go"
)

var jsonData = []string{
	`{
		"order_uid": "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		  "name": "Test Testov",
		  "phone": "+9720000000",
		  "zip": "2639809",
		  "city": "Kiryat Mozkin",
		  "address": "Ploshad Mira 15",
		  "region": "Kraiot",
		  "email": "test@gmail.com"
		},
		"payment": {
		  "transaction": "b563feb7b2b84b6test",
		  "request_id": "",
		  "currency": "USD",
		  "provider": "wbpay",
		  "amount": 1817,
		  "payment_dt": 1637907727,
		  "bank": "alpha",
		  "delivery_cost": 1500,
		  "goods_total": 317,
		  "custom_fee": 0
		},
		"items": [
		  {
			"chrt_id": 9934930,
			"track_number": "WBILMTESTTRACK",
			"price": 453,
			"rid": "ab4219087a764ae0btest",
			"name": "Mascaras",
			"sale": 30,
			"size": "0",
			"total_price": 317,
			"nm_id": 2389212,
			"brand": "Vivienne Sabo",
			"status": 202
		  }
		],
		"locale": "en",
		"internal_signature": "",
		"customer_id": "test",
		"delivery_service": "meest",
		"shardkey": "9",
		"sm_id": 99,
		"date_created": "2021-11-26T06:22:19Z",
		"oof_shard": "1"
	  }`,
	`{
		"order_uid": "test-order-uid",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		  "name": "Oleg Korolev",
		  "phone": "+9771683455",
		  "zip": "2639809",
		  "city": "Kiryat Mozkin",
		  "address": "Ploshad Mira 23",
		  "region": "Kraiot",
		  "email": "korolyov-work@mail.ru"
		},
		"payment": {
		  "transaction": "test-order-uid",
		  "request_id": "",
		  "currency": "USD",
		  "provider": "wbpay",
		  "amount": 2017,
		  "payment_dt": 1637907727,
		  "bank": "alpha",
		  "delivery_cost": 1700,
		  "goods_total": 317,
		  "custom_fee": 0
		},
		"items": [
		  {
			"chrt_id": 9935950,
			"track_number": "WBILMTESTTRACK",
			"price": 453,
			"rid": "ab4219087a764ae0btest",
			"name": "Masdcasras",
			"sale": 30,
			"size": "0",
			"total_price": 317,
			"nm_id": 2389212,
			"brand": "Vivienne Sato",
			"status": 202
		  },
		  {
			"chrt_id": 9911111,
			"track_number": "WBILMTESTTRACK",
			"price": 450,
			"rid": "ab4219087a764ae0btest",
			"name": "Mascaras",
			"sale": 30,
			"size": "0",
			"total_price": 322,
			"nm_id": 2389212,
			"brand": "Vivienne Sawo",
			"status": 202
		  }
		],
		"locale": "en",
		"internal_signature": "",
		"customer_id": "test",
		"delivery_service": "meest",
		"shardkey": "9",
		"sm_id": 99,
		"date_created": "2020-05-12T08:22:23Z",
		"oof_shard": "1"
	  }`,
}

// Создает подключение nats-streaming и отправляет данные в канал
func CreateStanHub(ch chan bool) {
	sc, err := stan.Connect("prod", "simple-pub")
	if err != nil {
		fmt.Println("Не удалось подключение к STAN")
		panic(err)
	}
	fmt.Println("Подключение к STAN успешно")
	defer sc.Close()
	for _, json := range jsonData {
		sc.Publish("oleg", []byte(json))
	}
	ch <- true
}

// Создает подписку на канал nats streaming, при получении данных из канала добавляет их в БД и в кэш
func SubscribeStan(db *sqlx.DB, ch1 chan bool, ch2 chan bool, cache map[string]*common.Order) {
	sc, err := stan.Connect("prod", "sub-1")
	if err != nil {
		fmt.Println("Не удалось подключение к STAN")
		log.Fatal(err)
	}
	fmt.Println("Подключение к STAN успешно")

	sc.Subscribe("oleg", func(m *stan.Msg) {
		if err := postgres.AddOrderToDB(db, string(m.Data), cache); err != nil {
			log.Fatal(err)
		}
	})
	ch1 <- true

	if <-ch2 {
		sc.Close()
	}

}

// Запускает nats-streaming, передает и слушает значения из канала
func StartStan(db *sqlx.DB, cache map[string]*common.Order) {
	ch1 := make(chan bool)
	ch2 := make(chan bool)
	go SubscribeStan(db, ch1, ch2, cache)
	if <-ch1 {
		CreateStanHub(ch2)
	}
}
