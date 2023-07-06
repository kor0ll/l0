package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/kor0ll/l0/internal/common"
)

const (
	orderTable     = "l0_task_schema.order"
	deliveryTable  = "l0_task_schema.delivery"
	paymentTable   = "l0_task_schema.payment"
	itemTable      = "l0_task_schema.item"
	orderItemTable = "l0_task_schema.order_item"
)

// Получает количество записей в БД
func GetOrdersCount(db *sqlx.DB, count *int) error {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", orderTable)
	row := db.QueryRow(query)
	err := row.Scan(count)

	return err
}

// Заполняет кэш данными, находящимися в БД
func PullCache(db *sqlx.DB, cache map[string]*common.Order) error {
	var orders []common.Order
	query := fmt.Sprintf("SELECT order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM %s", orderTable)
	err := db.Select(&orders, query)
	if err != nil {
		return err
	}

	fmt.Println(orders)

	for i, order := range orders {
		var delivery common.Delivery
		var deliveryPhone string
		query := fmt.Sprintf("SELECT delivery FROM %s WHERE order_uid = '%s'", orderTable, orders[i].OrderUid)
		err := db.Get(&deliveryPhone, query)
		if err != nil {
			return err
		}

		query = fmt.Sprintf("SELECT * FROM %s WHERE phone = '%s'", deliveryTable, deliveryPhone)
		err = db.Get(&delivery, query)
		if err != nil {
			return err
		}
		orders[i].Delivery = &delivery

		var payment common.Payment
		query = fmt.Sprintf("SELECT * FROM %s WHERE transaction = '%s'", paymentTable, orders[i].OrderUid)
		err = db.Get(&payment, query)
		if err != nil {
			return err
		}
		orders[i].Payment = &payment

		var items []*common.Item
		var idItems []int
		query = fmt.Sprintf("SELECT item_chrt_id FROM %s WHERE order_uid = '%s'", orderItemTable, orders[i].OrderUid)
		err = db.Select(&idItems, query)
		if err != nil {
			return err
		}
		for _, id := range idItems {
			var item common.Item
			query = fmt.Sprintf("SELECT * FROM %s WHERE chrt_id = '%d'", itemTable, id)
			err = db.Get(&item, query)
			if err != nil {
				return err
			}
			items = append(items, &item)
		}

		orders[i].Items = items

		cache[order.OrderUid] = &orders[i]
	}
	fmt.Println(cache)
	return nil
}

// Преобразует json строку в структуру Order и добавляет ее в БД
func AddOrderToDB(db *sqlx.DB, orderJson string, cache map[string]*common.Order) error {
	var order common.Order

	//проверка на правильность информации, поступающей из nats streaming
	if err := json.Unmarshal([]byte(orderJson), &order); err != nil {
		fmt.Println("Неправильный JSON")
		return err
	}

	//добавление в кэш
	cache[order.OrderUid] = &order

	var tmp string

	//добавление заказчика
	query := fmt.Sprintf("INSERT INTO %s (name, phone, zip, city, address, region, email) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING phone", deliveryTable)
	row := db.QueryRow(query,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	)
	if err := row.Scan(&tmp); err != nil {
		return err
	}

	//добавление заказа
	query = fmt.Sprintf("INSERT INTO %s (order_uid, track_number, entry, delivery, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING order_uid", orderTable)
	row = db.QueryRow(query,
		order.OrderUid,
		order.TrackNumber,
		order.Entry,
		order.Delivery.Phone,
		order.Locale,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.ShardKey,
		order.SmId,
		order.DateCreated,
		order.OofShard,
	)
	if err := row.Scan(&tmp); err != nil {
		return err
	}

	//добавление платежа
	query = fmt.Sprintf("INSERT INTO %s (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING transaction", paymentTable)
	row = db.QueryRow(query,
		order.Payment.Transaction,
		order.Payment.RequestId,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	)
	if err := row.Scan(&tmp); err != nil {
		return err
	}

	//добавление позиций
	for _, item := range order.Items {
		query = fmt.Sprintf("INSERT INTO %s (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING chrt_id", itemTable)
		row = db.QueryRow(query,
			item.ChrtId,
			item.TrackNumber,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.Size,
			item.TotalPrice,
			item.NmId,
			item.Brand,
			item.Status,
		)
		if err := row.Scan(&tmp); err != nil {
			return err
		}

		query = fmt.Sprintf("INSERT INTO %s (order_uid, item_chrt_id) VALUES ($1, $2) RETURNING id", orderItemTable)
		row = db.QueryRow(query,
			order.OrderUid,
			item.ChrtId,
		)
		if err := row.Scan(&tmp); err != nil {
			return err
		}
	}
	fmt.Println("Заказ добавлен в БД")

	return nil

}
