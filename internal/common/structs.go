package common

type Order struct {
	OrderUid          string    `json:"order_uid" db:"order_uid"`
	TrackNumber       string    `json:"track_number" db:"track_number"`
	Entry             string    `json:"entry" db:"entry"`
	Locale            string    `json:"locale" db:"locale"`
	InternalSignature string    `json:"internal_signature" db:"internal_signature"`
	CustomerId        string    `json:"customer_id" db:"customer_id"`
	DeliveryService   string    `json:"delivery_service" db:"delivery_service"`
	ShardKey          string    `json:"shardkey" db:"shardkey"`
	SmId              int       `json:"sm_id" db:"sm_id"`
	DateCreated       string    `json:"date_created" db:"date_created"`
	OofShard          string    `json:"oof_shard" db:"oof_shard"`
	Payment           *Payment  `json:"payment" db:"payment"`
	Items             []*Item   `json:"items" db:"items"`
	Delivery          *Delivery `json:"delivery" db:"delivery"`
}

type Payment struct {
	Transaction  string `json:"transaction" db:"transaction"`
	RequestId    string `json:"request_id" db:"request_id"`
	Currency     string `json:"currency" db:"currency"`
	Provider     string `json:"provider" db:"provider"`
	Amount       int    `json:"amount" db:"amount"`
	PaymentDt    int    `json:"payment_dt" db:"payment_dt"`
	Bank         string `json:"bank" db:"bank"`
	DeliveryCost int    `json:"delivery_cost" db:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total" db:"goods_total"`
	CustomFee    int    `json:"custom_fee" db:"custom_fee"`
}

type Item struct {
	ChrtId      int     `json:"chrt_id" db:"chrt_id"`
	TrackNumber string  `json:"track_number" db:"track_number"`
	Price       float32 `json:"price" db:"price"`
	Rid         string  `json:"rid" db:"rid"`
	Name        string  `json:"name" db:"name"`
	Sale        int     `json:"sale" db:"sale"`
	Size        string  `json:"size" db:"size"`
	TotalPrice  float32 `json:"total_price" db:"total_price"`
	NmId        int     `json:"nm_id" db:"nm_id"`
	Brand       string  `json:"brand" db:"brand"`
	Status      int     `json:"status" db:"status"`
}

type Delivery struct {
	Name    string `json:"name" db:"name"`
	Phone   string `json:"phone" db:"phone"`
	Zip     string `json:"zip" db:"zip"`
	City    string `json:"city" db:"city"`
	Address string `json:"address" db:"address"`
	Region  string `json:"region" db:"region"`
	Email   string `json:"email" db:"email"`
}
