package binance

import (
	"context"
	"encoding/json"
)

// CreateOrderService create order
type CreateOrderService struct {
	c                *Client
	symbol           string
	side             SideType
	orderType        OrderType
	timeInForce      *TimeInForce
	newOrderRespType *NewOrderRespType
	quantity         string
	price            *string
	newClientOrderID *string
	stopPrice        *string
	icebergQuantity  *string
}

// Symbol set symbol
func (s *CreateOrderService) Symbol(symbol string) *CreateOrderService {
	s.symbol = symbol
	return s
}

// Side set side
func (s *CreateOrderService) Side(side SideType) *CreateOrderService {
	s.side = side
	return s
}

// Type set type
func (s *CreateOrderService) Type(orderType OrderType) *CreateOrderService {
	s.orderType = orderType
	return s
}

// TimeInForce set timeInForce
func (s *CreateOrderService) TimeInForce(timeInForce TimeInForce) *CreateOrderService {
	s.timeInForce = &timeInForce
	return s
}

// Quantity set quantity
func (s *CreateOrderService) Quantity(quantity string) *CreateOrderService {
	s.quantity = quantity
	return s
}

// Price set price
func (s *CreateOrderService) Price(price string) *CreateOrderService {
	s.price = &price
	return s
}

// NewClientOrderID set newClientOrderID
func (s *CreateOrderService) NewClientOrderID(newClientOrderID string) *CreateOrderService {
	s.newClientOrderID = &newClientOrderID
	return s
}

// StopPrice set stopPrice
func (s *CreateOrderService) StopPrice(stopPrice string) *CreateOrderService {
	s.stopPrice = &stopPrice
	return s
}

// IcebergQuantity set icebergQuantity
func (s *CreateOrderService) IcebergQuantity(icebergQuantity string) *CreateOrderService {
	s.icebergQuantity = &icebergQuantity
	return s
}

// NewOrderRespType set icebergQuantity
func (s *CreateOrderService) NewOrderRespType(newOrderRespType NewOrderRespType) *CreateOrderService {
	s.newOrderRespType = &newOrderRespType
	return s
}

func (s *CreateOrderService) createOrder(ctx context.Context, endpoint string, opts ...RequestOption) (data []byte, err error) {
	r := &request{
		method:   "POST",
		endpoint: endpoint,
		secType:  secTypeSigned,
	}
	m := params{
		"symbol":   s.symbol,
		"side":     s.side,
		"type":     s.orderType,
		"quantity": s.quantity,
	}
	if s.timeInForce != nil {
		m["timeInForce"] = *s.timeInForce
	}
	if s.price != nil {
		m["price"] = *s.price
	}
	if s.newClientOrderID != nil {
		m["newClientOrderId"] = *s.newClientOrderID
	}
	if s.stopPrice != nil {
		m["stopPrice"] = *s.stopPrice
	}
	if s.icebergQuantity != nil {
		m["icebergQty"] = *s.icebergQuantity
	}
	if s.newOrderRespType != nil {
		m["newOrderRespType"] = *s.newOrderRespType
	}
	r.setFormParams(m)
	data, err = s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []byte{}, err
	}
	return data, nil
}

// Do send request
func (s *CreateOrderService) Do(ctx context.Context, opts ...RequestOption) (res *CreateOrderResponse, err error) {
	data, err := s.createOrder(ctx, "/api/v3/order", opts...)
	if err != nil {
		return nil, err
	}
	res = new(CreateOrderResponse)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Test send test api to check if the request is valid
func (s *CreateOrderService) Test(ctx context.Context, opts ...RequestOption) (err error) {
	_, err = s.createOrder(ctx, "/api/v3/order/test", opts...)
	return
}

// CreateOrderResponse define create order response
type CreateOrderResponse struct {
	Symbol           string  `json:"symbol"`
	OrderID          int64   `json:"orderId"`
	ClientOrderID    string  `json:"clientOrderId"`
	TransactTime     int64   `json:"transactTime"`
	Price            string  `json:"price"`
	OrigQuantity     string  `json:"origQty"`
	ExecutedQuantity string  `json:"executedQty"`
	Status           string  `json:"status"`
	TimeInForce      string  `json:"timeInForce"`
	Type             string  `json:"type"`
	Side             string  `json:"side"`
	Fills            []*Fill `json:"fills"`
}

// Fill may be returned in an array of fills in a CreateOrderResponse.
type Fill struct {
	Price           string `json:"price"`
	Quantity        string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
}

// ListOpenOrdersService list opened orders
type ListOpenOrdersService struct {
	c      *Client
	symbol string
}

// Symbol set symbol
func (s *ListOpenOrdersService) Symbol(symbol string) *ListOpenOrdersService {
	s.symbol = symbol
	return s
}

// Do send request
func (s *ListOpenOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*Order, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v3/openOrders",
		secType:  secTypeSigned,
	}
	if s.symbol != "" {
		r.setParam("symbol", s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*Order{}, err
	}
	res = make([]*Order, 0)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return []*Order{}, err
	}
	return res, nil
}

// GetOrderService get an order
type GetOrderService struct {
	c                 *Client
	symbol            string
	orderID           *int64
	origClientOrderID *string
}

// Symbol set symbol
func (s *GetOrderService) Symbol(symbol string) *GetOrderService {
	s.symbol = symbol
	return s
}

// OrderID set orderID
func (s *GetOrderService) OrderID(orderID int64) *GetOrderService {
	s.orderID = &orderID
	return s
}

// OrigClientOrderID set origClientOrderID
func (s *GetOrderService) OrigClientOrderID(origClientOrderID string) *GetOrderService {
	s.origClientOrderID = &origClientOrderID
	return s
}

// Do send request
func (s *GetOrderService) Do(ctx context.Context, opts ...RequestOption) (res *Order, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v3/order",
		secType:  secTypeSigned,
	}
	r.setParam("symbol", s.symbol)
	if s.orderID != nil {
		r.setParam("orderId", *s.orderID)
	}
	if s.origClientOrderID != nil {
		r.setParam("origClientOrderId", *s.origClientOrderID)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(Order)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Order define order info
type Order struct {
	Symbol           string `json:"symbol"`
	OrderID          int64  `json:"orderId"`
	ClientOrderID    string `json:"clientOrderId"`
	Price            string `json:"price"`
	OrigQuantity     string `json:"origQty"`
	ExecutedQuantity string `json:"executedQty"`
	Status           string `json:"status"`
	TimeInForce      string `json:"timeInForce"`
	Type             string `json:"type"`
	Side             string `json:"side"`
	StopPrice        string `json:"stopPrice"`
	IcebergQuantity  string `json:"icebergQty"`
	Time             int64  `json:"time"`
}

// ListOrdersService list all orders
type ListOrdersService struct {
	c       *Client
	symbol  string
	orderID *int64
	limit   *int
}

// Symbol set symbol
func (s *ListOrdersService) Symbol(symbol string) *ListOrdersService {
	s.symbol = symbol
	return s
}

// OrderID set orderID
func (s *ListOrdersService) OrderID(orderID int64) *ListOrdersService {
	s.orderID = &orderID
	return s
}

// Limit set limit
func (s *ListOrdersService) Limit(limit int) *ListOrdersService {
	s.limit = &limit
	return s
}

// Do send request
func (s *ListOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*Order, err error) {
	r := &request{
		method:   "GET",
		endpoint: "/api/v3/allOrders",
		secType:  secTypeSigned,
	}
	r.setParam("symbol", s.symbol)
	if s.orderID != nil {
		r.setParam("orderId", *s.orderID)
	}
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*Order{}, err
	}
	res = make([]*Order, 0)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return []*Order{}, err
	}
	return res, nil
}

// CancelOrderService cancel an order
type CancelOrderService struct {
	c                 *Client
	symbol            string
	orderID           *int64
	origClientOrderID *string
	newClientOrderID  *string
}

// Symbol set symbol
func (s *CancelOrderService) Symbol(symbol string) *CancelOrderService {
	s.symbol = symbol
	return s
}

// OrderID set orderID
func (s *CancelOrderService) OrderID(orderID int64) *CancelOrderService {
	s.orderID = &orderID
	return s
}

// OrigClientOrderID set origClientOrderID
func (s *CancelOrderService) OrigClientOrderID(origClientOrderID string) *CancelOrderService {
	s.origClientOrderID = &origClientOrderID
	return s
}

// NewClientOrderID set newClientOrderID
func (s *CancelOrderService) NewClientOrderID(newClientOrderID string) *CancelOrderService {
	s.newClientOrderID = &newClientOrderID
	return s
}

// Do send request
func (s *CancelOrderService) Do(ctx context.Context, opts ...RequestOption) (res *CancelOrderResponse, err error) {
	r := &request{
		method:   "DELETE",
		endpoint: "/api/v3/order",
		secType:  secTypeSigned,
	}
	r.setFormParam("symbol", s.symbol)
	if s.orderID != nil {
		r.setFormParam("orderId", *s.orderID)
	}
	if s.origClientOrderID != nil {
		r.setFormParam("origClientOrderId", *s.origClientOrderID)
	}
	if s.newClientOrderID != nil {
		r.setFormParam("newClientOrderId", *s.newClientOrderID)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CancelOrderResponse)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// CancelOrderResponse define response of canceling order
type CancelOrderResponse struct {
	Symbol            string `json:"symbol"`
	OrigClientOrderID string `json:"origClientOrderId"`
	OrderID           int64  `json:"orderId"`
	ClientOrderID     string `json:"clientOrderId"`
}
