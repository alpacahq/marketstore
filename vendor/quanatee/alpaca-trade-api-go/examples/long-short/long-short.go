package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/shopspring/decimal"
)

type alpacaClientContainer struct {
	client    *alpaca.Client
	long      bucket
	short     bucket
	allStocks []stockField
	blacklist []string
}
type bucket struct {
	list        []string
	qty         int
	adjustedQty int
	equityAmt   float64
}
type stockField struct {
	name string
	pc   float64
}

var alpacaClient alpacaClientContainer

func init() {
	API_KEY := "YOUR_API_KEY_HERE"
	API_SECRET := "YOUR_API_SECRET_HERE"
	BASE_URL := "https://paper-api.alpaca.markets"

	// Check for environment variables
	if common.Credentials().ID == "" {
		os.Setenv(common.EnvApiKeyID, API_KEY)
	}
	if common.Credentials().Secret == "" {
		os.Setenv(common.EnvApiSecretKey, API_SECRET)
	}
	alpaca.SetBaseUrl(BASE_URL)

	// Format the allStocks variable for use in the class.
	allStocks := []stockField{}
	stockList := []string{"DOMO", "TLRY", "SQ", "MRO", "AAPL", "GM", "SNAP", "SHOP", "SPLK", "BA", "AMZN", "SUI", "SUN", "TSLA", "CGC", "SPWR", "NIO", "CAT", "MSFT", "PANW", "OKTA", "TWTR", "TM", "RTN", "ATVI", "GS", "BAC", "MS", "TWLO", "QCOM"}
	for _, stock := range stockList {
		allStocks = append(allStocks, stockField{stock, 0})
	}

	alpacaClient = alpacaClientContainer{
		alpaca.NewClient(common.Credentials()),
		bucket{[]string{}, -1, -1, 0},
		bucket{[]string{}, -1, -1, 0},
		make([]stockField, len(allStocks)),
		[]string{},
	}

	copy(alpacaClient.allStocks, allStocks)
}

func main() {
	// First, cancel any existing orders so they don't impact our buying power.
	status, until, limit := "open", time.Now(), 100
	orders, _ := alpacaClient.client.ListOrders(&status, &until, &limit)
	for _, order := range orders {
		_ = alpacaClient.client.CancelOrder(order.ID)
	}

	// Wait for market to open.
	fmt.Println("Waiting for market to open...")
	for {
		isOpen := alpacaClient.awaitMarketOpen()
		if isOpen {
			break
		}
		time.Sleep(1 * time.Minute)
	}
	fmt.Println("Market Opened.")

	for {
		alpacaClient.run()
	}
}

// Rebalance the portfolio every minute, making necessary trades.
func (alp alpacaClientContainer) run() {

	// Figure out when the market will close so we can prepare to sell beforehand.
	clock, _ := alpacaClient.client.GetClock()
	if clock.NextClose.Sub(clock.Timestamp) < 15*time.Minute {
		// Close all positions when 15 minutes til market close.
		fmt.Println("Market closing soon.  Closing positions.")

		positions, _ := alpacaClient.client.ListPositions()
		for _, position := range positions {
			var orderSide string
			if position.Side == "long" {
				orderSide = "sell"
			} else {
				orderSide = "buy"
			}
			qty, _ := position.Qty.Float64()
			qty = math.Abs(qty)
			alpacaClient.submitOrder(int(qty), position.Symbol, orderSide)
		}
		// Run script again after market close for next trading day.
		fmt.Println("Sleeping until market close (15 minutes).")
		time.Sleep(15 * time.Minute)
	} else {
		// Rebalance the portfolio.
		alpacaClient.rebalance()
		time.Sleep(1 * time.Minute)
	}
}

// Spin until the market is open.
func (alp alpacaClientContainer) awaitMarketOpen() bool {
	clock, _ := alpacaClient.client.GetClock()
	if clock.IsOpen {
		return true
	}
	timeToOpen := int(clock.NextOpen.Sub(clock.Timestamp).Minutes())
	fmt.Printf("%d minutes until next market open.\n", timeToOpen)
	return false
}

// Rebalance our position after an update.
func (alp alpacaClientContainer) rebalance() {
	alpacaClient.rerank()

	fmt.Printf("We are taking a long position in: %v.\n", alpacaClient.long.list)
	fmt.Printf("We are taking a short position in: %v.\n", alpacaClient.short.list)

	fmt.Print("We are taking a long position in: ")
	fmt.Printf("%v", alpacaClient.long.list)
	fmt.Println()
	fmt.Print("We are taking a short position in: ")
	fmt.Printf("%v", alpacaClient.short.list)
	fmt.Println()

	// Clear existing orders again.
	status, until, limit := "open", time.Now(), 100
	orders, _ := alpacaClient.client.ListOrders(&status, &until, &limit)
	for _, order := range orders {
		_ = alpacaClient.client.CancelOrder(order.ID)
	}

	// Remove positions that are no longer in the short or long list, and make a list of positions that do not need to change.  Adjust position quantities if needed.
	alpacaClient.blacklist = nil
	var executed [2][]string
	positions, _ := alpacaClient.client.ListPositions()
	for _, position := range positions {
		indLong := indexOf(alpacaClient.long.list, position.Symbol)
		indShort := indexOf(alpacaClient.short.list, position.Symbol)

		rawQty, _ := position.Qty.Float64()
		qty := int(math.Abs(rawQty))
		side := "buy"
		if indLong < 0 {
			// Position is not in long list.
			if indShort < 0 {
				// Position not in short list either.  Clear position.
				if position.Side == "long" {
					side = "sell"
				} else {
					side = "buy"
				}
				alpacaClient.submitOrder(int(math.Abs(float64(qty))), position.Symbol, side)
			} else {
				if position.Side == "long" {
					// Position changed from long to short.  Clear long position to prep for short sell.
					side = "sell"
					alpacaClient.submitOrder(qty, position.Symbol, side)
				} else {
					// Position in short list
					if qty == alpacaClient.short.qty {
						// Position is where we want it.  Pass for now
					} else {
						// Need to adjust position amount.
						diff := qty - alpacaClient.short.qty
						if diff > 0 {
							// Too many short positions.  Buy some back to rebalance.
							side = "buy"
						} else {
							// Too little short positions.  Sell some more.
							diff = int(math.Abs(float64(diff)))
							side = "sell"
						}
						qty = diff
						alpacaClient.submitOrder(qty, position.Symbol, side)
					}
					executed[1] = append(executed[1], position.Symbol)
					alpacaClient.blacklist = append(alpacaClient.blacklist, position.Symbol)
				}
			}
		} else {
			// Position in long list.
			if position.Side == "short" {
				// Position changed from short to long.  Clear short position to prep for long purchase.
				side = "buy"
				alpacaClient.submitOrder(qty, position.Symbol, side)
			} else {
				if qty == alpacaClient.long.qty {
					// Position is where we want it.  Pass for now.
				} else {
					// Need to adjust position amount
					diff := qty - alpacaClient.long.qty
					if diff > 0 {
						// Too many long positions.  Sell some to rebalance.
						side = "sell"
					} else {
						diff = int(math.Abs(float64(diff)))
						side = "buy"
					}
					qty = diff
					alpacaClient.submitOrder(qty, position.Symbol, side)
				}
				executed[0] = append(executed[0], position.Symbol)
				alpacaClient.blacklist = append(alpacaClient.blacklist, position.Symbol)
			}
		}
	}

	// Send orders to all remaining stocks in the long and short list.
	longBOResp := alpacaClient.sendBatchOrder(alpacaClient.long.qty, alpacaClient.long.list, "buy")
	executed[0] = append(executed[0], longBOResp[0][:]...)
	if len(longBOResp[1][:]) > 0 {
		// Handle rejected/incomplete orders and determine new quantities to purchase.

		longTPResp := alpacaClient.getTotalPrice(executed[0])
		if longTPResp > 0 {
			alpacaClient.long.adjustedQty = int(alpacaClient.long.equityAmt / longTPResp)
		} else {
			alpacaClient.long.adjustedQty = -1
		}
	} else {
		alpacaClient.long.adjustedQty = -1
	}

	shortBOResp := alpacaClient.sendBatchOrder(alpacaClient.short.qty, alpacaClient.short.list, "sell")
	executed[1] = append(executed[1], shortBOResp[0][:]...)
	if len(shortBOResp[1][:]) > 0 {
		// Handle rejected/incomplete orders and determine new quantities to purchase.
		shortTPResp := alpacaClient.getTotalPrice(executed[1])
		if shortTPResp > 0 {
			alpacaClient.short.adjustedQty = int(alpacaClient.short.equityAmt / shortTPResp)
		} else {
			alpacaClient.short.adjustedQty = -1
		}
	} else {
		alpacaClient.short.adjustedQty = -1
	}

	// Reorder stocks that didn't throw an error so that the equity quota is reached.
	if alpacaClient.long.adjustedQty > -1 {
		alpacaClient.long.qty = alpacaClient.long.adjustedQty - alpacaClient.long.qty
		for _, stock := range executed[0] {
			alpacaClient.submitOrder(alpacaClient.long.qty, stock, "buy")
		}
	}

	if alpacaClient.short.adjustedQty > -1 {
		alpacaClient.short.qty = alpacaClient.short.adjustedQty - alpacaClient.short.qty
		for _, stock := range executed[1] {
			alpacaClient.submitOrder(alpacaClient.short.qty, stock, "sell")
		}
	}
}

// Re-rank all stocks to adjust longs and shorts.
func (alp alpacaClientContainer) rerank() {
	alpacaClient.rank()

	// Grabs the top and bottom quarter of the sorted stock list to get the long and short lists.
	longShortAmount := int(len(alpacaClient.allStocks) / 4)
	alpacaClient.long.list = nil
	alpacaClient.short.list = nil

	for i, stock := range alpacaClient.allStocks {
		if i < longShortAmount {
			alpacaClient.short.list = append(alpacaClient.short.list, stock.name)
		} else if i > (len(alpacaClient.allStocks) - 1 - longShortAmount) {
			alpacaClient.long.list = append(alpacaClient.long.list, stock.name)
		} else {
			continue
		}
	}

	// Determine amount to long/short based on total stock price of each bucket.
	account, _ := alpacaClient.client.GetAccount()
	equity, _ := account.Cash.Float64()
	positions, _ := alpacaClient.client.ListPositions()
	for _, position := range positions {
		rawVal, _ := position.MarketValue.Float64()
		equity += rawVal
	}

	alpacaClient.short.equityAmt = equity * 0.30
	alpacaClient.long.equityAmt = equity + alpacaClient.short.equityAmt

	longTotal := alpacaClient.getTotalPrice(alpacaClient.long.list)
	shortTotal := alpacaClient.getTotalPrice(alpacaClient.short.list)

	alpacaClient.long.qty = int(alpacaClient.long.equityAmt / longTotal)
	alpacaClient.short.qty = int(alpacaClient.short.equityAmt / shortTotal)
}

// Get the total price of the array of input stocks.
func (alp alpacaClientContainer) getTotalPrice(arr []string) float64 {
	totalPrice := 0.0
	for _, stock := range arr {
		numBars := 1
		bar, _ := alpacaClient.client.GetSymbolBars(stock, alpaca.ListBarParams{Timeframe: "minute", Limit: &numBars})
		totalPrice += float64(bar[0].Close)
	}
	return totalPrice
}

// Submit an order if quantity is above 0.
func (alp alpacaClientContainer) submitOrder(qty int, symbol string, side string) error {
	account, _ := alpacaClient.client.GetAccount()
	if qty > 0 {
		adjSide := alpaca.Side(side)
		_, err := alpacaClient.client.PlaceOrder(alpaca.PlaceOrderRequest{
			AccountID:   account.ID,
			AssetKey:    &symbol,
			Qty:         decimal.NewFromFloat(float64(qty)),
			Side:        adjSide,
			Type:        "market",
			TimeInForce: "day",
		})
		if err == nil {
			fmt.Printf("Market order of | %d %s %s | completed.\n", qty, symbol, side)
		} else {
			fmt.Printf("Order of | %d %s %s | did not go through.\n", qty, symbol, side)
		}
		return err
	}
	fmt.Printf("Quantity is <= 0, order of | %d %s %s | not sent.\n", qty, symbol, side)
	return nil
}

// Submit a batch order that returns completed and uncompleted orders.
func (alp alpacaClientContainer) sendBatchOrder(qty int, stocks []string, side string) [2][]string {
	var executed []string
	var incomplete []string
	for _, stock := range stocks {
		index := indexOf(alpacaClient.blacklist, stock)
		if index == -1 {
			resp := alpacaClient.submitOrder(qty, stock, side)
			if resp != nil {
				incomplete = append(incomplete, stock)
			} else {
				executed = append(executed, stock)
			}
		}
	}
	return [2][]string{executed, incomplete}
}

// Get percent changes of the stock prices over the past 10 days.
func (alp alpacaClientContainer) getPercentChanges() {
	length := 10
	for i, stock := range alpacaClient.allStocks {
		startTime, endTime := time.Unix(time.Now().Unix()-int64(length*60), 0), time.Now()
		bars, _ := alpacaClient.client.GetSymbolBars(stock.name, alpaca.ListBarParams{Timeframe: "minute", StartDt: &startTime, EndDt: &endTime})
		percentChange := (bars[len(bars)-1].Close - bars[0].Open) / bars[0].Open
		alpacaClient.allStocks[i].pc = float64(percentChange)
	}
}

// Mechanism used to rank the stocks, the basis of the Long-Short Equity Strategy.
func (alp alpacaClientContainer) rank() {
	// Ranks all stocks by percent change over the past 10 days (higher is better).
	alpacaClient.getPercentChanges()

	// Sort the stocks in place by the percent change field (marked by pc).
	sort.Slice(alpacaClient.allStocks, func(i, j int) bool {
		return alpacaClient.allStocks[i].pc < alpacaClient.allStocks[j].pc
	})
}

// Helper method to imitate the indexOf array method.
func indexOf(arr []string, str string) int {
	for i, elem := range arr {
		if elem == str {
			return i
		}
	}
	return -1
}
