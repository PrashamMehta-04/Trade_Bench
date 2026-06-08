package telemetry

import (
	"sort"
	"sync"
)

// OrderBook represents a simplified price-time priority orderbook for validation.
type OrderBook struct {
	mu   sync.Mutex
	Bids []*OrderEvent
	Asks []*OrderEvent
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids: make([]*OrderEvent, 0),
		Asks: make([]*OrderEvent, 0),
	}
}

// ProcessOrder adds an order and returns potential fills based on price-time priority.
func (ob *OrderBook) ProcessOrder(order *OrderEvent) (fills []OrderEvent) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if order.Side == Buy {
		// Try to match with Asks
		for i := 0; i < len(ob.Asks) && order.Quantity > 0; {
			ask := ob.Asks[i]
			if order.Price >= ask.Price {
				qty := min(order.Quantity, ask.Quantity)
				fills = append(fills, OrderEvent{
					OrderID:   order.OrderID,
					Side:      Buy,
					FillPrice: ask.Price,
					FillQty:   qty,
				})
				order.Quantity -= qty
				ask.Quantity -= qty
				if ask.Quantity == 0 {
					ob.Asks = append(ob.Asks[:i], ob.Asks[i+1:]...)
					continue
				}
			}
			i++
		}
		if order.Quantity > 0 {
			ob.Bids = append(ob.Bids, order)
			sort.Slice(ob.Bids, func(i, j int) bool {
				if ob.Bids[i].Price == ob.Bids[j].Price {
					return false // Keep insertion order for time priority
				}
				return ob.Bids[i].Price > ob.Bids[j].Price
			})
		}
	} else {
		// Try to match with Bids
		for i := 0; i < len(ob.Bids) && order.Quantity > 0; {
			bid := ob.Bids[i]
			if order.Price <= bid.Price {
				qty := min(order.Quantity, bid.Quantity)
				fills = append(fills, OrderEvent{
					OrderID:   order.OrderID,
					Side:      Sell,
					FillPrice: bid.Price,
					FillQty:   qty,
				})
				order.Quantity -= qty
				bid.Quantity -= qty
				if bid.Quantity == 0 {
					ob.Bids = append(ob.Bids[:i], ob.Bids[i+1:]...)
					continue
				}
			}
			i++
		}
		if order.Quantity > 0 {
			ob.Asks = append(ob.Asks, order)
			sort.Slice(ob.Asks, func(i, j int) bool {
				if ob.Asks[i].Price == ob.Asks[j].Price {
					return false
				}
				return ob.Asks[i].Price < ob.Asks[j].Price
			})
		}
	}
	return fills
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
