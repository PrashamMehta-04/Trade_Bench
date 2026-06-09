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

// FillEvent represents a specific fill for an order.
type FillEvent struct {
	OrderID string
	Price   float64
	Quantity     float64
}

// ProcessOrder adds an order and returns all fills generated (for both the incoming and matched orders).
func (ob *OrderBook) ProcessOrder(order *OrderEvent) (fills []FillEvent) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if order.Side == Buy {
		// Try to match with Asks
		for i := 0; i < len(ob.Asks) && order.Quantity > 0; {
			ask := ob.Asks[i]
			if order.Price >= ask.Price {
				qty := min(order.Quantity, ask.Quantity)
				// Fill for the buyer (incoming)
				fills = append(fills, FillEvent{
					OrderID:  order.OrderID,
					Price:    ask.Price,
					Quantity: qty,
				})
				// Fill for the seller (existing)
				fills = append(fills, FillEvent{
					OrderID:  ask.OrderID,
					Price:    ask.Price,
					Quantity: qty,
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
				// Fill for the seller (incoming)
				fills = append(fills, FillEvent{
					OrderID:  order.OrderID,
					Price:    bid.Price,
					Quantity: qty,
				})
				// Fill for the buyer (existing)
				fills = append(fills, FillEvent{
					OrderID:  bid.OrderID,
					Price:    bid.Price,
					Quantity: qty,
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
