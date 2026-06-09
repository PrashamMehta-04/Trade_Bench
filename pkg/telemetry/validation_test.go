package telemetry

import (
	"testing"
)

func TestOrderBook_ProcessOrder(t *testing.T) {
	ob := NewOrderBook()

	// 1. Add some asks
	ob.ProcessOrder(&OrderEvent{
		OrderID:  "ask1",
		Side:     Sell,
		Price:    100.0,
		Quantity: 10.0,
	})
	ob.ProcessOrder(&OrderEvent{
		OrderID:  "ask2",
		Side:     Sell,
		Price:    101.0,
		Quantity: 10.0,
	})

	// 2. Add a buy order that matches ask1
	fills := ob.ProcessOrder(&OrderEvent{
		OrderID:  "buy1",
		Side:     Buy,
		Price:    100.5,
		Quantity: 5.0,
	})

	if len(fills) != 2 {
		t.Fatalf("Expected 2 fills (buyer and seller), got %d", len(fills))
	}
	// Buyer fill
	if fills[0].OrderID != "buy1" || fills[0].Price != 100.0 || fills[0].Quantity != 5.0 {
		t.Errorf("Unexpected buyer fill: %+v", fills[0])
	}
	// Seller fill
	if fills[1].OrderID != "ask1" || fills[1].Price != 100.0 || fills[1].Quantity != 5.0 {
		t.Errorf("Unexpected seller fill: %+v", fills[1])
	}

	// 3. Add a buy order that matches the rest of ask1 and part of ask2
	fills = ob.ProcessOrder(&OrderEvent{
		OrderID:  "buy2",
		Side:     Buy,
		Price:    102.0,
		Quantity: 10.0,
	})

	if len(fills) != 4 {
		t.Fatalf("Expected 4 fills, got %d", len(fills))
	}
	// First match with ask1 (remaining 5)
	if fills[0].OrderID != "buy2" || fills[0].Price != 100.0 || fills[0].Quantity != 5.0 {
		t.Errorf("Unexpected first buyer fill: %+v", fills[0])
	}
	if fills[1].OrderID != "ask1" || fills[1].Price != 100.0 || fills[1].Quantity != 5.0 {
		t.Errorf("Unexpected first seller fill: %+v", fills[1])
	}
	// Second match with ask2 (5 units)
	if fills[2].OrderID != "buy2" || fills[2].Price != 101.0 || fills[2].Quantity != 5.0 {
		t.Errorf("Unexpected second buyer fill: %+v", fills[2])
	}
	if fills[3].OrderID != "ask2" || fills[3].Price != 101.0 || fills[3].Quantity != 5.0 {
		t.Errorf("Unexpected second seller fill: %+v", fills[3])
	}

	if len(ob.Asks) != 1 || ob.Asks[0].Quantity != 5.0 {
		t.Errorf("Expected 5 remaining in ask2, got %+v", ob.Asks)
	}
}

func TestOrderBook_TimePriority(t *testing.T) {
	ob := NewOrderBook()

	// Add two asks at the same price
	ob.ProcessOrder(&OrderEvent{
		OrderID:  "ask1",
		Side:     Sell,
		Price:    100.0,
		Quantity: 10.0,
	})
	ob.ProcessOrder(&OrderEvent{
		OrderID:  "ask2",
		Side:     Sell,
		Price:    100.0,
		Quantity: 10.0,
	})

	// Buy order should match ask1 first
	fills := ob.ProcessOrder(&OrderEvent{
		OrderID:  "buy1",
		Side:     Buy,
		Price:    100.0,
		Quantity: 5.0,
	})

	if len(fills) != 2 || fills[0].OrderID != "buy1" || fills[0].Price != 100.0 || fills[0].Quantity != 5.0 {
		t.Errorf("Unexpected buyer fill: %+v", fills[0])
	}
	if fills[1].OrderID != "ask1" {
		t.Errorf("Expected match with ask1, but got %s", fills[1].OrderID)
	}
}
