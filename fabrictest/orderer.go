/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fabrictest

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

// Config controls the network characteristics and batching behavior of the test orderer.
type Config struct {
	// BlockCutTime is the maximum time to wait before cutting a block (0 = cut immediately).
	BlockCutTime time.Duration
	// MaxTxPerBlock is the maximum number of transactions per block (0 = no limit).
	MaxTxPerBlock int
	// OrdererPort is the port of the orderer (0 = random)
	OrdererPort int
	// PeerPort is the port of the peer (0 = random)
	PeerPort int
}

func newTestOrderer(l *ledger, cfg Config) *testOrderer {
	o := &testOrderer{
		ledger:   l,
		config:   cfg,
		inCh:     make(chan *common.Envelope, 64),
		cutCh:    make(chan chan error),
		revertCh: make(chan revertReq),
		stopCh:   make(chan struct{}),
	}

	go o.batchingLoop()

	return o
}

// testOrderer receives transaction envelopes and cuts blocks.
// Contrary to a normal orderer, it does MVCC validation in place.
type testOrderer struct {
	ledger   *ledger
	inCh     chan *common.Envelope
	cutCh    chan chan error
	revertCh chan revertReq
	config   Config
	stopCh   chan struct{}
}

// revertReq asks batchingLoop to truncate the chain to block num.
type revertReq struct {
	num   uint64
	reply chan error
}

// stop signals batchingLoop to exit.
func (o *testOrderer) stop() {
	close(o.stopCh)
}

// Broadcast is the GRPC endpoint that lets users submit transactions.
func (o *testOrderer) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		o.inCh <- env

		if err := stream.Send(&orderer.BroadcastResponse{
			Status: common.Status_SUCCESS,
		}); err != nil {
			return err
		}
	}
}

// cutBlock forces a block with no transactions to be cut, advancing block
// height by one. The request is funneled through batchingLoop's single
// goroutine so it stays serialized with regular transaction processing.
func (o *testOrderer) cutBlock(ctx context.Context) error {
	reply := make(chan error, 1)

	select {
	case o.cutCh <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// revert truncates the chain so num is the last block; the next block cut takes
// number num+1. Like cutBlock, it is serialized with transaction processing.
// Transactions still waiting in the current batch are dropped.
func (o *testOrderer) revert(ctx context.Context, num uint64) error {
	req := revertReq{num: num, reply: make(chan error, 1)}

	select {
	case o.revertCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *testOrderer) batchingLoop() {
	var (
		batch []*common.Envelope
		timer *time.Timer
	)

	// flush cuts a block
	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := o.ledger.process(context.Background(), batch); err != nil {
			panic(err)
		}

		batch = nil
		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}

	for {
		cfg := o.config

		var timerC <-chan time.Time
		if timer != nil {
			timerC = timer.C
		}

		select {
		case env := <-o.inCh:
			batch = append(batch, env)

			// Start timer on first tx in batch (Fabric semantics: timer and max size
			// both trigger a cut, whichever comes first).
			if len(batch) == 1 && cfg.BlockCutTime > 0 {
				timer = time.NewTimer(cfg.BlockCutTime)
			}

			// Cut immediately if no timer configured, or if max tx count is reached.
			if cfg.BlockCutTime == 0 || (cfg.MaxTxPerBlock > 0 && len(batch) >= cfg.MaxTxPerBlock) {
				flush()
				continue
			}

		case <-timerC:
			flush()

		case reply := <-o.cutCh:
			reply <- o.ledger.process(context.Background(), nil)

		case req := <-o.revertCh:
			batch = nil
			if timer != nil {
				timer.Stop()
				timer = nil
			}
			req.reply <- o.ledger.truncate(req.num)

		case <-o.stopCh:
			return
		}
	}
}

// Deliver delivers blocks to peers. We skip that part; we let the orderer do the
// validations and store the blocks on the ledger directly.
func (o *testOrderer) Deliver(orderer.AtomicBroadcast_DeliverServer) error {
	return errors.New("not implemented")
}
