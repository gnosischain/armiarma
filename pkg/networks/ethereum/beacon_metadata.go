package ethereum

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/pkg/errors"

	"github.com/migalabs/armiarma/pkg/networks/ethereum/rpc/methods"
	"github.com/migalabs/armiarma/pkg/networks/ethereum/rpc/reqresp"

	log "github.com/sirupsen/logrus"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/protolambda/zrnt/eth2/beacon/common"
)

// ReqBeaconMetadata opens a new Stream from the given host to send a RPC reqresping the BeaconStatus of the given peer.ID.
// Returns the BeaconStatus of the given peer if succeed, error if failed.
func (en *LocalEthereumNode) ReqBeaconMetadata(
	ctx context.Context,
	wg *sync.WaitGroup,
	h host.Host,
	peerID peer.ID,
	result *common.MetaData,
	finErr *error) {

	defer wg.Done()

	// declare the result output of the RPC call
	var metadata common.MetaData

	attnets := new(common.AttnetBits)
	bytes, err := hex.DecodeString("ffffffffffffffff") //TODO: remove hardcodding!!!!
	if err != nil {
		log.Panic("unable to decode ForkDigest", err.Error())
	}
	attnets.UnmarshalText(bytes)

	// Generate the Server Error Code
	var resCode reqresp.ResponseCode // error by default
	// record the error into the error channel
	err = methods.MetaDataRPCv2NoSnappy.RunRequest(ctx, h.NewStream, peerID, new(reqresp.SnappyCompression),
		reqresp.RequestSSZInput{Obj: nil}, 1,
		func() error {
			return nil
		},
		func(chunk reqresp.ChunkedResponseHandler) error {
			resCode = chunk.ResultCode()
			switch resCode {
			case reqresp.ServerErrCode, reqresp.InvalidReqCode:
				msg, err := chunk.ReadErrMsg()
				if err != nil {
					return errors.Errorf("error reqresping BeaconMetadata RPC: %s", msg)
				}
			case reqresp.SuccessCode:
				if err := chunk.ReadObj(&metadata); err != nil {
					return errors.Wrap(err, "from reqresping BeaconMetadata RPC")
				}
			default:
				return errors.New("unexpected result code for BeaconMetadata RPC reqresp")
			}
			return nil
		})
	*finErr = err
	*result = metadata
}

func (en *LocalEthereumNode) ServeBeaconMetadata(h host.Host) {

	go func() {
		sCtxFn := func() context.Context {
			reqCtx, _ := context.WithTimeout(en.ctx, RPCTimeout)
			return reqCtx
		}
		comp := new(reqresp.SnappyCompression)
		listenReq := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
			var reqMetadata common.MetaData
			err := handler.ReadRequest(&reqMetadata)
			if err != nil {
				_ = handler.WriteErrorChunk(reqresp.InvalidReqCode, "could not parse status request")
				log.Tracef("failed to read metadata request: %v from %s", err, peerId.String())
			} else {
				if err := handler.WriteResponseChunk(reqresp.SuccessCode, &en.LocalMetadata); err != nil {
					log.Tracef("failed to respond to metadata request: %v", err)
				} else {
					log.Tracef("handled metadata request")
				}
			}
		}
		m := methods.MetaDataRPCv2
		streamHandler := m.MakeStreamHandler(sCtxFn, comp, listenReq)
		h.SetStreamHandler(m.Protocol, streamHandler)
		log.Info("Started serving Beacon Metadata")
		// wait untill the ctx is down
		<-en.ctx.Done() // TODO: do it better
		log.Info("Stopped serving Beacon Metadata")
	}()
}
