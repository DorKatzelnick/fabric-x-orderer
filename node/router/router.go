/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	rand3 "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	rand2 "math/rand"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
)

type Net interface {
	Stop()
	Address() string
}

type Router struct {
	mapper           ShardMapper
	net              Net
	shardRouters     map[types.ShardID]*ShardRouter
	logger           *flogging.FabricLogger
	shardIDs         []types.ShardID
	routerNodeConfig *nodeconfig.RouterNodeConfig
	configuration    *config.Configuration
	verifier         *requestfilter.RulesVerifier
	configStore      *configstore.Store
	configSubmitter  ConfigurationSubmitter
	decisionPuller   DecisionPuller
	signer           identity.SignerSerializer
	metrics          *RouterMetrics
	stopChan         chan struct{}
	stopOnce         sync.Once
	armaStopChan     chan struct{}
	drainChan        chan struct{}
	drainOnce        sync.Once
	feedbackWG       sync.WaitGroup
	configSeq        uint32
	wal              *wal.WriteAheadLogFile
}

func NewRouter(config *nodeconfig.RouterNodeConfig, configuration *config.Configuration, logger *flogging.FabricLogger, signer identity.SignerSerializer, armaStopChan chan struct{}, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules) *Router {
	logger.Infof("Creating new router with PartyID: %d", config.PartyID)

	r := &Router{
		logger:       logger,
		signer:       signer,
		armaStopChan: armaStopChan,
	}

	r.initFromConfig(config, configuration, configUpdateProposer, configRulesVerifier)

	r.init()
	r.metrics.Start()

	return r
}

func (r *Router) initFromConfig(rconfig *nodeconfig.RouterNodeConfig, configuration *config.Configuration, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules) {
	if rconfig.NumOfConnectionsForBatcher == 0 {
		rconfig.NumOfConnectionsForBatcher = config.DefaultRouterParams.NumberOfConnectionsPerBatcher
	}

	if rconfig.NumOfgRPCStreamsPerConnection == 0 {
		rconfig.NumOfgRPCStreamsPerConnection = config.DefaultRouterParams.NumberOfStreamsPerConnection
	}

	r.configuration = configuration
	r.routerNodeConfig = rconfig
	// shardIDs is an array of all shard ids
	var shardIDs []types.ShardID
	// batcherEndpoints are the endpoints of all batchers from the router's party by shard id
	batcherEndpoints := make(map[types.ShardID]string)
	tlsCAsOfBatchers := make(map[types.ShardID][][]byte)
	for _, shard := range rconfig.Shards {
		shardIDs = append(shardIDs, shard.ShardId)
		for _, batcher := range shard.Batchers {
			if rconfig.PartyID != batcher.PartyID {
				continue
			}
			batcherEndpoints[shard.ShardId] = batcher.Endpoint
			var tlsCAsOfBatcher [][]byte
			for _, rawTLSCA := range batcher.TLSCACerts {
				tlsCAsOfBatcher = append(tlsCAsOfBatcher, rawTLSCA)
			}

			tlsCAsOfBatchers[shard.ShardId] = tlsCAsOfBatcher
		}
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return int(shardIDs[i]) < int(shardIDs[j])
	})
	r.shardIDs = shardIDs

	r.mapper = CreateMapperCRC64(r.logger, uint16(len(shardIDs)))

	r.verifier = createVerifier(rconfig)

	r.configSubmitter = NewConfigSubmitter(rconfig, r.logger, r.verifier, r.signer, configUpdateProposer, configRulesVerifier)

	r.shardRouters = make(map[types.ShardID]*ShardRouter)
	for _, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(r.logger, batcherEndpoints[shardId], tlsCAsOfBatchers[shardId], r.routerNodeConfig.TLSCertificateFile, r.routerNodeConfig.TLSPrivateKeyFile, r.routerNodeConfig.NumOfConnectionsForBatcher, r.routerNodeConfig.NumOfgRPCStreamsPerConnection, r.verifier, r.configSubmitter)
	}

	configStore, err := configstore.NewStore(rconfig.FileStorePath)
	if err != nil {
		r.logger.Panicf("Failed creating router config store: %s", err)
	}
	r.configStore = configStore

	walDir := filepath.Join(rconfig.FileStorePath, "wal")
	routerWAL, walInitState, err := wal.InitializeAndReadAll(r.logger, walDir, wal.DefaultOptions())
	if err != nil {
		r.logger.Panicf("Failed initializing router WAL: %s", err)
	}
	r.wal = routerWAL

	seekInfo := delivery.NextSeekInfo(uint64(getNextDecisionNumber(r.configStore, walInitState, r.logger)))

	// TODO - pull decisions from all consenter nodes, not only the one in party
	r.decisionPuller = CreateConsensusDecisionReplicator(rconfig, seekInfo, r.logger)

	r.metrics = NewRouterMetrics(rconfig, r.logger)
	r.configSeq = uint32(r.routerNodeConfig.Bundle.ConfigtxValidator().Sequence())

	// initialize channels and once
	r.stopChan = make(chan struct{})
	r.drainChan = make(chan struct{})
	r.stopOnce = sync.Once{}
	r.drainOnce = sync.Once{}
}

func NewRouterOld(config *nodeconfig.RouterNodeConfig, configuration *config.Configuration, logger *flogging.FabricLogger, signer identity.SignerSerializer, armaStopChan chan struct{}, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules) *Router {
	logger.Infof("Creating new router with PartyID: %d", config.PartyID)

	// shardIDs is an array of all shard ids
	var shardIDs []types.ShardID
	// batcherEndpoints are the endpoints of all batchers from the router's party by shard id
	batcherEndpoints := make(map[types.ShardID]string)
	tlsCAsOfBatchers := make(map[types.ShardID][][]byte)
	for _, shard := range config.Shards {
		shardIDs = append(shardIDs, shard.ShardId)
		for _, batcher := range shard.Batchers {
			if config.PartyID != batcher.PartyID {
				continue
			}
			batcherEndpoints[shard.ShardId] = batcher.Endpoint
			var tlsCAsOfBatcher [][]byte
			for _, rawTLSCA := range batcher.TLSCACerts {
				tlsCAsOfBatcher = append(tlsCAsOfBatcher, rawTLSCA)
			}

			tlsCAsOfBatchers[shard.ShardId] = tlsCAsOfBatcher
		}
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return int(shardIDs[i]) < int(shardIDs[j])
	})

	configStore, err := configstore.NewStore(config.FileStorePath)
	if err != nil {
		logger.Panicf("Failed creating router config store: %s", err)
	}

	walDir := filepath.Join(config.FileStorePath, "wal")
	routerWAL, walInitState, err := wal.InitializeAndReadAll(logger, walDir, wal.DefaultOptions())
	if err != nil {
		logger.Panicf("Failed initializing router WAL: %s", err)
	}

	seekInfo := delivery.NextSeekInfo(uint64(getNextDecisionNumber(configStore, walInitState, logger)))

	// TODO - pull decisions from all consenter nodes, not only the one in party
	decisionPuller := CreateConsensusDecisionReplicator(config, seekInfo, logger)

	verifier := createVerifier(config)

	configSubmitter := NewConfigSubmitter(config, logger, verifier, signer, configUpdateProposer, configRulesVerifier)

	metrics := NewRouterMetrics(config, logger)

	r := createRouter(shardIDs, batcherEndpoints, tlsCAsOfBatchers, metrics, config, configuration, logger, armaStopChan, verifier, signer, configStore, configSubmitter, decisionPuller, routerWAL)
	r.init()
	r.metrics.Start()
	return r
}

// getNextDecisionNumber return the number of the next decision to be pulled from consensus, based on the last config block stored in config store and the decision stored in WAL.
func getNextDecisionNumber(configStore *configstore.Store, walInitState [][]byte, logger *flogging.FabricLogger) types.DecisionNum {
	if len(walInitState) > 0 {
		lastWalEntry := walInitState[len(walInitState)-1]
		decision := &state.Header{}
		err := decision.Deserialize(lastWalEntry)
		if err != nil {
			logger.Panicf("Failed deserializing last decision header from router WAL: %s", err)
		}
		logger.Infof("Last decision number in router's WAL is %d", decision.Num)
		// we pull the same decision again, in case the router failed before storing the config block in that decision
		logger.Infof("Router will start pulling consensus decisions from decision number %d", decision.Num)
		return decision.Num
	}

	logger.Infof("No entries in router's WAL")

	// get last config block from config store
	lastBlock, err := configStore.Last()
	if err != nil {
		logger.Panicf("Failed getting last config block from config store: %s", err)
	}

	if lastBlock.Header.Number == 0 {
		logger.Infof("Last config block is genesis block. Router will start pulling consensus decisions from decision number 1")
		return 1
	}

	// last config block is not genesis block, extract decision number from its metadata
	ordererBlockMetadata := lastBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	_, _, _, lastConfigBlockDecisionNumber, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(ordererBlockMetadata)
	if err != nil {
		logger.Panicf("Failed extracting decision number from last config block: %s", err)
	}

	logger.Infof("Last config block decision number in router's config store is %d. Router will start pulling consensus decisions from decision number %d", lastConfigBlockDecisionNumber, lastConfigBlockDecisionNumber+1)
	return lastConfigBlockDecisionNumber + 1
}

func (r *Router) StartRouterService() {
	srv := node_utils.CreateGRPCRouter(r.routerNodeConfig)
	r.net = srv

	protos.RegisterRequestTransmitServer(srv.Server(), r)
	orderer.RegisterAtomicBroadcastServer(srv.Server(), r)

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
		r.logger.Infof("Router network service was stopped")
	}()

	r.configSubmitter.Start()

	node_utils.StopSignalListen(r.stopChan, r, r.logger, r.Address())

	go r.pullAndProcessDecisions()
}

func (r *Router) MonitoringServiceAddress() string {
	return r.metrics.monitor.Address()
}

func (r *Router) Address() string {
	if r.net == nil {
		return ""
	}

	return r.net.Address()
}

func (r *Router) Stop() {
	r.logger.Infof("Stopping router listening on %s, PartyID: %d", r.net.Address(), r.routerNodeConfig.PartyID)

	r.net.Stop()
	r.metrics.Stop()

	// stop config submitter goroutine
	r.configSubmitter.Stop()

	// stop decision puller goroutine
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})

	r.wal.Close()

	for _, sr := range r.shardRouters {
		sr.Stop()
	}

	close(r.armaStopChan)
}

func (r *Router) SoftStop() error {
	routerAddress := r.net.Address()
	partyID := r.routerNodeConfig.PartyID

	r.logger.Infof("Initiating soft stop of router listening on %s, PartyID: %d", routerAddress, partyID)

	// stop accepting new requests in broadcast and submit handlers
	// closing the stop chan will also stop the decision puller, if needed.
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
	r.wal.Close()

	// next, we stop the shard routers, which will be responsible for sending responses to pending requests
	for _, sr := range r.shardRouters {
		sr.SoftStop(fmt.Errorf("router is stopping, cannot process request"))
	}

	// wait until all feedback channels are drained and all responses are sent
	r.drainOnce.Do(func() {
		close(r.drainChan)
	})
	r.feedbackWG.Wait()

	// then, we stop other components
	r.configSubmitter.Stop()

	r.metrics.Stop()

	r.net.Stop() // this will close all client connections, so some (immediate) responses may not be sent.

	r.logger.Warnf("Router on %s, PartyID: %d, has been stopped.", routerAddress, partyID)

	return nil
}

func extractNewSharedConfig(configBlock *common.Block) (*config_protos.SharedConfig, error) {
	sharedConfig := &config_protos.SharedConfig{}
	// read shared config from block
	consensusMetadata, err := config.ReadConsensusMetadataFromConfigBlock(configBlock)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read consensus metadata from config block")
	}
	err = proto.Unmarshal(consensusMetadata, sharedConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}
	return sharedConfig, nil
}

func (r *Router) extractNewConfig(configBlock *common.Block) (*nodeconfig.RouterNodeConfig, *config.Configuration, error) {
	r.logger.Infof("Extracting new config from config block with number %d", configBlock.Header.Number)
	if r.configuration == nil {
		return nil, nil, errors.New("current configuration is nil")
	}
	newConfiguaration := &config.Configuration{
		LocalConfig:  r.configuration.LocalConfig, // previous local config is kept.
		SharedConfig: &config_protos.SharedConfig{},
	}

	// read shared config from block
	consensusMetadata, err := config.ReadConsensusMetadataFromConfigBlock(configBlock)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read consensus metadata from config block")
	}
	err = proto.Unmarshal(consensusMetadata, newConfiguaration.SharedConfig)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}

	// extract new router node config.
	return newConfiguaration.ExtractRouterConfig(configBlock), newConfiguaration, nil
}

func (r *Router) ApplyConfig(configBlock *common.Block) error {
	// extract new router node config from the last config block and conifguration
	newSharedConfig, err := extractNewSharedConfig(configBlock)
	if err != nil {
		r.logger.Errorf("Failed to extract new shared config from last config block: %v. Admin's action required. ", err)
		return errors.Wrapf(err, "failed to extract new shared config from last config block")
	}

	newConfiguration := &config.Configuration{
		LocalConfig:  r.configuration.LocalConfig, // previous local config is kept.
		SharedConfig: newSharedConfig,
	}

	// fisrt, check party is evicted in the new configuration.
	evicted, err := config.IsPartyEvicted(r.routerNodeConfig.PartyID, newConfiguration)
	if err != nil {
		return errors.Wrapf(err, "failed to check if router's party was evicted in the new configuration")
	} else if evicted {
		r.logger.Warnf("Router's party %d was evicted in the new configuration", r.routerNodeConfig.PartyID)
		return fmt.Errorf("router's party %d was evicted in the new configuration", r.routerNodeConfig.PartyID)
	}

	// extract the new router node config.
	newRouterNodeConfig, _, err := r.extractNewConfig(configBlock)
	if err != nil {
		r.logger.Warnf("Failed to extract new config from last config block: %v. Admin's action required. ", err)
		return errors.Wrapf(err, "failed to extract new router node config from config block")
	}

	configSeq := newRouterNodeConfig.Bundle.ConfigtxValidator().Sequence()
	r.logger.Infof("New config was extracted from last config block, new config sequence: %d, current config sequence: %d", configSeq, r.configSeq)

	// check if there is a change that requires admin restart.
	currPartyConfig, _ := config.FindParty(r.routerNodeConfig.PartyID, r.configuration)
	newPartyConfig, _ := config.FindParty(r.routerNodeConfig.PartyID, newConfiguration)
	requireRestart, err := config.IsNodeConfigChangeRestartRequired(currPartyConfig.RouterConfig, newPartyConfig.RouterConfig, r.logger)
	if err != nil {
		return errors.Wrapf(err, "failed to check if node config change requires restart")
	} else if requireRestart {
		r.logger.Warnf("Router's node config change requires restart, will not dynamically restart")
		return fmt.Errorf("router's node config change requires restart, will not dynamically restart")
	}

	r.logger.Infof("Applying new config with sequence %d, router will be restarted dynamically", configSeq)

	newRouter := NewRouter(newRouterNodeConfig, newConfiguration, r.logger, r.signer, r.armaStopChan, &policy.DefaultConfigUpdateProposer{}, &verify.DefaultOrdererRules{})
	newRouter.StartRouterService()
	newRouter.logger.Infof("Router started with new config sequence %d, listening on %s, PartyID: %d", configSeq, newRouter.Address(), newRouter.routerNodeConfig.PartyID)
	return nil
}

func (r *Router) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	clientAddr, err := utils.ExtractClientAddressFromContext(stream.Context())
	if err == nil {
		r.logger.Infof("Client connected: %s", clientAddr)
	}
	if clientCert := utils.ExtractCertificateFromContext(stream.Context()); clientCert != nil {
		r.logger.Infof("Client's certificate: \n%s", utils.CertificateToString(clientCert))
	}

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan Response, 1000)
	go r.sendFeedbackOnBroadcastStream(stream, exit, feedbackChan)

	for {
		reqEnv, err := stream.Recv()
		if err == io.EOF {
			r.logger.Infof("Received EOF from stream, closing broadcast from client %s", clientAddr)
			return nil
		}
		if err != nil {
			r.logger.Infof("Received error from stream: %v, closing broadcastfrom client %s", err, clientAddr)
			return err
		}

		r.metrics.incomingTxs.Add(1)

		request := &protos.Request{Payload: reqEnv.Payload, Signature: reqEnv.Signature, ConfigSeq: r.configSeq}
		reqID, shardRouter := r.getShardRouterAndReqID(request)

		select {
		case <-r.stopChan:
			r.sendBroadcastResponse(stream, Response{
				err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
				reqID: reqID,
			})
		default:
			// create a routing request with nil trace. the request is not traced in router.
			tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID}
			shardRouter.Forward(tr)
		}
	}
}

func (r *Router) init() {
	for _, shardId := range r.shardIDs {
		r.shardRouters[shardId].InitShardRouter()
	}
}

func (r *Router) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return fmt.Errorf("not implemented")
}

func createRouter(shardIDs []types.ShardID, batcherEndpoints map[types.ShardID]string, batcherRootCAs map[types.ShardID][][]byte, metrics *RouterMetrics, rconfig *nodeconfig.RouterNodeConfig, configuration *config.Configuration, logger *flogging.FabricLogger, armaStopChan chan struct{}, verifier *requestfilter.RulesVerifier, signer identity.SignerSerializer, configStore *configstore.Store, configSubmitter ConfigurationSubmitter, decisionPuller DecisionPuller, routerWAL *wal.WriteAheadLogFile) *Router {
	if rconfig.NumOfConnectionsForBatcher == 0 {
		rconfig.NumOfConnectionsForBatcher = config.DefaultRouterParams.NumberOfConnectionsPerBatcher
	}

	if rconfig.NumOfgRPCStreamsPerConnection == 0 {
		rconfig.NumOfgRPCStreamsPerConnection = config.DefaultRouterParams.NumberOfStreamsPerConnection
	}

	r := &Router{
		mapper:           CreateMapperCRC64(logger, uint16(len(shardIDs))),
		shardRouters:     make(map[types.ShardID]*ShardRouter),
		logger:           logger,
		shardIDs:         shardIDs,
		routerNodeConfig: rconfig,
		configuration:    configuration,
		verifier:         verifier,
		signer:           signer,
		configStore:      configStore,
		configSubmitter:  configSubmitter,
		decisionPuller:   decisionPuller,
		stopChan:         make(chan struct{}),
		drainChan:        make(chan struct{}),
		armaStopChan:     armaStopChan,
		metrics:          metrics,
		configSeq:        uint32(rconfig.Bundle.ConfigtxValidator().Sequence()),
		wal:              routerWAL,
	}

	for _, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(logger, batcherEndpoints[shardId], batcherRootCAs[shardId], rconfig.TLSCertificateFile, rconfig.TLSPrivateKeyFile, rconfig.NumOfConnectionsForBatcher, rconfig.NumOfgRPCStreamsPerConnection, verifier, configSubmitter)
	}

	return r
}

func (r *Router) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	rand := r.initRand()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan Response, 100)
	go r.sendFeedbackOnSubmitStream(stream, exit, feedbackChan)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		r.metrics.incomingTxs.Add(1)

		reqID, shardRouter := r.getShardRouterAndReqID(req)

		select {
		case <-r.stopChan:
			r.sendSubmitResponse(stream, Response{
				err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
				reqID: reqID,
			})
		default:
			trace := createTraceID(rand)
			tr := &TrackedRequest{request: req, responses: feedbackChan, reqID: reqID, trace: trace}
			tr.request.ConfigSeq = r.configSeq
			shardRouter.Forward(tr)
		}
	}
}

func (r *Router) initRand() *rand2.Rand {
	seed := make([]byte, 8)
	if _, err := rand3.Read(seed); err != nil {
		panic(err)
	}

	src := rand2.NewSource(int64(binary.BigEndian.Uint64(seed)))
	rand := rand2.New(src)
	return rand
}

func (r *Router) getShardRouterAndReqID(req *protos.Request) ([]byte, *ShardRouter) {
	shardIndex, reqID := r.mapper.Map(req.Payload)
	shardId := r.shardIDs[shardIndex]
	r.logger.Debugf("request %x is mapped to shard %d", req.Payload, shardId)
	shardRouter, exists := r.shardRouters[shardId]
	if !exists {
		r.logger.Panicf("Mapped request %d to a non existent shard", shardId)
	}
	return reqID, shardRouter
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	r.metrics.incomingTxs.Add(1)

	reqID, shardRouter := r.getShardRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan Response, 1)

	tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID, trace: trace}
	tr.request.ConfigSeq = r.configSeq
	shardRouter.Forward(tr)

	r.logger.Debugf("Forwarded request %x", request.Payload)

	var response Response
	select {
	case res := <-feedbackChan:
		response = res
	case <-r.stopChan:
		response = Response{
			err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
			reqID: reqID,
		}
	case <-ctx.Done():
		response = Response{
			err:   fmt.Errorf("context done before receiving response for request %x: %v", reqID, ctx.Err()),
			reqID: reqID,
		}
	}

	r.metrics.increaseErrorCount(response.err)
	return responseToSubmitResponse(&response), nil
}

func (r *Router) sendFeedbackOnSubmitStream(stream protos.RequestTransmit_SubmitStreamServer, exit chan struct{}, feedbackChan chan Response) {
	r.feedbackWG.Add(1)
	defer r.feedbackWG.Done()
	for {
		select {
		case <-exit:
			return
		case response := <-feedbackChan:
			r.metrics.increaseErrorCount(response.err)
			resp := responseToSubmitResponse(&response)
			err := stream.Send(resp)
			if err != nil {
				r.logger.Errorf("error sending response to client: %v", err)
			}
		case <-r.drainChan:
			if len(feedbackChan) == 0 {
				return
			}
		}
	}
}

func (r *Router) sendSubmitResponse(stream protos.RequestTransmit_SubmitStreamServer, response Response) {
	err := stream.Send(responseToSubmitResponse(&response))
	if err != nil {
		r.logger.Errorf("error sending response to client: %v", err)
	}
	r.metrics.increaseErrorCount(response.err)
}

func (r *Router) sendFeedbackOnBroadcastStream(stream orderer.AtomicBroadcast_BroadcastServer, exit chan struct{}, feedbackChan chan Response) {
	r.feedbackWG.Add(1)
	defer r.feedbackWG.Done()
	for {
		select {
		case <-exit:
			return
		case response := <-feedbackChan:
			err := stream.Send(responseToBroadcastResponse(&response))
			if err != nil {
				r.logger.Errorf("error sending response to client: %v", err)
			}
			r.metrics.increaseErrorCount(response.err)
		case <-r.drainChan:
			if len(feedbackChan) == 0 {
				return
			}
		}
	}
}

func (r *Router) sendBroadcastResponse(stream orderer.AtomicBroadcast_BroadcastServer, response Response) {
	err := stream.Send(responseToBroadcastResponse(&response))
	if err != nil {
		r.logger.Errorf("error sending response to client: %v", err)
	}
	r.metrics.increaseErrorCount(response.err)
}

func createTraceID(rand *rand2.Rand) []byte {
	var n1, n2 int64
	if rand == nil {
		n1 = rand2.Int63n(math.MaxInt64)
		n2 = rand2.Int63n(math.MaxInt64)
	} else {
		n1 = rand.Int63n(math.MaxInt64)
		n2 = rand.Int63n(math.MaxInt64)
	}

	trace := make([]byte, 16)
	binary.BigEndian.PutUint64(trace, uint64(n1))
	binary.BigEndian.PutUint64(trace[8:], uint64(n2))
	return trace
}

func createVerifier(config *nodeconfig.RouterNodeConfig) *requestfilter.RulesVerifier {
	rv := requestfilter.NewRulesVerifier(nil)
	rv.AddRule(requestfilter.PayloadNotEmptyRule{})
	rv.AddRule(requestfilter.NewMaxSizeFilter(config))
	rv.AddStructureRule(requestfilter.NewSigFilter(config, policies.ChannelWriters))
	return rv
}

// pullAndProcessDecisions pulls decisions from consensus and processes them.
// it store the last decision in wal, and config blocks in config store.
// this function should be run as a goroutine.
func (r *Router) pullAndProcessDecisions() {
	decisionsChan := r.decisionPuller.ReplicateDecision()
	defer func() {
		r.decisionPuller.Stop()
		r.logger.Infof("Stopped decision puller")
	}()

	for {
		select {
		case decision, ok := <-decisionsChan:
			if !ok {
				r.logger.Infof("Decisions channel closed, stopping decisions processing")
				return
			}

			// store the decision in WAL, keeping only the last decision
			err := r.wal.Append(decision.Serialize(), true)
			if err != nil {
				r.logger.Panicf("Failed storing decision in router WAL: %s", err)
			}

			// check if the header contains a config block
			if decision.Num != decision.DecisionNumOfLastConfigBlock {
				continue
			}
			block := decision.AvailableCommonBlocks[len(decision.AvailableCommonBlocks)-1]
			blockNum := block.GetHeader().GetNumber()
			if !protoutil.IsConfigBlock(block) {
				r.logger.Errorf("Expected config block but got non-config block number %d", blockNum)
				continue
			}

			r.logger.Infof("Pulled config block number %d from consensus", blockNum)

			// check if the config block should be stored
			lastBlockInStore, err := r.configStore.Last()
			if err != nil {
				r.logger.Panicf("Failed getting last config block from config store: %s", err)
			}
			if lastBlockInStore.Header.Number >= blockNum {
				r.logger.Infof("Config block number %d is not newer than last config block number %d in config store, skipping", blockNum, lastBlockInStore.Header.Number)
				continue
			}

			// store the config block in config store
			if err := r.configStore.Add(block); err != nil {
				r.logger.Panicf("Failed adding config block to config store: %s", err)
			}
			r.logger.Infof("Added config block %d to config store", blockNum)

			// initiate router restart and apply new config
			r.logger.Warnf("Soft stop")
			go func() {
				err := r.SoftStop()
				if err != nil {
					r.logger.Warnf("The router was not Soft-Stopped properly: %v.", err)
					// r.logger.Warnf("Closing arma process..")
					// close(r.armaStopChan)
					return
				}

				err = r.ApplyConfig(block)
				if err != nil {
					r.logger.Warnf("Failed to apply last config: %v. Admin's action required. ", err)
					// r.logger.Warnf("Closing arma process..")
					// close(r.armaStopChan)
					return
				}
			}()

			// do not pull additional decisions, until the router is restarted.
			r.logger.Infof("Stopping decisions pulling from consensus")
			return

		case <-r.stopChan:
			r.logger.Infof("Stopping decisions pulling from consensus")
			return
		}
	}
}

// IsAllStreamsOK checks that all the streams across all shard-routers are non-faulty.
// Use for testing only.
func (r *Router) IsAllStreamsOK() bool {
	for _, sr := range r.shardRouters {
		if !sr.IsAllStreamsOKinSR() {
			return false
		}
	}
	return true
}

// IsAllConnectionsDown checks that all streams across all shard-routers are disconnected from a batcher.
// Use for testing only.
func (r *Router) IsAllConnectionsDown() bool {
	for _, sr := range r.shardRouters {
		if !sr.IsConnectionsToBatcherDown() {
			return false
		}
	}
	return true
}

// GetConfigStoreSize returns the number of config blocks stored in the config store.
// Use for testing only.
func (r *Router) GetConfigStoreSize() int {
	list, err := r.configStore.ListBlockNumbers()
	if err != nil {
		r.logger.Panicf("Failed listing config store block numbers: %s", err)
	}
	return len(list)
}
