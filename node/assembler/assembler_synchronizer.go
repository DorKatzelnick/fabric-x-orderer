/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
)

// type BFTConfigGetter interface {
// 	BFTConfig() (types.Configuration, []uint64)
// }

type SynchronizerWithStop interface {
	Sync() error
	Stop()
}

type SynchronizerFactory interface {
	// CreateSynchronizer creates a new Assembler Synchronizer.
	CreateSynchronizer(
		logger *flogging.FabricLogger,
		selfID uint64,
		localConfigCluster config.Cluster,
		support AssemblerSupport,
		bccsp bccsp.BCCSP,
		clusterDialer *comm.PredicateDialer,
		targetHeight uint64,
	) SynchronizerWithStop
}

type SynchronizerCreator struct{}

func (*SynchronizerCreator) CreateSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	support AssemblerSupport,
	bccsp bccsp.BCCSP,
	clusterDialer *comm.PredicateDialer,
	targetHeight uint64,
) SynchronizerWithStop {
	return newSynchronizer(logger, selfID, localConfigCluster, support, bccsp, clusterDialer, targetHeight)
}

// newSynchronizer creates a new synchronizer
func newSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	support AssemblerSupport,
	bccsp bccsp.BCCSP,
	clusterDialer *comm.PredicateDialer,
	targetHeight uint64,
) SynchronizerWithStop {
	switch localConfigCluster.ReplicationPolicy {
	case "consensus", "":
		logger.Debug("Creating a BFTSynchronizer")
		return &AssemblerSynchronizer{
			SelfPartyID:         selfID,
			TargetHeight:        targetHeight,
			Support:             support,
			CryptoProvider:      bccsp,
			ClusterDialer:       clusterDialer,
			LocalConfigCluster:  localConfigCluster,
			BlockPullerFactory:  &GenesisFetcherCreator{},
			VerifierFactory:     &noopVerifierCreator{}, // TODO rewrite &verifierCreator{} and replace
			BFTDelivererFactory: &bftDelivererCreator{},
			Logger:              logger,
		}

	default:
		logger.Panicf("Unsupported Cluster.ReplicationPolicy: %s", localConfigCluster.ReplicationPolicy)
		return nil
	}
}

type AssemblerSynchronizer struct {
	Logger              *flogging.FabricLogger
	SelfPartyID         uint64
	TargetHeight        uint64
	Support             AssemblerSupport
	CryptoProvider      bccsp.BCCSP
	ClusterDialer       *comm.PredicateDialer
	LocalConfigCluster  config.Cluster
	BlockPullerFactory  GenesisFetcherFactory
	VerifierFactory     VerifierFactory
	BFTDelivererFactory BFTDelivererFactory

	mutex      sync.Mutex
	syncBuffer *SyncBuffer
}

// Sync starts the synchronization process, which includes fetching the genesis block,
// and then fetching blocks until the TargetHeight is reached. It returns when synchronization is complete, or if an error occurs.
func (a *AssemblerSynchronizer) Sync() error {
	a.Logger.Debugf("Starting Assembler Synchronizer")
	return a.synchronize()
}

func (a *AssemblerSynchronizer) Stop() {
	a.Logger.Infof("Stopping Assembler Synchronizer")
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.syncBuffer != nil {
		a.syncBuffer.Stop()
	}
}

func (a *AssemblerSynchronizer) synchronize() error {

	startHeight := a.Support.Height()

	if startHeight > a.TargetHeight {
		return fmt.Errorf("error synchronizing assembler: startHeight %d is greater than targetHeight %d", startHeight, a.TargetHeight)
	}

	if startHeight == 0 {
		genesisBlock, err := a.fetchGenesisBlock()
		if err != nil {
			a.Logger.Panicf("Cannot join the cluster: %s", errors.Wrap(err, "failed to fetch genesis block"))
		}
		a.Support.WriteConfigBlock(genesisBlock)
		startHeight = a.Support.Height()
		a.Logger.Infof("Fetched and wrote genesis block, new height: %d, party: %d", startHeight, a.SelfPartyID)
	}

	capacityBlocks := uint(100)
	a.mutex.Lock()
	a.syncBuffer = NewSyncBuffer(capacityBlocks)
	a.mutex.Unlock()

	// Create the BFT block deliverer
	bftDeliverer, err := a.createBFTDeliverer(startHeight, arma_types.PartyID(a.SelfPartyID))
	if err != nil {
		return errors.Wrapf(err, "cannot create BFT block deliverer")
	}

	// Start a go-routine that fetches block and inserts them into the syncBuffer.
	go bftDeliverer.DeliverBlocks()
	defer bftDeliverer.Stop()

	_, err = a.getBlocksFromSyncBuffer(startHeight, a.TargetHeight)
	if err != nil {
		return errors.Wrap(err, "failed to get any blocks from SyncBuffer")
	}

	return nil
}

func (a *AssemblerSynchronizer) getBlocksFromSyncBuffer(startHeight, targetHeight uint64) (*common.Block, error) {
	targetSeq := targetHeight - 1
	seq := startHeight
	var blocksFetched int
	a.Logger.Debugf("Will fetch sequences [%d-%d]", seq, targetSeq)

	var lastPulledBlock *common.Block
	for seq <= targetSeq {
		block := a.syncBuffer.PullBlock(seq)
		if block == nil {
			a.Logger.Debugf("Failed to fetch block [%d] from cluster", seq)
			break
		}
		if protoutil.IsConfigBlock(block) {
			a.Support.WriteConfigBlock(block)
			a.Logger.Debugf("Fetched and committed config block [%d] from cluster", seq)
		} else {
			a.Support.WriteBlockSync(block)
			a.Logger.Debugf("Fetched and committed block [%d] from cluster", seq)
		}
		lastPulledBlock = block

		seq++
		blocksFetched++
	}

	a.syncBuffer.Stop()

	if lastPulledBlock == nil {
		return nil, errors.Errorf("failed pulling block %d", seq)
	}

	a.Logger.Infof("Finished synchronizing with cluster, fetched %d blocks, starting from block [%d], up until and including block [%d]",
		blocksFetched, startHeight, lastPulledBlock.Header.Number)

	return lastPulledBlock, nil
}

// createBFTDeliverer creates and initializes the BFT block deliverer.
func (a *AssemblerSynchronizer) createBFTDeliverer(startHeight uint64, myParty arma_types.PartyID) (BFTBlockDeliverer, error) {
	lastBlock := a.Support.Block(startHeight - 1)
	lastConfigBlock, err := a.Support.LastConfigBlock(lastBlock)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve last config block")
	}

	blockOps := &utils.CommonConfigBlockOperations{}
	lastConfigEnv, err := blockOps.ConfigFromBlock(lastConfigBlock)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve last config envelope")
	}

	var updatableVerifier deliverclient.CloneableUpdatableBlockVerifier
	updatableVerifier, err = a.VerifierFactory.CreateBlockVerifier(lastConfigBlock, lastBlock, a.CryptoProvider, a.Logger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create BlockVerificationAssistant")
	}

	clientConfig := a.ClusterDialer.Config // The cluster and block puller use slightly different options
	clientConfig.AsyncConnect = false
	clientConfig.SecOpts.VerifyCertificate = nil

	// The maximal amount of time to wait before retrying to connect.
	maxRetryInterval := 10 * time.Second // TODO s.LocalConfigCluster.ReplicationRetryTimeout
	// The minimal amount of time to wait before retrying. The retry interval doubles after every unsuccessful attempt.
	minRetryInterval := maxRetryInterval / 50
	// The maximal duration of a Sync. After this time Sync returns with whatever it had pulled until that point.
	maxRetryDuration := time.Minute // TODO s.LocalConfigCluster.ReplicationPullTimeout * time.Duration(s.LocalConfigCluster.ReplicationMaxRetries)
	// If a remote orderer does not deliver blocks for this amount of time, even though it can do so, it is replaced as the block deliverer.
	blockCensorshipTimeOut := maxRetryDuration / 3

	bftDeliverer := a.BFTDelivererFactory.CreateBFTDeliverer(
		a.Support.ChannelID(),
		a.syncBuffer,
		&ledgerInfoAdapter{a.Support},
		updatableVerifier,
		blocksprovider.DialerAdapter{ClientConfig: clientConfig},
		&orderers.ConnectionSourceFactory{}, // no overrides in the orderer
		a.CryptoProvider,
		make(chan struct{}),
		a.Support,
		blocksprovider.DeliverAdapter{},
		&blocksprovider.BFTCensorshipMonitorFactory{},
		&AssemblerEndpointExtractor{},
		flogging.MustGetLogger("orderer.blocksprovider").With("channel", a.Support.ChannelID()),
		minRetryInterval,
		maxRetryInterval,
		blockCensorshipTimeOut,
		maxRetryDuration,
		func() (stopRetries bool) {
			a.syncBuffer.Stop()
			return true // In the orderer we must limit the time we try to do Synch()
		},
	)

	a.Logger.Infof("Created a BFTDeliverer: %+v", bftDeliverer)
	bftDeliverer.Initialize(lastConfigEnv.GetConfig(), myParty)

	return bftDeliverer, nil
}

// fetchGenesisBlock fetches the genesis block from remote orderers.
// TODO make this method stoppable, currently it can take a long time if remote endpoints are not responsive, and we have no way to interrupt it.
func (a *AssemblerSynchronizer) fetchGenesisBlock() (*common.Block, error) {
	a.Logger.Infof("Fetching genesis block, party: %d", a.SelfPartyID)
	blockPuller, err := a.BlockPullerFactory.CreateGenesisFetcher(arma_types.PartyID(a.SelfPartyID), a.Support, a.ClusterDialer, a.LocalConfigCluster, a.CryptoProvider, a.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create GenesisFetcher")
	}
	defer blockPuller.Close()

	genesisByEndpoint, err := blockPuller.GenesisByEndpoints()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get GenesisByEndpoints")
	}

	a.Logger.Infof("Received genesis blocks from %d endpoints: %v", len(genesisByEndpoint), slices.Collect(maps.Keys(genesisByEndpoint)))

	// Calculate required matches
	clusterSize := len(a.Support.SharedConfig().Consenters())
	f, requiredMatches, _ := utils.ComputeFTQ(uint16(clusterSize))
	a.Logger.Infof("Cluster size: %d, F: %d, required matches: %d", clusterSize, f, requiredMatches)

	// Count occurrences of each genesis block by hash
	blockCounts := make(map[string]int)
	blockByHash := make(map[string]*common.Block)
	endpointToHash := []string{}
	for endpoint, block := range genesisByEndpoint {
		if block == nil {
			a.Logger.Warnf("Nil genesis block from endpoint: %s", endpoint)
			continue
		}

		blockBytes, err := proto.Marshal(block)
		if err != nil {
			a.Logger.Warnf("Cannot marshal genesis block from endpoint: %s; err: %s", endpoint, err)
			continue
		}

		blockHash := sha256.Sum256(blockBytes)
		blockHashStr := hex.EncodeToString(blockHash[:])

		blockCounts[blockHashStr]++
		blockByHash[blockHashStr] = block
		endpointToHash = append(endpointToHash, fmt.Sprintf("[EP: %s, H: %s]", endpoint, blockHashStr))
	}

	// Find a block that appears at least F+1 times
	for blockHash, count := range blockCounts {
		if count >= int(requiredMatches) {
			genesisBlock := blockByHash[blockHash]
			a.Logger.Infof("Found genesis block with %d matching copies (required: %d)", count, requiredMatches)
			return genesisBlock, nil
		}
	}

	return nil, errors.Errorf("could not find genesis block with at least %d matching copies: %+v", requiredMatches, endpointToHash)
}

type GenesisFetcherFactory interface {
	// CreateGenesisFetcher creates a new genesis fetcher.
	CreateGenesisFetcher(
		myPartyID types.PartyID,
		support AssemblerSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
		logger *flogging.FabricLogger,
	) (GenesisFetcher, error)
}

type GenesisFetcherCreator struct{}

func (*GenesisFetcherCreator) CreateGenesisFetcher(
	myPartyID types.PartyID,
	support AssemblerSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (GenesisFetcher, error) {
	return newBlockPuller(myPartyID, support, baseDialer, clusterConfig, bccsp, logger)
}

// newBlockPuller creates a new block puller, which is used for target height detection.
func newBlockPuller(
	myPartyID types.PartyID,
	support AssemblerSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (GenesisFetcher, error) {
	// TODO replace this with the actual implementation
	verifyBlockSequenceNoOp := func(blocks []*common.Block, _ string) error {
		// TODO
		return nil
	}

	stdDialer := &comm.StandardDialer{
		Config: baseDialer.Config.Clone(),
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// Extract endpoint and TLS cert from the config, excluding the self endpoint.
	endpoints, err := extractEndpointCriteriaFromConfig(myPartyID, support)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract endpoint criteria from config")
	}

	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	myCert, err := x509.ParseCertificate(der.Bytes)
	if err != nil {
		logger.Warnf("Failed parsing my own TLS certificate: %v, therefore we may connect to our own endpoint when pulling blocks", err)
	}

	// TODO Fabric defaults. Extend the config to have these values, and use the config values instead of hardcoding them here.
	// Cluster: Cluster{
	// 	ReplicationMaxRetries:          12,
	// 	RPCTimeout:                     time.Second * 7,
	// 	DialTimeout:                    time.Second * 5,
	// 	ReplicationBufferSize:          20971520,
	// 	SendBufferSize:                 100,
	// 	ReplicationRetryTimeout:        time.Second * 5,
	// 	ReplicationPullTimeout:         time.Second * 5,
	// 	CertExpirationWarningThreshold: time.Hour * 24 * 7,
	// 	ReplicationPolicy:              "consensus", // BFT default; on etcdraft it is ignored
	// },

	bp := &comm.BlockPuller{
		MyOwnTLSCert:        myCert,
		VerifyBlockSequence: verifyBlockSequenceNoOp,
		Logger:              logger,
		RetryTimeout:        time.Second * 5,  // clusterConfig.ReplicationRetryTimeout,
		MaxTotalBufferBytes: 20 * 1024 * 1024, // clusterConfig.ReplicationBufferSize,
		FetchTimeout:        time.Second * 5,  // clusterConfig.ReplicationPullTimeout,
		Endpoints:           endpoints,        // TODO the block puller is not party aware yet
		Signer:              support,
		TLSCert:             der.Bytes,
		Channel:             support.ChannelID(),
		Dialer:              stdDialer,
	}

	logger.Infof("Built new block puller (target height detector) with endpoints: %+v", endpoints)

	return bp, nil
}

type GenesisFetcher interface {
	GenesisByEndpoints() (map[string]*common.Block, error)
	Close()
}

type SyncBuffer struct {
	blockCh  chan *common.Block
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewSyncBuffer(capacity uint) *SyncBuffer {
	if capacity == 0 {
		capacity = 10
	}
	return &SyncBuffer{
		blockCh: make(chan *common.Block, capacity),
		stopCh:  make(chan struct{}),
	}
}

// HandleBlock gives the block to the next stage of processing after fetching it from a remote orderer.
func (sb *SyncBuffer) HandleBlock(channelID string, block *common.Block) error {
	if block == nil || block.Header == nil {
		return errors.Errorf("empty block or block header, channel: %s", channelID)
	}

	select {
	case sb.blockCh <- block:
		return nil
	case <-sb.stopCh:
		return errors.Errorf("SyncBuffer stopping, channel: %s", channelID)
	}
}

func (sb *SyncBuffer) PullBlock(seq uint64) *common.Block {
	var block *common.Block
	for {
		select {
		case block = <-sb.blockCh:
			if block == nil || block.Header == nil {
				return nil
			}
			if block.GetHeader().GetNumber() == seq {
				return block
			}
			if block.GetHeader().GetNumber() < seq {
				continue
			}
			if block.GetHeader().GetNumber() > seq {
				return nil
			}
		case <-sb.stopCh:
			return nil
		}
	}
}

func (sb *SyncBuffer) Stop() {
	sb.stopOnce.Do(func() {
		close(sb.stopCh)
	})
}

// todo - write common function for consenter enpoints / assembler endpoints.
func extractEndpointCriteriaFromConfig(myPartyID types.PartyID, support AssemblerSupport) ([]comm.EndpointCriteria, error) {
	party2endpoint, err := config.ExtractConsenterAddresses(support.SharedConfig())
	if err != nil {
		return nil, err
	}

	var endpoints []comm.EndpointCriteria
	for party, ep := range party2endpoint {
		if party == myPartyID {
			continue
		}
		endpointCriteria := &comm.EndpointCriteria{
			Endpoint:   ep.Address,
			TLSRootCAs: ep.RootCerts,
		}
		endpoints = append(endpoints, *endpointCriteria)
	}

	return endpoints, nil
}

type AssemblerSupport interface {
	identity.SignerSerializer
	Height() uint64
	SharedConfig() channelconfig.Orderer
	ChannelID() string
	WriteConfigBlock(block *common.Block)
	WriteBlockSync(block *common.Block)
	Block(number uint64) *common.Block
	LastConfigBlock(block *common.Block) (*common.Block, error)
}

//go:generate counterfeiter -o mocks/verifier_factory.go --fake-name VerifierFactory . VerifierFactory

type VerifierFactory interface {
	CreateBlockVerifier(
		configBlock *common.Block,
		lastBlock *common.Block,
		cryptoProvider bccsp.BCCSP,
		lg *flogging.FabricLogger,
	) (deliverclient.CloneableUpdatableBlockVerifier, error)
}

type verifierCreator struct{}

func (*verifierCreator) CreateBlockVerifier(
	configBlock *common.Block,
	lastBlock *common.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	updatableVerifier, err := deliverclient.NewBlockVerificationAssistant(configBlock, lastBlock, cryptoProvider, lg)
	return updatableVerifier, err
}

//go:generate counterfeiter -o mocks/bft_deliverer_factory.go --fake-name BFTDelivererFactory . BFTDelivererFactory

type BFTDelivererFactory interface {
	CreateBFTDeliverer(
		channelID string,
		blockHandler blocksprovider.BlockHandler,
		ledger blocksprovider.LedgerInfo,
		updatableBlockVerifier blocksprovider.UpdatableBlockVerifier,
		dialer blocksprovider.Dialer,
		orderersSourceFactory blocksprovider.OrdererConnectionSourceFactory,
		cryptoProvider bccsp.BCCSP,
		doneC chan struct{},
		signer identity.SignerSerializer,
		deliverStreamer blocksprovider.DeliverStreamer,
		censorshipDetectorFactory blocksprovider.CensorshipDetectorFactory,
		endpointsExtractor blocksprovider.EndpointsExtractor,
		logger *flogging.FabricLogger,
		initialRetryInterval time.Duration,
		maxRetryInterval time.Duration,
		blockCensorshipTimeout time.Duration,
		maxRetryDuration time.Duration,
		maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
	) BFTBlockDeliverer
}

type bftDelivererCreator struct{}

func (*bftDelivererCreator) CreateBFTDeliverer(
	channelID string,
	blockHandler blocksprovider.BlockHandler,
	ledger blocksprovider.LedgerInfo,
	updatableBlockVerifier blocksprovider.UpdatableBlockVerifier,
	dialer blocksprovider.Dialer,
	orderersSourceFactory blocksprovider.OrdererConnectionSourceFactory,
	cryptoProvider bccsp.BCCSP,
	doneC chan struct{},
	signer identity.SignerSerializer,
	deliverStreamer blocksprovider.DeliverStreamer,
	censorshipDetectorFactory blocksprovider.CensorshipDetectorFactory,
	endpointsExtractor blocksprovider.EndpointsExtractor,
	logger *flogging.FabricLogger,
	initialRetryInterval time.Duration,
	maxRetryInterval time.Duration,
	blockCensorshipTimeout time.Duration,
	maxRetryDuration time.Duration,
	maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
) BFTBlockDeliverer {
	bftDeliverer := &blocksprovider.BFTDeliverer{
		ChannelID:                       channelID,
		BlockHandler:                    blockHandler,
		Ledger:                          ledger,
		UpdatableBlockVerifier:          updatableBlockVerifier,
		Dialer:                          dialer,
		OrderersSourceFactory:           orderersSourceFactory,
		CryptoProvider:                  cryptoProvider,
		DoneC:                           doneC,
		Signer:                          signer,
		DeliverStreamer:                 deliverStreamer,
		CensorshipDetectorFactory:       censorshipDetectorFactory,
		EndpointsExtractor:              endpointsExtractor,
		Logger:                          logger,
		InitialRetryInterval:            initialRetryInterval,
		MaxRetryInterval:                maxRetryInterval,
		BlockCensorshipTimeout:          blockCensorshipTimeout,
		MaxRetryDuration:                maxRetryDuration,
		MaxRetryDurationExceededHandler: maxRetryDurationExceededHandler,
	}
	return bftDeliverer
}

//go:generate counterfeiter -o mocks/bft_block_deliverer.go --fake-name BFTBlockDeliverer . BFTBlockDeliverer
type BFTBlockDeliverer interface {
	Stop()
	DeliverBlocks()
	Initialize(channelConfig *common.Config, selfPartyID types.PartyID)
}

// ledgerInfoAdapter translates from blocksprovider.LedgerInfo in to calls to ConsenterSupport.
type ledgerInfoAdapter struct {
	support AssemblerSupport
}

func (a *ledgerInfoAdapter) LedgerHeight() (uint64, error) {
	return a.support.Height(), nil
}

func (a *ledgerInfoAdapter) GetCurrentBlockHash() ([]byte, error) {
	return nil, errors.New("not implemented: never used in orderer")
}

// AssemblerEndpointExtractor implements blocksprovider.EndpointsExtractor
type AssemblerEndpointExtractor struct{}

// ExtractEndpoints extracts assembler endpoints from the given orderer configuration
func (*AssemblerEndpointExtractor) ExtractEndpoints(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	return config.ExtractAssemblerAddresses(ordererConfig)
}

// ==== TODO remove the noopVerifierCreator and use the real verifier in synchronizer after the real verifier is implemented. The noopVerifierCreator and noopBlockVerifier can be used in tests or when block verification is not needed.

// noopVerifierCreator creates a block verifier that does not actually verify blocks, which can be used in tests or when block verification is not needed.
type noopVerifierCreator struct{}

func (*noopVerifierCreator) CreateBlockVerifier(
	configBlock *common.Block,
	lastBlock *common.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	return &noopBlockVerifier{}, nil
}

// noopBlockVerifier is a block verifier that does not actually verify blocks, which can be used in tests or when block verification is not needed.
type noopBlockVerifier struct{}

// VerifyBlock checks block integrity and its relation to the chain, and verifies the signatures.
func (*noopBlockVerifier) VerifyBlock(block *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) VerifyBlockAttestation(block *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) UpdateConfig(configBlock *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) UpdateBlockHeader(block *common.Block) {
	// TODO
}

func (*noopBlockVerifier) Clone() deliverclient.CloneableUpdatableBlockVerifier {
	return &noopBlockVerifier{}
}
