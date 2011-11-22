package waffle

import (
	"encoding/gob"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type Resp int

const (
	OK = iota
	NOT_OK
)

// GobRpc will ship with the base distribution
type GobRPCClientBase struct {
	lock          sync.Mutex
	workerClients map[string]*rpc.Client
}

func (c *GobRPCClientBase) init() {
	c.workerClients = make(map[string]*rpc.Client)

	// Register the base types with gob
	gob.Register(&VertexBase{})
	gob.Register(&EdgeBase{})
	gob.Register(&MsgBase{})
	gob.Register(&WorkerError{})
}

type GobMasterRPCClient struct {
	GobRPCClientBase
}

func NewGobMasterRPCClient() *GobMasterRPCClient {
	c := &GobMasterRPCClient{}
	c.init()
	return c
}

func (c *GobMasterRPCClient) workerClient(workerAddr string) (*rpc.Client, error) {
	c.lock.Lock()
	if _, ok := c.workerClients[workerAddr]; !ok {
		cl, err := rpc.DialHTTP("tcp", workerAddr)
		if err != nil {
			return nil, err
		}
		c.workerClients[workerAddr] = cl
	}
	client := c.workerClients[workerAddr]
	c.lock.Unlock()
	return client, nil
}

func (s *GobMasterRPCClient) PushTopology(workerAddr string, info *TopologyInfo) error {
	client, err := s.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRPCServer.RecieveTopology", info, &resp); err != nil {
		return err
	}
	return nil
}

func (s *GobMasterRPCClient) ExecutePhase(workerAddr string, exec *PhaseExec) error {
	client, err := s.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRPCServer.ExecPhase", exec, &resp); err != nil {
		return err
	}
	return nil
}

type GobWorkerRPCClient struct {
	GobRPCClientBase
	mclient *rpc.Client
}

func NewGobWorkerRPCClient() *GobWorkerRPCClient {
	c := &GobWorkerRPCClient{}
	c.init()
	return c
}

func (c *GobWorkerRPCClient) workerClient(workerAddr string) (*rpc.Client, error) {
	// TODO avoid lock contention, clean up the node interface so we can access wmap cleanly
	c.lock.Lock()
	if _, ok := c.workerClients[workerAddr]; !ok {
		cl, err := rpc.DialHTTP("tcp", workerAddr)
		if err != nil {
			return nil, err
		}
		c.workerClients[workerAddr] = cl
	}
	client := c.workerClients[workerAddr]
	c.lock.Unlock()
	return client, nil
}

func (c *GobWorkerRPCClient) masterClient(masterAddr string) (*rpc.Client, error) {
	if c.mclient == nil {
		client, err := rpc.DialHTTP("tcp", masterAddr)
		if err != nil {
			return nil, err
		}
		c.mclient = client
	}
	return c.mclient, nil
}

func (c *GobWorkerRPCClient) Register(masterAddr, host, port string) (string, error) {
	mclient, err := c.masterClient(masterAddr)
	if err != nil {
		return "", err
	}
	var resp RegisterResp
	if err = mclient.Call("GobMasterRPCServer.RegisterWorker", &RegisterInfo{Addr: host, Port: port}, &resp); err != nil {
		return "", err
	}
	return resp.JobId, nil
}

func (c *GobWorkerRPCClient) SendSummary(masterAddr string, summary *PhaseSummary) error {
	mclient, err := c.masterClient(masterAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err := mclient.Call("GobMasterRPCServer.EnterPhaseBarrier", summary, &resp); err != nil {
		return err
	}
	return nil
}

func (c *GobWorkerRPCClient) SendVertices(workerAddr string, vertices []Vertex) error {
	client, err := c.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRPCServer.QueueVertices", vertices, &resp); err != nil {
		return err
	}
	return nil
}

func (c *GobWorkerRPCClient) SendMessages(workerAddr string, msgs []Msg) error {
	client, err := c.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRPCServer.QueueMessages", msgs, &resp); err != nil {
		return err
	}
	return nil
}

type GobMasterRPCServer struct {
	master *Master
}

func NewGobMasterRPCServer() *GobMasterRPCServer {
	return &GobMasterRPCServer{}
}

func (s *GobMasterRPCServer) Start(master *Master) {
	s.master = master
	rpc.Register(s)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", net.JoinHostPort(s.master.Host(), s.master.Port()))
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}

func (s *GobMasterRPCServer) RegisterWorker(info *RegisterInfo, resp *RegisterResp) error {
	jobId, err := s.master.RegisterWorker(info.Addr, info.Port)
	if err != nil {
		return err
	}
	resp.JobId = jobId
	return nil
}

func (s *GobMasterRPCServer) EnterPhaseBarrier(summary *PhaseSummary, resp *Resp) error {
	*resp = OK
	s.master.EnterBarrier(summary)
	return nil
}

type GobWorkerRPCServer struct {
	worker *Worker
}

func NewGobWorkerRPCServer() *GobWorkerRPCServer {
	return &GobWorkerRPCServer{}
}

func (s *GobWorkerRPCServer) Start(worker *Worker) {
	s.worker = worker
	rpc.Register(s)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", net.JoinHostPort(s.worker.Host(), s.worker.Port()))
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}

func (s *GobWorkerRPCServer) ExecPhase(exec *PhaseExec, resp *Resp) error {
	*resp = OK
	return s.worker.ExecPhase(exec)
}

func (s *GobWorkerRPCServer) RecieveTopology(info *TopologyInfo, resp *Resp) error {
	*resp = OK
	s.worker.SetTopology(info.PartitionMap)
	return nil
}

func (s *GobWorkerRPCServer) QueueMessages(msgs []Msg, resp *Resp) error {
	*resp = OK
	s.worker.QueueMessages(msgs)
	return nil
}

func (s *GobWorkerRPCServer) QueueVertices(vertices []Vertex, resp *Resp) error {
	*resp = OK
	s.worker.QueueVertices(vertices)
	return nil
}
